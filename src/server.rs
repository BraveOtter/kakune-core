//! Public HTTP/TLS transport. Authentication remains in the same API router.
use crate::config::ApiConfig;
use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::{conn::auto::Builder, graceful::GracefulShutdown},
    service::TowerToHyperService,
};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot},
    task::JoinSet,
    time::Instant,
};
use tokio_rustls::TlsAcceptor;

pub fn tls_config(config: &ApiConfig) -> Result<Option<Arc<ServerConfig>>, String> {
    match (&config.tls_cert, &config.tls_key) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => {
            let chain = CertificateDer::pem_file_iter(cert)
                .map_err(|error| format!("cannot read TLS certificate: {error}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("invalid TLS certificate: {error}"))?;
            let key = PrivateKeyDer::from_pem_file(key)
                .map_err(|_| "cannot read TLS private key".to_string())?;
            let mut server = ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|error| error.to_string())?
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(|error| format!("invalid TLS configuration: {error}"))?;
            server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            Ok(Some(Arc::new(server)))
        }
        _ => Err("TLS certificate and key must be configured together".to_string()),
    }
}

pub async fn serve(
    listener: TcpListener,
    app: Router,
    config: &ApiConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    serve_until(listener, app, config, async move {
        shutdown.await;
        Instant::now() + crate::lifecycle::CORE_GRACE_BUDGET
    })
    .await
}

/// Serves HTTP or TLS connections until the supplied absolute grace deadline.
/// The deadline is created by the lifecycle controller and is never renewed
/// while connections are drained.
pub async fn serve_until(
    listener: TcpListener,
    app: Router,
    config: &ApiConfig,
    shutdown: impl Future<Output = Instant> + Send + 'static,
) -> Result<(), String> {
    serve_until_ready(listener, app, config, shutdown, None).await
}

/// As `serve_until`, with a one-shot observation that the validated listener
/// has entered its accept loop. Startup code publishes readiness only after it
/// receives this signal.
pub async fn serve_until_ready(
    listener: TcpListener,
    app: Router,
    config: &ApiConfig,
    shutdown: impl Future<Output = Instant> + Send + 'static,
    ready: Option<oneshot::Sender<Result<SocketAddr, String>>>,
) -> Result<(), String> {
    let tls = tls_config(config)?;
    let local_address = listener.local_addr().map_err(|error| error.to_string())?;
    if !local_address.ip().is_loopback() && tls.is_none() {
        return Err("non-loopback listeners require TLS".to_string());
    }
    let acceptor = tls.map(TlsAcceptor::from);
    let slots = Arc::new(Semaphore::new(256));
    let mut connections = JoinSet::new();
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
    if let Some(ready) = ready {
        let _ = ready.send(Ok(local_address));
    }

    let deadline = loop {
        tokio::select! {
            deadline = &mut shutdown => break deadline,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| error.to_string())?;
                let Ok(slot) = slots.clone().try_acquire_owned() else { continue; };
                let acceptor = acceptor.clone();
                let watcher = graceful.watcher();
                let service = TowerToHyperService::new(app.clone());
                connections.spawn(async move {
                    let _slot = slot;
                    if let Some(acceptor) = acceptor {
                        if let Ok(Ok(stream)) = tokio::time::timeout(
                            Duration::from_secs(10),
                            acceptor.accept(stream),
                        ).await {
                            let builder = Builder::new(TokioExecutor::new());
                            let connection = builder
                                .serve_connection_with_upgrades(TokioIo::new(stream), service);
                            let _ = watcher.watch(connection).await;
                        }
                    } else {
                        let builder = Builder::new(TokioExecutor::new());
                        let connection = builder
                            .serve_connection_with_upgrades(TokioIo::new(stream), service);
                        let _ = watcher.watch(connection).await;
                    }
                });
            }
        }
    };

    drop(listener);
    if tokio::time::timeout_at(deadline, graceful.shutdown())
        .await
        .is_err()
    {
        connections.abort_all();
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{Router, http::StatusCode, routing::get};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn shutdown_closes_idle_keep_alive_connections_before_the_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should be inspectable");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let config = ApiConfig::default();
        let server = tokio::spawn(async move {
            serve_until_ready(
                listener,
                Router::new().route("/", get(|| async { StatusCode::NO_CONTENT })),
                &config,
                async move {
                    shutdown_receiver
                        .await
                        .expect("shutdown signal should arrive")
                },
                None,
            )
            .await
        });
        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("test client should connect");
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("test request should be sent");
        let mut response = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !response.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer))
                .await
                .expect("server should respond promptly")
                .expect("test response should be readable");
            assert_ne!(
                count, 0,
                "server should answer before the connection closes"
            );
            response.extend_from_slice(&buffer[..count]);
        }

        shutdown_sender
            .send(Instant::now() + Duration::from_secs(2))
            .expect("server should still be waiting for shutdown");
        let result = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("idle keep-alive connection should not consume the grace deadline")
            .expect("server task should join");
        assert!(result.is_ok(), "server should stop gracefully: {result:?}");
    }
}
