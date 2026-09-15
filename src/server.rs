//! Public HTTP/TLS transport. Authentication remains in the same API router.
use crate::config::ApiConfig;
use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
    service::TowerToHyperService,
};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet};
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
    let tls = tls_config(config)?;
    if !listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .ip()
        .is_loopback()
        && tls.is_none()
    {
        return Err("non-loopback listeners require TLS".to_string());
    }
    let Some(tls) = tls else {
        return axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|error| error.to_string());
    };
    let acceptor = TlsAcceptor::from(tls);
    let slots = Arc::new(Semaphore::new(256));
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| error.to_string())?;
                let Ok(slot) = slots.clone().try_acquire_owned() else { continue; };
                let acceptor = acceptor.clone();
                let service = TowerToHyperService::new(app.clone());
                connections.spawn(async move {
                    let _slot = slot;
                    if let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream)).await {
                        let _ = Builder::new(TokioExecutor::new()).serve_connection_with_upgrades(TokioIo::new(stream), service).await;
                    }
                });
            }
        }
    }
    // Bound shutdown even for clients keeping streams open indefinitely.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}
