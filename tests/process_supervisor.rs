use kakune_core::process_supervisor;
use std::{process::Stdio, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

#[tokio::test]
async fn cancellation_closes_pipes_inherited_by_descendants() {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_kakune-process-fixture"));
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null());
    let mut child = process_supervisor::spawn_async(command).unwrap();
    let mut output = tokio::io::BufReader::new(child.stdout().take().unwrap());
    let mut ready = String::new();
    tokio::time::timeout(Duration::from_secs(5), output.read_line(&mut ready))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.trim(), "descendant-ready");
    child.start_kill().unwrap();
    tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    let mut remaining = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), output.read_to_end(&mut remaining))
        .await
        .expect("descendant must release inherited stdout")
        .unwrap();
}

#[test]
fn synchronous_cancellation_kills_descendants() {
    use std::io::{BufRead, Read};
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_kakune-process-fixture"));
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null());
    let mut child = process_supervisor::spawn_sync(command).unwrap();
    let mut output = std::io::BufReader::new(child.stdout().take().unwrap());
    let mut ready = String::new();
    output.read_line(&mut ready).unwrap();
    assert_eq!(ready.trim(), "descendant-ready");
    child.kill().unwrap();
    child.wait().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut remaining = Vec::new();
        send.send(output.read_to_end(&mut remaining)).unwrap();
    });
    receive
        .recv_timeout(Duration::from_secs(2))
        .expect("descendant must release inherited stdout")
        .unwrap();
    reader.join().unwrap();
}
