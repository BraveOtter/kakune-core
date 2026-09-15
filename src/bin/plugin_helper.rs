//! Helper binary used by the plugin_process integration test to simulate a
//! minimal JSON-RPC plugin process.  The test executable spawns this binary
//! with a plugin manifest pointing at its own path; we read JSON-RPC requests
//! from stdin and emit responses on stdout.  See
//! `tests/plugin_process.rs` for the manifest wiring.

use std::io::{BufRead, Write};

use serde_json::{Value, json};

fn main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut input = stdin.lock();
    for expected_id in 1_u64.. {
        let mut line = String::new();
        if input.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let request: Value = serde_json::from_str(&line).expect("request JSON");
        let id = request.get("id").cloned().expect("request ID");
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .expect("method");
        assert_eq!(id, Value::from(expected_id), "request IDs are sequential");
        if method == "host-roundtrip" {
            writeln!(stdout,"{}",json!({"jsonrpc":"2.0","id":"plugin:1","method":"artifact/write","params":{"text":"roundtrip"}})).unwrap();
            stdout.flush().unwrap();
            let mut reply = String::new();
            input.read_line(&mut reply).unwrap();
            let reply: Value = serde_json::from_str(&reply).unwrap();
            assert_eq!(reply["id"], "plugin:1");
            writeln!(
                stdout,
                "{}",
                json!({"jsonrpc":"2.0","id":id,"result":reply})
            )
            .unwrap();
            stdout.flush().unwrap();
            continue;
        }
        if method == "ignore" {
            continue;
        }
        if method == "oversize" {
            writeln!(
                stdout,
                "{}",
                json!({"jsonrpc":"2.0","id":id,"result":"x".repeat(256)})
            )
            .expect("oversized response written");
            stdout.flush().expect("stdout flushed");
            continue;
        }
        if method == "initialize" {
            writeln!(
                stdout,
                "{}",
                json!({"jsonrpc":"2.0","method":"log/emit","params":{"message":"started"}})
            )
            .expect("notification written");
            eprintln!("plugin diagnostic output");
        }
        if method == "node/execute" {
            let params = request
                .get("params")
                .and_then(Value::as_object)
                .expect("structured node parameters");
            assert!(params.get("executionId").and_then(Value::as_str).is_some());
            assert!(params.get("nodeId").and_then(Value::as_str).is_some());
            assert_eq!(params.get("nodeType"), Some(&json!("org.example.greet@1")));
            assert!(params.get("operationId").and_then(Value::as_str).is_some());
            let name = params
                .get("inputs")
                .and_then(Value::as_object)
                .and_then(|inputs| inputs.get("name"))
                .and_then(Value::as_str)
                .expect("name input");
            if name == "fail" {
                writeln!(
                    stdout,
                    "{}",
                    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"rejected"}})
                )
                .expect("error response written");
            } else if name == "wait" {
                std::thread::sleep(std::time::Duration::from_secs(30));
            } else {
                writeln!(
                    stdout,
                    "{}",
                    json!({"jsonrpc":"2.0","id":id,"result":{"route":"success","outputs":{"greeting":format!("Hello, {name}!")},"message":format!("greeted {name}")}})
                )
                .expect("node response written");
            }
            stdout.flush().expect("stdout flushed");
            continue;
        }
        writeln!(
            stdout,
            "{}",
            json!({"jsonrpc":"2.0","id":id,"result":{"ok":true}})
        )
        .expect("response written");
        stdout.flush().expect("stdout flushed");
        if method == "shutdown" {
            return;
        }
    }
}
