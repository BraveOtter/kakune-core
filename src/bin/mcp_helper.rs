//! Helper binary used by the mcp_client integration test to simulate a minimal
//! MCP stdio server.  The test executable spawns this binary via an MCP
//! stdio config; we read JSON-RPC requests from stdin and emit responses on
//! stdout.  See `tests/mcp_client.rs` for the spawn wiring.

use std::io::{BufRead, Write};

use serde_json::{Value, json};

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    if std::env::var("KAKUNE_MCP_HELPER_STDERR").as_deref() == Ok("large") {
        eprint!("{}", "diagnostic ".repeat(8 * 1024));
    }
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut initialized = false;
    for line in stdin.lock().lines() {
        let request: Value =
            serde_json::from_str(&line.expect("stdin line")).expect("request JSON");
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .expect("request method");
        if method == "notifications/initialized" {
            assert!(request.get("id").is_none());
            initialized = true;
            continue;
        }
        let id = request.get("id").cloned().expect("request ID");
        assert!(initialized || method == "initialize");
        match method {
            "initialize" => {
                let mut result = json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "test-mcp-server", "version": "1.0.0"}
                });
                if mode == "large-init" {
                    result["serverInfo"]["name"] = json!("x".repeat(1024));
                }
                respond(&mut stdout, id, result);
            }
            "tools/list" => {
                if mode == "notification-flood" {
                    for _ in 0..=256 {
                        writeln!(
                            stdout,
                            "{}",
                            json!({
                                "jsonrpc":"2.0",
                                "method":"notifications/progress",
                                "params":{}
                            })
                        )
                        .expect("notification written");
                    }
                }
                let mut result = json!({
                    "tools": [{
                        "name": "greet",
                        "description": "Greets a name",
                        "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}}},
                        "outputSchema": {"type": "object"}
                    }]
                });
                if std::env::var("KAKUNE_MCP_HELPER_PAGE_LOOP").as_deref() == Ok("true") {
                    result["nextCursor"] = json!("next-page");
                }
                respond(&mut stdout, id, result)
            }
            "tools/call" => {
                let params = request
                    .get("params")
                    .and_then(Value::as_object)
                    .expect("call params");
                let name = params
                    .get("name")
                    .and_then(Value::as_str)
                    .expect("tool name");
                if name == "ignore" {
                    continue;
                }
                if name == "oversize" {
                    respond(
                        &mut stdout,
                        id,
                        json!({"content": [{"type": "text", "text": "x".repeat(512)}]}),
                    );
                    continue;
                }
                let name = params
                    .get("arguments")
                    .and_then(Value::as_object)
                    .and_then(|arguments| arguments.get("name"))
                    .and_then(Value::as_str)
                    .expect("name argument");
                respond(
                    &mut stdout,
                    id,
                    json!({
                        "content": [{"type": "text", "text": format!("Hello, {name}!")}],
                        "structuredContent": {"greeting": format!("Hello, {name}!")}
                    }),
                );
            }
            other => panic!("unexpected MCP method {other}"),
        }
    }
}

fn respond(stdout: &mut std::io::Stdout, id: Value, result: Value) {
    writeln!(
        stdout,
        "{}",
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    )
    .expect("response written");
    stdout.flush().expect("stdout flushed");
}
