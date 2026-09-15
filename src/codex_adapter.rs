//! Adapter between the runtime's message blocks and the Responses wire format.
use crate::minimax::{Message, MessageContent, MessageRole, MessagesResponse, Usage};
use serde_json::{Value, json};

pub fn request(
    messages: Vec<Message>,
    tools: Option<Vec<Value>>,
    choice: Option<Value>,
) -> Result<Value, String> {
    let mut input = Vec::new();
    for message in messages {
        let role = match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
        };
        let blocks = match message.content {
            MessageContent::Text(text) => vec![json!({"type":"text","text":text})],
            MessageContent::Blocks(blocks) => blocks,
        };
        for block in blocks {
            match block["type"].as_str() {
                Some("text") => input.push(json!({"role":role,"content":block["text"]})),
                Some("tool_use") => input.push(json!({"type":"function_call","call_id":block["id"],"name":block["name"],"arguments":block["input"].to_string()})),
                Some("tool_result") => input.push(json!({"type":"function_call_output","call_id":block["tool_use_id"],"output":block["content"].as_str().map(str::to_owned).unwrap_or_else(|| block["content"].to_string())})),
                Some("codex_item") => input.push(block["item"].clone()),
                _ => return Err("unsupported message block for Codex".into()),
            }
        }
    }
    let mut request = json!({"input":input});
    if let Some(tools) = tools {
        request["tools"] = Value::Array(tools.into_iter().map(|tool| json!({"type":"function","name":tool["name"],"description":tool["description"],"parameters":tool["input_schema"],"strict":false})).collect());
    }
    if let Some(choice) = choice {
        request["tool_choice"] = match choice["type"].as_str() {
            Some("tool") => json!({"type":"function","name":choice["name"]}),
            Some("any") => json!("required"),
            Some("auto") => json!("auto"),
            _ => return Err("unsupported Codex tool choice".into()),
        };
    }
    Ok(request)
}

pub fn response(raw: Value) -> Result<MessagesResponse, String> {
    let mut content = Vec::new();
    for item in raw["output"]
        .as_array()
        .ok_or("Codex response omitted output")?
    {
        match item["type"].as_str() {
            Some("function_call") => content.push(json!({"type":"tool_use","id":item["call_id"],"name":item["name"],"input":serde_json::from_str::<Value>(item["arguments"].as_str().ok_or("missing function arguments")?).map_err(|_| "invalid function arguments")?})),
            Some("message") => {
                for block in item["content"].as_array().ok_or("missing message content")? {
                    if block["type"] == "output_text" { content.push(json!({"type":"text","text":block["text"]})); }
                }
            }
            Some("reasoning") => content.push(json!({"type":"codex_item","item":item})),
            _ => {},
        }
    }
    let usage = raw.get("usage").map(|value| Usage {
        input_tokens: value["input_tokens"].as_u64(),
        output_tokens: value["output_tokens"].as_u64(),
        total_tokens: value["total_tokens"].as_u64(),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: value["input_tokens_details"]["cached_tokens"].as_u64(),
    });
    Ok(MessagesResponse {
        id: raw["id"].as_str().map(str::to_owned),
        model: raw["model"].as_str().map(str::to_owned),
        role: Some(MessageRole::Assistant),
        content,
        stop_reason: None,
        usage,
        raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tool_round_trip_preserves_reasoning_and_call_identity() {
        let reply = response(json!({"output":[{"type":"reasoning","encrypted_content":"opaque"},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":\"hello\"}"}]})).unwrap();
        let request = request(
            vec![
                Message {
                    role: MessageRole::Assistant,
                    content: MessageContent::Blocks(reply.content),
                },
                Message {
                    role: MessageRole::User,
                    content: MessageContent::Blocks(vec![
                        json!({"type":"tool_result","tool_use_id":"call_1","content":"found"}),
                    ]),
                },
            ],
            None,
            None,
        )
        .unwrap();
        assert_eq!(request["input"][0]["encrypted_content"], "opaque");
        assert_eq!(request["input"][1]["call_id"], "call_1");
        assert_eq!(
            request["input"][2],
            json!({"type":"function_call_output","call_id":"call_1","output":"found"})
        );
    }
    #[test]
    fn forced_structured_tool_uses_responses_schema() {
        let value = request(vec![], Some(vec![json!({"name":"submit_result","description":"result","input_schema":{"type":"object"}})]), Some(json!({"type":"tool","name":"submit_result"}))).unwrap();
        assert_eq!(value["tools"][0]["parameters"]["type"], "object");
        assert_eq!(
            value["tool_choice"],
            json!({"type":"function","name":"submit_result"})
        );
    }
}
