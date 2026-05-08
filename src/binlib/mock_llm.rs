//! `mock_llm_server` 的可测试逻辑。
//!
//! 目标：在不启动固定端口长期进程的前提下，对确定性分支做轻量覆盖。

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json, Response,
    },
};
use futures::stream::Stream;
use serde_json::Value;
use tokio::time::sleep;

#[derive(Clone, Debug, PartialEq)]
pub struct MockConfig {
    pub delay_ms: u64,
    pub error_rate: f32,
    pub rate_limit_rate: f32,
}

impl MockConfig {
    pub fn from_env() -> Self {
        Self {
            delay_ms: std::env::var("MOCK_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(50),
            error_rate: std::env::var("MOCK_ERROR_RATE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            rate_limit_rate: std::env::var("MOCK_RATE_LIMIT_RATE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockOutcome {
    RateLimited,
    InternalError,
    Success,
}

/// 基于随机样本做纯决策，方便测试边界值行为。
pub fn decide_outcome(
    config: &MockConfig,
    rate_limit_sample: f32,
    error_sample: f32,
) -> MockOutcome {
    if rate_limit_sample < config.rate_limit_rate {
        return MockOutcome::RateLimited;
    }

    if error_sample < config.error_rate {
        return MockOutcome::InternalError;
    }

    MockOutcome::Success
}

pub fn extract_request_options(body: &Value) -> (String, bool) {
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let stream = body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    (model, stream)
}

pub fn build_rate_limit_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::json!({
            "error": { "message": "Rate limit exceeded", "type": "rate_limit_error" }
        })),
    )
        .into_response()
}

pub fn build_internal_error_response() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "error": { "message": "Mock server error", "type": "server_error" }
        })),
    )
        .into_response()
}

pub fn build_success_body(model: &str) -> Value {
    serde_json::json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": 1234567890,
        "model": model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "This is a mock response from the test server."
            },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 20,
            "total_tokens": 30
        }
    })
}

pub async fn chat_handler(
    State(config): State<Arc<MockConfig>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let outcome = decide_outcome(&config, rand::random::<f32>(), rand::random::<f32>());

    match outcome {
        MockOutcome::RateLimited => build_rate_limit_response(),
        MockOutcome::InternalError => build_internal_error_response(),
        MockOutcome::Success => {
            let (model, stream) = extract_request_options(&body);
            if stream {
                let delay_ms = config.delay_ms;
                let stream = mock_sse_stream(model, delay_ms);
                Sse::new(stream)
                    .keep_alive(KeepAlive::default())
                    .into_response()
            } else {
                sleep(Duration::from_millis(config.delay_ms)).await;
                (StatusCode::OK, Json(build_success_body(&model))).into_response()
            }
        }
    }
}

pub fn mock_sse_stream(
    model: String,
    delay_ms: u64,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let req_id = format!("chatcmpl-{}", uuid_part());
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_secs();

    let chunks = vec![
        "This",
        " is",
        " a",
        " mock",
        " response",
        " for",
        " streaming",
        " testing",
        ".",
    ];

    let events: Vec<Event> = chunks
        .into_iter()
        .map(|chunk| {
            Event::default().data(
                serde_json::json!({
                    "id": &req_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": &model,
                    "choices": [{
                        "index": 0,
                        "delta": {"content": chunk},
                        "finish_reason": null
                    }]
                })
                .to_string(),
            )
        })
        .chain(std::iter::once(
            Event::default().data(
                serde_json::json!({
                    "id": &req_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": &model,
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop"
                    }]
                })
                .to_string(),
            ),
        ))
        .chain(std::iter::once(Event::default().data("[DONE]")))
        .collect();

    let chunk_delay = Duration::from_millis(delay_ms / 2);
    let initial_delay = Duration::from_millis(delay_ms);

    async_stream::stream! {
        sleep(initial_delay).await;
        for event in events {
            yield Ok(event);
            sleep(chunk_delay).await;
        }
    }
}

pub fn uuid_part() -> String {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    format!("{:x}", t % 0xFFFF_FFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decide_outcome_rate_limited_wins_first() {
        let config = MockConfig {
            delay_ms: 0,
            error_rate: 1.0,
            rate_limit_rate: 1.0,
        };
        assert_eq!(decide_outcome(&config, 0.0, 0.0), MockOutcome::RateLimited);
    }

    #[test]
    fn test_decide_outcome_internal_error() {
        let config = MockConfig {
            delay_ms: 0,
            error_rate: 1.0,
            rate_limit_rate: 0.0,
        };
        assert_eq!(
            decide_outcome(&config, 0.5, 0.0),
            MockOutcome::InternalError
        );
    }

    #[test]
    fn test_decide_outcome_success() {
        let config = MockConfig {
            delay_ms: 0,
            error_rate: 0.0,
            rate_limit_rate: 0.0,
        };
        assert_eq!(decide_outcome(&config, 0.5, 0.5), MockOutcome::Success);
    }

    #[test]
    fn test_extract_request_options_defaults() {
        let body = serde_json::json!({});
        assert_eq!(
            extract_request_options(&body),
            ("unknown".to_string(), false)
        );
    }

    #[test]
    fn test_extract_request_options_reads_model_and_stream() {
        let body = serde_json::json!({"model": "glm-5.1", "stream": true});
        assert_eq!(
            extract_request_options(&body),
            ("glm-5.1".to_string(), true)
        );
    }

    #[test]
    fn test_build_success_body_keeps_expected_shape() {
        let body = build_success_body("glm-5.1");
        assert_eq!(body["model"], "glm-5.1");
        assert_eq!(body["usage"]["total_tokens"], 30);
        assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    }

    #[test]
    fn test_uuid_part_is_hex() {
        let value = uuid_part();
        assert!(!value.is_empty());
        assert!(value.chars().all(|ch| ch.is_ascii_hexdigit()));
    }
}
