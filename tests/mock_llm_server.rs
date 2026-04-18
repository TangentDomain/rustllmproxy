use std::time::Duration;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Json, IntoResponse},
    routing::post,
    Router,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::time::sleep;

#[derive(Clone)]
struct MockConfig {
    delay_ms: u64,
    error_rate: f32,
    rate_limit_rate: f32,
}

#[tokio::main]
async fn main() {
    let config = MockConfig {
        delay_ms: std::env::var("MOCK_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100),
        error_rate: std::env::var("MOCK_ERROR_RATE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        rate_limit_rate: std::env::var("MOCK_RATE_LIMIT_RATE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
    };

    let port: u16 = std::env::var("MOCK_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8765);

    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("Mock LLM server listening on http://{addr}");
    println!("Config: delay={}ms, error_rate={:.2}, rate_limit_rate={:.2}",
        config.delay_ms, config.error_rate, config.rate_limit_rate);
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_handler))
        .route("/v4/chat/completions", post(chat_handler))
        .with_state(Arc::new(config));

    axum::serve(listener, app).await.unwrap()
}

async fn chat_handler(
    State(config): State<Arc<MockConfig>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    sleep(Duration::from_millis(config.delay_ms)).await;

    if rand::random::<f32>() < config.rate_limit_rate {
        return (StatusCode::TOO_MANY_REQUESTS, Json(serde_json::json!({
            "error": { "message": "Rate limit exceeded", "type": "rate_limit_error" }
        })));
    }

    if rand::random::<f32>() < config.error_rate {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": { "message": "Mock server error", "type": "server_error" }
        })));
    }

    let model = body.get("model").and_then(|v| v.as_str()).unwrap_or("unknown");
    (StatusCode::OK, Json(serde_json::json!({
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
    })))
}
