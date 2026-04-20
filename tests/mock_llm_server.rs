use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json,
    },
    routing::post,
    Router,
};
use futures::stream::Stream;
use serde_json::Value;
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
            .unwrap_or(50),
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
        .unwrap_or(8766);

    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("Mock LLM server listening on http://{addr}");
    println!(
        "Config: delay={}ms, error_rate={:.2}, rate_limit_rate={:.2}",
        config.delay_ms, config.error_rate, config.rate_limit_rate
    );
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
    if rand::random::<f32>() < config.rate_limit_rate {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": { "message": "Rate limit exceeded", "type": "rate_limit_error" }
            })),
        )
            .into_response();
    }

    if rand::random::<f32>() < config.error_rate {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": { "message": "Mock server error", "type": "server_error" }
            })),
        )
            .into_response();
    }

    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);

    if stream {
        let delay_ms = config.delay_ms;
        let stream = mock_sse_stream(model, delay_ms);
        let sse = Sse::new(stream).keep_alive(KeepAlive::default());
        sse.into_response()
    } else {
        sleep(Duration::from_millis(config.delay_ms)).await;
        (
            StatusCode::OK,
            Json(serde_json::json!({
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
            })),
        )
            .into_response()
    }
}

fn mock_sse_stream(
    model: String,
    delay_ms: u64,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let req_id = format!("chatcmpl-{}", uuid_part());
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let chunks = vec![
        "This", " is", " a", " mock", " response",
        " for", " streaming", " testing", ".",
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

fn uuid_part() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", t % 0xFFFF_FFFF)
}
