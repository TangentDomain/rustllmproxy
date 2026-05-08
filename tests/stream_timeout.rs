use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::post;
use axum::Router;
use futures::StreamExt;
use reqwest::Client;
use tokio::time::{sleep, timeout};

mod common;

use common::{spawn_mock, spawn_proxy_with_routes, TestBackend, TestConfigBuilder, TEST_API_KEY};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_idle_timeout_triggers_without_effective_chunks() {
    let (mock_addr, mock_handle) =
        spawn_mock(Router::new().route("/v1/chat/completions", post(mock_chat_completions))).await;

    let config = TestConfigBuilder::new()
        .timeout_secs(3)
        .stream_idle_timeout_secs(1)
        .stream_first_chunk_timeout_secs(1)
        .fallback_timeout_secs(3)
        .backend(TestBackend::openai("mock-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy_with_routes(
        config,
        Router::new().route("/openai/v1/{*path}", post(openai_passthrough_handler)),
    )
    .await;

    let client = Client::new();
    let url = format!("http://{proxy_addr}/openai/v1/chat/completions");
    let body = serde_json::json!({
        "model": "mock-model",
        "stream": true,
        "messages": [{"role": "user", "content": "hi"}]
    });

    let resp = client
        .post(url)
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&body).expect("serialize body"))
        .send()
        .await
        .expect("request should start");

    assert_eq!(resp.status(), StatusCode::OK);

    let result = timeout(Duration::from_secs(5), async {
        let mut stream = resp.bytes_stream();
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok::<(), reqwest::Error>(())
    })
    .await;

    assert!(
        result.is_ok(),
        "stream did not terminate within timeout window"
    );
    let inner = result.unwrap();
    assert!(inner.is_err(), "expected stream to end with timeout error");

    proxy_handle.abort();
    mock_handle.abort();
}

async fn openai_passthrough_handler(
    State(proxy): State<Arc<llmproxy::proxy::Proxy>>,
    mut req: axum::http::Request<Body>,
) -> axum::response::Response<Body> {
    let uri = req.uri().to_string();
    let new_path = uri.replacen("/openai", "", 1);
    *req.uri_mut() = new_path.parse().expect("valid uri");
    req.extensions_mut().insert("openai".to_string());
    proxy.handle(req).await
}

async fn mock_chat_completions(
    axum::Json(payload): axum::Json<serde_json::Value>,
) -> Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>> {
    assert_eq!(payload["model"], "mock-model");
    let stream = async_stream::stream! {
        yield Ok(Event::default().data(r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"}}]}"#));
        sleep(Duration::from_secs(2)).await;
        yield Ok(Event::default().data(r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":"hello"}}]}"#));
        sleep(Duration::from_secs(2)).await;
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}
