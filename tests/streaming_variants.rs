mod common;

use std::convert::Infallible;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::post;
use axum::Router;
use common::{
    openai_chat_body, spawn_mock, spawn_proxy, TestBackend, TestConfigBuilder, TEST_API_KEY,
};
use futures::StreamExt;
use reqwest::Client;
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normal_openai_stream_completes_and_preserves_streaming_headers() {
    let (mock_addr, mock_handle) =
        spawn_mock(Router::new().route("/v1/chat/completions", post(normal_stream_handler))).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("stream-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;

    let resp = send_stream_request(proxy_addr, "mock-model").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("x-backend")
            .and_then(|v| v.to_str().ok()),
        Some("stream-backend")
    );
    assert_eq!(
        resp.headers()
            .get("x-accel-buffering")
            .and_then(|v| v.to_str().ok()),
        Some("no")
    );
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("no-cache")
    );
    assert!(resp.headers().get("x-request-id").is_some());

    let body = resp.text().await.expect("stream body");
    assert!(body.contains("hello"), "body={body}");
    assert!(body.contains("[DONE]"), "body={body}");

    proxy_handle.abort();
    mock_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_chunk_timeout_terminates_stream_before_any_sse_data() {
    let (mock_addr, mock_handle) =
        spawn_mock(Router::new().route("/v1/chat/completions", post(first_chunk_delayed_handler)))
            .await;
    let config = TestConfigBuilder::new()
        .timeout_secs(5)
        .stream_first_chunk_timeout_secs(1)
        .stream_idle_timeout_secs(5)
        .backend(TestBackend::openai("stream-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;

    let resp = send_stream_request(proxy_addr, "mock-model").await;
    let result = read_stream_to_end(resp, Duration::from_secs(4)).await;
    assert!(result.is_err(), "expected first-chunk timeout stream error");

    proxy_handle.abort();
    mock_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn total_stream_timeout_terminates_even_when_effective_chunks_keep_arriving() {
    let (mock_addr, mock_handle) =
        spawn_mock(Router::new().route("/v1/chat/completions", post(slow_never_done_handler)))
            .await;
    let config = TestConfigBuilder::new()
        .timeout_secs(1)
        .stream_first_chunk_timeout_secs(1)
        .stream_idle_timeout_secs(5)
        .backend(TestBackend::openai("stream-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;

    let resp = send_stream_request(proxy_addr, "mock-model").await;
    let result = read_stream_to_end(resp, Duration::from_secs(4)).await;
    assert!(result.is_err(), "expected total stream timeout error");

    proxy_handle.abort();
    mock_handle.abort();
}

async fn send_stream_request(proxy_addr: std::net::SocketAddr, model: &str) -> reqwest::Response {
    let client = Client::new();
    client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(openai_chat_body(model, "stream"))
        .send()
        .await
        .expect("stream request")
}

async fn read_stream_to_end(
    resp: reqwest::Response,
    max_duration: Duration,
) -> Result<(), reqwest::Error> {
    timeout(max_duration, async {
        let mut stream = resp.bytes_stream();
        while let Some(item) = stream.next().await {
            item?;
        }
        Ok::<(), reqwest::Error>(())
    })
    .await
    .expect("stream read should finish within test timeout")
}

async fn normal_stream_handler(
    axum::Json(payload): axum::Json<serde_json::Value>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    assert_eq!(payload["model"], "mock-model");
    let stream = async_stream::stream! {
        yield Ok(Event::default().data(r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":"hello"}}]}"#));
        yield Ok(Event::default().data("[DONE]"));
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn first_chunk_delayed_handler(
    axum::Json(payload): axum::Json<serde_json::Value>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    assert_eq!(payload["model"], "mock-model");
    let stream = async_stream::stream! {
        sleep(Duration::from_secs(2)).await;
        yield Ok(Event::default().data(r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":"late"}}]}"#));
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn slow_never_done_handler(
    axum::Json(payload): axum::Json<serde_json::Value>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    assert_eq!(payload["model"], "mock-model");
    let stream = async_stream::stream! {
        loop {
            yield Ok(Event::default().data(r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":"tick"}}]}"#));
            sleep(Duration::from_millis(200)).await;
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}
