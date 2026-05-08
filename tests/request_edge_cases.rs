mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use common::{spawn_mock, spawn_proxy, TestBackend, TestConfigBuilder, TEST_API_KEY};
use reqwest::Client;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_model_is_rejected_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (mock_addr, mock_handle) = spawn_counting_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("mock-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(r#"{"messages":[{"role":"user","content":"missing model"}]}"#)
        .send()
        .await
        .expect("missing model request");
    let status = resp.status();
    let text = resp.text().await.expect("missing model body");

    assert_eq!(status, StatusCode::BAD_REQUEST, "body={text}");
    assert!(text.contains("model"), "body={text}");
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    proxy_handle.abort();
    mock_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_model_returns_503_without_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (mock_addr, mock_handle) = spawn_counting_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("mock-backend", mock_addr).with_models(&["supported-model"]))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let body = serde_json::to_string(&serde_json::json!({
        "model": "unknown-model",
        "messages": [{"role": "user", "content": "unsupported"}]
    }))
    .expect("serialize unsupported model body");
    let resp = client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .expect("unsupported model request");
    let status = resp.status();
    let text = resp.text().await.expect("unsupported model body");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body={text}");
    assert!(text.contains("All backends exhausted"), "body={text}");
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    proxy_handle.abort();
    mock_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disabled_auth_bypasses_api_key_check_and_forwards() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (mock_addr, mock_handle) = spawn_counting_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .auth_enabled(false)
        .backend(TestBackend::openai("mock-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let body = serde_json::to_string(&serde_json::json!({
        "model": "mock-model",
        "messages": [{"role": "user", "content": "auth disabled"}]
    }))
    .expect("serialize auth disabled body");
    let resp = client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .expect("auth disabled request");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    proxy_handle.abort();
    mock_handle.abort();
}

async fn spawn_counting_mock(
    hits: Arc<AtomicUsize>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/v1/chat/completions", post(counting_openai_handler))
        .with_state(hits);
    spawn_mock(app).await
}

async fn counting_openai_handler(
    State(hits): State<Arc<AtomicUsize>>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    assert_eq!(payload["model"], "mock-model");
    hits.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!({
        "id": "edge-case-test",
        "object": "chat.completion",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "edge-ok"},
            "finish_reason": "stop"
        }]
    }))
}
