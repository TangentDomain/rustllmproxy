mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use common::{openai_chat_body, spawn_mock, spawn_proxy, TestBackend, TestConfigBuilder, TEST_API_KEY};
use reqwest::Client;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_is_public_but_openai_routes_require_auth() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (mock_addr, mock_handle) = spawn_counting_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("mock-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let health = client
        .get(format!("http://{proxy_addr}/health"))
        .send()
        .await
        .expect("health request");
    assert_eq!(health.status(), StatusCode::OK);

    let protected = client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(openai_chat_body("mock-model", "hi"))
        .send()
        .await
        .expect("protected request");
    assert_eq!(protected.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    proxy_handle.abort();
    mock_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_and_valid_api_keys_are_enforced_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (mock_addr, mock_handle) = spawn_counting_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("mock-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();
    let url = format!("http://{proxy_addr}/openai/v1/chat/completions");

    let invalid = client
        .post(&url)
        .header("Authorization", "Bearer wrong-key")
        .header("Content-Type", "application/json")
        .body(openai_chat_body("mock-model", "invalid"))
        .send()
        .await
        .expect("invalid request");
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    let valid = client
        .post(&url)
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(openai_chat_body("mock-model", "valid"))
        .send()
        .await
        .expect("valid request");
    assert_eq!(valid.status(), StatusCode::OK);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    proxy_handle.abort();
    mock_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_api_key_authenticates_and_rate_limit_blocks_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (mock_addr, mock_handle) = spawn_counting_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .rate_limit(1)
        .backend(TestBackend::openai("mock-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();
    let url = format!("http://{proxy_addr}/openai/v1/chat/completions?api_key={TEST_API_KEY}");

    let first = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(openai_chat_body("mock-model", "first"))
        .send()
        .await
        .expect("first request");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    let second = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(openai_chat_body("mock-model", "second"))
        .send()
        .await
        .expect("second request");
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    proxy_handle.abort();
    mock_handle.abort();
}

async fn spawn_counting_mock(hits: Arc<AtomicUsize>) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
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
        "id": "auth-test",
        "object": "chat.completion",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "auth-ok"},
            "finish_reason": "stop"
        }]
    }))
}
