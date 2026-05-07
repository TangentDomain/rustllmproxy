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
async fn falls_back_on_429_and_5xx_to_next_backend() {
    for first_status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::INTERNAL_SERVER_ERROR] {
        let first_hits = Arc::new(AtomicUsize::new(0));
        let second_hits = Arc::new(AtomicUsize::new(0));
        let (first_addr, first_handle) = spawn_status_mock(first_status, Arc::clone(&first_hits)).await;
        let (second_addr, second_handle) = spawn_success_mock(Arc::clone(&second_hits), "fallback-ok").await;
        let config = TestConfigBuilder::new()
            .backend(TestBackend::openai("first-backend", first_addr).with_models(&["mock-model"]))
            .backend(TestBackend::openai("second-backend", second_addr).with_models(&["fallback-model"]))
            .fallback_chain("mock-model", &["fallback-model"])
            .build();
        let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
        let client = Client::new();

        let resp = client
            .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
            .header("Authorization", format!("Bearer {TEST_API_KEY}"))
            .header("Content-Type", "application/json")
            .body(openai_chat_body("mock-model", "fallback"))
            .send()
            .await
            .expect("fallback request");
        let backend = resp
            .headers()
            .get("x-backend")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let status = resp.status();
        let body = resp.text().await.expect("response body");

        assert_eq!(status, StatusCode::OK, "body={body}");
        assert_eq!(backend.as_deref(), Some("second-backend"));
        assert!(body.contains("fallback-ok"), "body={body}");
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 1);

        proxy_handle.abort();
        first_handle.abort();
        second_handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returns_401_and_422_directly_without_fallback() {
    for direct_status in [StatusCode::UNAUTHORIZED, StatusCode::UNPROCESSABLE_ENTITY] {
        let first_hits = Arc::new(AtomicUsize::new(0));
        let second_hits = Arc::new(AtomicUsize::new(0));
        let (first_addr, first_handle) = spawn_status_mock(direct_status, Arc::clone(&first_hits)).await;
        let (second_addr, second_handle) = spawn_success_mock(Arc::clone(&second_hits), "should-not-hit").await;
        let config = TestConfigBuilder::new()
            .backend(TestBackend::openai("first-backend", first_addr).with_models(&["mock-model"]))
            .backend(TestBackend::openai("second-backend", second_addr).with_models(&["fallback-model"]))
            .fallback_chain("mock-model", &["fallback-model"])
            .build();
        let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
        let client = Client::new();

        let resp = client
            .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
            .header("Authorization", format!("Bearer {TEST_API_KEY}"))
            .header("Content-Type", "application/json")
            .body(openai_chat_body("mock-model", "direct"))
            .send()
            .await
            .expect("direct client error request");

        assert_eq!(resp.status(), direct_status);
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 0);

        proxy_handle.abort();
        first_handle.abort();
        second_handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn falls_back_on_provider_specific_4xx_but_reports_503_when_exhausted() {
    let first_hits = Arc::new(AtomicUsize::new(0));
    let second_hits = Arc::new(AtomicUsize::new(0));
    let (first_addr, first_handle) = spawn_status_mock(StatusCode::BAD_REQUEST, Arc::clone(&first_hits)).await;
    let (second_addr, second_handle) = spawn_status_mock(StatusCode::FORBIDDEN, Arc::clone(&second_hits)).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("first-backend", first_addr).with_models(&["mock-model"]))
        .backend(TestBackend::openai("second-backend", second_addr).with_models(&["fallback-model"]))
        .fallback_chain("mock-model", &["fallback-model"])
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(openai_chat_body("mock-model", "exhausted"))
        .send()
        .await
        .expect("exhausted request");
    let status = resp.status();
    let text = resp.text().await.expect("json error body");
    let body: serde_json::Value = serde_json::from_str(&text).expect("parse json error body");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], "All backends exhausted");
    assert_eq!(first_hits.load(Ordering::SeqCst), 1);
    assert_eq!(second_hits.load(Ordering::SeqCst), 1);

    proxy_handle.abort();
    first_handle.abort();
    second_handle.abort();
}

async fn spawn_status_mock(
    status: StatusCode,
    hits: Arc<AtomicUsize>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/v1/chat/completions", post(status_handler))
        .with_state((status, hits));
    spawn_mock(app).await
}

async fn spawn_success_mock(
    hits: Arc<AtomicUsize>,
    content: &'static str,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/v1/chat/completions", post(success_handler))
        .with_state((hits, content));
    spawn_mock(app).await
}

async fn status_handler(
    State((status, hits)): State<(StatusCode, Arc<AtomicUsize>)>,
    Json(payload): Json<serde_json::Value>,
) -> (StatusCode, String) {
    assert!(payload["model"] == "mock-model" || payload["model"] == "fallback-model");
    hits.fetch_add(1, Ordering::SeqCst);
    (status, format!("mock status {}", status.as_u16()))
}

async fn success_handler(
    State((hits, content)): State<(Arc<AtomicUsize>, &'static str)>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    assert!(payload["model"] == "mock-model" || payload["model"] == "fallback-model");
    hits.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!({
        "id": "fallback-test",
        "object": "chat.completion",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop"
        }]
    }))
}
