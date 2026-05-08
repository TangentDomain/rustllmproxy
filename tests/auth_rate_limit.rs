mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use common::{
    openai_chat_body, spawn_mock, spawn_proxy, TestBackend, TestConfigBuilder, TEST_API_KEY,
};
use llmproxy::middleware::extract_client_credential_from_request;
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backends_route_stays_responsive_under_repeated_authenticated_requests() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (mock_addr, mock_handle) = spawn_counting_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .rate_limit(10_000)
        .backend(TestBackend::openai("mock-backend", mock_addr))
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();
    let url = format!("http://{proxy_addr}/backends");

    for _ in 0..128 {
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client
                .get(&url)
                .header("Authorization", format!("Bearer {TEST_API_KEY}"))
                .send(),
        )
        .await
        .expect("backends request should not stall")
        .expect("backends request");
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    proxy_handle.abort();
    mock_handle.abort();
}

#[test]
fn shared_credential_extraction_preserves_existing_bearer_and_query_behavior() {
    let header_request = Request::builder()
        .uri("/openai/v1/chat/completions?api_key=query-key")
        .header("authorization", "Bearer header-key")
        .body(())
        .expect("header request");
    assert_eq!(
        extract_client_credential_from_request(&header_request).as_deref(),
        Some("header-key")
    );

    let query_request = Request::builder()
        .uri("/openai/v1/chat/completions?foo=1&api_key=query-key&bar=2")
        .body(())
        .expect("query request");
    assert_eq!(
        extract_client_credential_from_request(&query_request).as_deref(),
        Some("query-key")
    );

    let lowercase_bearer_request = Request::builder()
        .uri("/openai/v1/chat/completions?api_key=query-key")
        .header("authorization", "bearer lower-key")
        .body(())
        .expect("lowercase bearer request");
    assert_eq!(
        extract_client_credential_from_request(&lowercase_bearer_request).as_deref(),
        Some("query-key")
    );

    let spaced_bearer_request = Request::builder()
        .uri("/openai/v1/chat/completions")
        .header("authorization", "Bearer  spaced-key")
        .body(())
        .expect("spaced bearer request");
    assert_eq!(
        extract_client_credential_from_request(&spaced_bearer_request).as_deref(),
        Some(" spaced-key")
    );

    let malformed_query_request = Request::builder()
        .uri("/openai/v1/chat/completions?x=1&api_key=&y=2")
        .body(())
        .expect("malformed query request");
    assert_eq!(
        extract_client_credential_from_request(&malformed_query_request).as_deref(),
        Some("")
    );
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
        "id": "auth-test",
        "object": "chat.completion",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "auth-ok"},
            "finish_reason": "stop"
        }]
    }))
}
