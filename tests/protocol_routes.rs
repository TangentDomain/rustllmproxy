mod common;

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::post;
use axum::{Json, Router};
use common::{
    anthropic_messages_body, openai_chat_body, spawn_mock, spawn_proxy, TestBackend,
    TestConfigBuilder, TEST_API_KEY,
};
use reqwest::Client;

#[derive(Clone, Debug)]
struct CapturedRequest {
    path: String,
    auth: Option<String>,
    x_api_key: Option<String>,
    anthropic_version: Option<String>,
    model: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_route_strips_prefix_sets_auth_and_applies_model_mapping() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let (mock_addr, mock_handle) =
        spawn_capture_mock("/v1/chat/completions", Arc::clone(&captured)).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("openai-backend", mock_addr).with_models(&["backend-model"]))
        .model_mapping("alias-model", &["backend-model"])
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(openai_chat_body("alias-model", "route"))
        .send()
        .await
        .expect("openai route request");
    let status = resp.status();
    let body = resp.text().await.expect("response body");

    assert_eq!(status, StatusCode::OK, "body={body}");
    let requests = captured.lock().expect("captured requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/chat/completions");
    assert_eq!(requests[0].auth.as_deref(), Some("Bearer mock-key"));
    assert_eq!(requests[0].x_api_key, None);
    assert_eq!(requests[0].model, "backend-model");

    proxy_handle.abort();
    mock_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_route_strips_prefix_sets_anthropic_headers_and_filters_protocol() {
    let openai_hits = Arc::new(Mutex::new(Vec::new()));
    let anthropic_hits = Arc::new(Mutex::new(Vec::new()));
    let (openai_addr, openai_handle) =
        spawn_capture_mock("/v1/messages", Arc::clone(&openai_hits)).await;
    let (anthropic_addr, anthropic_handle) =
        spawn_capture_mock("/v1/messages", Arc::clone(&anthropic_hits)).await;
    let config = TestConfigBuilder::new()
        .backend(TestBackend::openai("openai-backend", openai_addr).with_models(&["mock-model"]))
        .backend(
            TestBackend::anthropic("anthropic-backend", anthropic_addr)
                .with_models(&["mock-model"]),
        )
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{proxy_addr}/anthropic/v1/messages"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(anthropic_messages_body("mock-model", "route"))
        .send()
        .await
        .expect("anthropic route request");
    let backend = resp
        .headers()
        .get("x-backend")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let status = resp.status();
    let body = resp.text().await.expect("response body");

    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(backend.as_deref(), Some("anthropic-backend"));
    assert!(openai_hits.lock().expect("openai hits").is_empty());

    let requests = anthropic_hits.lock().expect("anthropic hits");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/messages");
    assert_eq!(requests[0].auth, None);
    assert_eq!(requests[0].x_api_key.as_deref(), Some("mock-key"));
    assert_eq!(requests[0].anthropic_version.as_deref(), Some("2023-06-01"));
    assert_eq!(requests[0].model, "mock-model");

    proxy_handle.abort();
    openai_handle.abort();
    anthropic_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn model_list_routes_return_protocol_specific_shapes() {
    let (mock_addr, mock_handle) =
        spawn_capture_mock("/unused", Arc::new(Mutex::new(Vec::new()))).await;
    let config = TestConfigBuilder::new()
        .backend(
            TestBackend::openai("openai-backend", mock_addr).with_models(&["model-a", "model-b"]),
        )
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = Client::new();

    let openai = client
        .get(format!("http://{proxy_addr}/openai/v1/models"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .send()
        .await
        .expect("openai models");
    assert_eq!(openai.status(), StatusCode::OK);
    let openai_text = openai.text().await.expect("openai models body");
    let openai_json: serde_json::Value = serde_json::from_str(&openai_text).expect("openai json");
    assert_eq!(openai_json["object"], "list");
    assert!(openai_json["data"].as_array().expect("openai data").len() >= 2);

    let anthropic = client
        .get(format!("http://{proxy_addr}/anthropic/v1/models"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .send()
        .await
        .expect("anthropic models");
    assert_eq!(anthropic.status(), StatusCode::OK);
    let anthropic_text = anthropic.text().await.expect("anthropic models body");
    let anthropic_json: serde_json::Value =
        serde_json::from_str(&anthropic_text).expect("anthropic json");
    assert!(
        anthropic_json["models"]
            .as_array()
            .expect("anthropic models")
            .len()
            >= 2
    );

    proxy_handle.abort();
    mock_handle.abort();
}

async fn spawn_capture_mock(
    route: &'static str,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route(route, post(capture_handler))
        .with_state(captured);
    spawn_mock(app).await
}

async fn capture_handler(
    State(captured): State<Arc<Mutex<Vec<CapturedRequest>>>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Json<serde_json::Value> {
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("request json");
    captured
        .lock()
        .expect("captured requests")
        .push(CapturedRequest {
            path: uri.path().to_string(),
            auth: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            x_api_key: headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            anthropic_version: headers
                .get("anthropic-version")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            model: payload["model"].as_str().unwrap_or_default().to_string(),
        });

    Json(serde_json::json!({
        "id": "protocol-test",
        "object": "chat.completion",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "protocol-ok"},
            "finish_reason": "stop"
        }]
    }))
}
