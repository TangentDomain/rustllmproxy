mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use common::{
    openai_chat_body, proxy_introspection_routes, spawn_mock, spawn_proxy_with_routes, TestBackend,
    TestConfigBuilder, TEST_API_KEY,
};
use reqwest::Client;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn falls_back_on_429_and_5xx_to_next_backend() {
    for first_status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
    ] {
        let first_hits = Arc::new(AtomicUsize::new(0));
        let second_hits = Arc::new(AtomicUsize::new(0));
        let (first_addr, first_handle) =
            spawn_status_mock(first_status, Arc::clone(&first_hits)).await;
        let (second_addr, second_handle) =
            spawn_success_mock(Arc::clone(&second_hits), "fallback-ok").await;
        let config = TestConfigBuilder::new()
            .backend(
                TestBackend::openai("first-backend", first_addr)
                    .with_models(&["mock-model"])
                    .with_connect_timeout_secs(1),
            )
            .backend(
                TestBackend::openai("second-backend", second_addr)
                    .with_models(&["fallback-model"])
                    .with_connect_timeout_secs(1),
            )
            .fallback_chain("mock-model", &["fallback-model"])
            .build();
        let (proxy_addr, proxy_handle) = spawn_proxy_with_routes(
            config,
            common::protocol_routes().merge(proxy_introspection_routes()),
        )
        .await;
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
        let (first_addr, first_handle) =
            spawn_status_mock(direct_status, Arc::clone(&first_hits)).await;
        let (second_addr, second_handle) =
            spawn_success_mock(Arc::clone(&second_hits), "should-not-hit").await;
        let config = TestConfigBuilder::new()
            .backend(
                TestBackend::openai("first-backend", first_addr)
                    .with_models(&["mock-model"])
                    .with_connect_timeout_secs(1),
            )
            .backend(
                TestBackend::openai("second-backend", second_addr)
                    .with_models(&["fallback-model"])
                    .with_connect_timeout_secs(1),
            )
            .fallback_chain("mock-model", &["fallback-model"])
            .build();
        let (proxy_addr, proxy_handle) = spawn_proxy_with_routes(
            config,
            common::protocol_routes().merge(proxy_introspection_routes()),
        )
        .await;
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
        assert!(resp.headers().get("x-request-id").is_some());
        assert!(resp.headers().get("x-backend").is_some());
        assert!(resp.headers().get("x-ttfb-ms").is_some());
        assert!(resp.headers().get("x-total-ms").is_some());

        proxy_handle.abort();
        first_handle.abort();
        second_handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn falls_back_on_provider_specific_4xx_but_reports_503_when_exhausted() {
    let first_hits = Arc::new(AtomicUsize::new(0));
    let second_hits = Arc::new(AtomicUsize::new(0));
    let (first_addr, first_handle) =
        spawn_status_mock(StatusCode::BAD_REQUEST, Arc::clone(&first_hits)).await;
    let (second_addr, second_handle) =
        spawn_status_mock(StatusCode::FORBIDDEN, Arc::clone(&second_hits)).await;
    let config = TestConfigBuilder::new()
        .backend(
            TestBackend::openai("first-backend", first_addr)
                .with_models(&["mock-model"])
                .with_connect_timeout_secs(1),
        )
        .backend(
            TestBackend::openai("second-backend", second_addr)
                .with_models(&["fallback-model"])
                .with_connect_timeout_secs(1),
        )
        .fallback_chain("mock-model", &["fallback-model"])
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy_with_routes(
        config,
        common::protocol_routes().merge(proxy_introspection_routes()),
    )
    .await;
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
    let headers = resp.headers().clone();
    let text = resp.text().await.expect("json error body");
    let body: serde_json::Value = serde_json::from_str(&text).expect("parse json error body");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"], "All backends exhausted");
    assert!(headers.get("x-request-id").is_some());
    assert_eq!(
        headers
            .get("x-backend")
            .and_then(|value| value.to_str().ok()),
        Some("exhausted")
    );
    assert_eq!(
        headers
            .get("x-ttfb-ms")
            .and_then(|value| value.to_str().ok()),
        Some("0")
    );
    assert!(headers.get("x-total-ms").is_some());
    assert_eq!(first_hits.load(Ordering::SeqCst), 1);
    assert_eq!(second_hits.load(Ordering::SeqCst), 1);

    let backends_resp = client
        .get(format!("http://{proxy_addr}/backends"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .send()
        .await
        .expect("backends request after provider 4xx");
    assert_eq!(backends_resp.status(), StatusCode::OK);
    let backends_text = backends_resp.text().await.expect("backends body");
    let backends_json: serde_json::Value =
        serde_json::from_str(&backends_text).expect("backends json");
    for backend_name in ["first-backend", "second-backend"] {
        let backend = backends_json["backends"]
            .as_array()
            .expect("backends array")
            .iter()
            .find(|backend| backend["name"] == backend_name)
            .expect("backend entry");
        assert_eq!(backend["healthy"], true);
        assert_eq!(backend["fail_count"], 0);
    }

    proxy_handle.abort();
    first_handle.abort();
    second_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_server_errors_mark_backend_unhealthy_but_429_does_not() {
    let healthy_hits = Arc::new(AtomicUsize::new(0));
    let rate_limited_hits = Arc::new(AtomicUsize::new(0));

    let (healthy_addr, healthy_handle) =
        spawn_success_mock(Arc::clone(&healthy_hits), "healthy").await;
    let (rate_limited_addr, rate_limited_handle) = spawn_status_mock(
        StatusCode::TOO_MANY_REQUESTS,
        Arc::clone(&rate_limited_hits),
    )
    .await;

    let config = TestConfigBuilder::new()
        .backend(
            TestBackend::openai("rate-limited-backend", rate_limited_addr)
                .with_models(&["ratelimit-model"])
                .with_connect_timeout_secs(1),
        )
        .backend(
            TestBackend::openai("healthy-backend", healthy_addr)
                .with_models(&["ratelimit-fallback-model"])
                .with_connect_timeout_secs(1),
        )
        .fallback_chain("ratelimit-model", &["ratelimit-fallback-model"])
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy_with_routes(
        config,
        common::protocol_routes().merge(proxy_introspection_routes()),
    )
    .await;
    let client = Client::new();

    for _ in 0..3 {
        let resp = client
            .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
            .header("Authorization", format!("Bearer {TEST_API_KEY}"))
            .header("Content-Type", "application/json")
            .body(openai_chat_body("ratelimit-model", "keep healthy"))
            .send()
            .await
            .expect("429 fallback request");
        assert_eq!(resp.status(), StatusCode::OK);
        let backend = resp
            .headers()
            .get("x-backend")
            .and_then(|value| value.to_str().ok());
        assert_eq!(backend, Some("healthy-backend"));
    }

    let backends_resp = client
        .get(format!("http://{proxy_addr}/backends"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .send()
        .await
        .expect("backends request after 429");
    assert_eq!(backends_resp.status(), StatusCode::OK);
    let backends_text = backends_resp.text().await.expect("backends body");
    let backends_json: serde_json::Value =
        serde_json::from_str(&backends_text).expect("backends json");
    let rate_limited_backend = backends_json["backends"]
        .as_array()
        .expect("backends array")
        .iter()
        .find(|backend| backend["name"] == "rate-limited-backend")
        .expect("rate-limited backend entry");
    assert_eq!(rate_limited_backend["healthy"], true);

    proxy_handle.abort();
    healthy_handle.abort();
    rate_limited_handle.abort();

    let unreachable_addr: std::net::SocketAddr = "127.0.0.1:9".parse().expect("discard port addr");
    let healthy_hits = Arc::new(AtomicUsize::new(0));
    let (healthy_addr, healthy_handle) =
        spawn_success_mock(Arc::clone(&healthy_hits), "healthy").await;
    let config = TestConfigBuilder::new()
        .backend(
            TestBackend::openai("broken-backend", unreachable_addr)
                .with_models(&["broken-model"])
                .with_connect_timeout_secs(1),
        )
        .backend(
            TestBackend::openai("healthy-backend", healthy_addr)
                .with_models(&["healthy-model"])
                .with_connect_timeout_secs(1),
        )
        .fallback_chain("broken-model", &["healthy-model"])
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy_with_routes(
        config,
        common::protocol_routes().merge(proxy_introspection_routes()),
    )
    .await;

    for _ in 0..3 {
        let resp = client
            .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
            .header("Authorization", format!("Bearer {TEST_API_KEY}"))
            .header("Content-Type", "application/json")
            .body(openai_chat_body("broken-model", "trip unhealthy"))
            .send()
            .await
            .expect("server error fallback request");
        assert_eq!(resp.status(), StatusCode::OK);
        let backend = resp
            .headers()
            .get("x-backend")
            .and_then(|value| value.to_str().ok());
        assert_eq!(backend, Some("healthy-backend"));
    }

    sleep(Duration::from_millis(50)).await;
    let backends_resp = client
        .get(format!("http://{proxy_addr}/backends"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .send()
        .await
        .expect("backends request after server errors");
    assert_eq!(backends_resp.status(), StatusCode::OK);
    let backends_text = backends_resp.text().await.expect("backends body");
    let backends_json: serde_json::Value =
        serde_json::from_str(&backends_text).expect("backends json");
    let broken_backend = backends_json["backends"]
        .as_array()
        .expect("backends array")
        .iter()
        .find(|backend| backend["name"] == "broken-backend")
        .expect("broken backend entry");
    assert_eq!(broken_backend["healthy"], false);
    assert_eq!(broken_backend["fail_count"], 3);

    proxy_handle.abort();
    healthy_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_stream_success_records_metrics_without_corrupting_response() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (backend_addr, backend_handle) = spawn_success_with_usage_mock(Arc::clone(&hits)).await;
    let config = TestConfigBuilder::new()
        .backend(
            TestBackend::openai("metrics-backend", backend_addr)
                .with_models(&["metrics-model"])
                .with_connect_timeout_secs(1),
        )
        .build();
    let (proxy_addr, proxy_handle) = spawn_proxy_with_routes(
        config,
        common::protocol_routes().merge(proxy_introspection_routes()),
    )
    .await;
    let client = Client::new();

    let resp = client
        .post(format!("http://{proxy_addr}/openai/v1/chat/completions"))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .header("Content-Type", "application/json")
        .body(openai_chat_body("metrics-model", "record metrics"))
        .send()
        .await
        .expect("metrics request");
    assert_eq!(resp.status(), StatusCode::OK);
    let headers = resp.headers().clone();
    let body_text = resp.text().await.expect("response body");
    let body_json: serde_json::Value = serde_json::from_str(&body_text).expect("response json");
    assert_eq!(body_json["choices"][0]["message"]["content"], "metrics-ok");
    assert_eq!(body_json["usage"]["completion_tokens"], 8);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        headers
            .get("x-backend")
            .and_then(|value| value.to_str().ok()),
        Some("metrics-backend")
    );
    assert!(headers.get("x-request-id").is_some());
    assert!(headers.get("x-ttfb-ms").is_some());
    assert!(headers.get("x-total-ms").is_some());

    let backend_metrics = client
        .get(format!(
            "http://{proxy_addr}/test/backend-metrics/metrics-backend"
        ))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .send()
        .await
        .expect("backend metrics request");
    assert_eq!(backend_metrics.status(), StatusCode::OK);
    let backend_metrics_text = backend_metrics.text().await.expect("backend metrics body");
    let backend_metrics_json: serde_json::Value =
        serde_json::from_str(&backend_metrics_text).expect("backend metrics json");
    let backend_avg = backend_metrics_json["avg_tok_per_sec"]
        .as_f64()
        .expect("backend avg tok/s");
    assert!(backend_avg > 0.0, "backend_avg={backend_avg}");

    let model_metrics = client
        .get(format!(
            "http://{proxy_addr}/test/model-metrics/metrics-model"
        ))
        .header("Authorization", format!("Bearer {TEST_API_KEY}"))
        .send()
        .await
        .expect("model metrics request");
    assert_eq!(model_metrics.status(), StatusCode::OK);
    let model_metrics_text = model_metrics.text().await.expect("model metrics body");
    let model_metrics_json: serde_json::Value =
        serde_json::from_str(&model_metrics_text).expect("model metrics json");
    let model_avg = model_metrics_json["avg_tok_per_sec"]
        .as_f64()
        .expect("model avg tok/s");
    assert!(model_avg > 0.0, "model_avg={model_avg}");

    proxy_handle.abort();
    backend_handle.abort();
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

async fn spawn_success_with_usage_mock(
    hits: Arc<AtomicUsize>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/v1/chat/completions", post(success_with_usage_handler))
        .with_state(hits);
    spawn_mock(app).await
}

async fn status_handler(
    State((status, hits)): State<(StatusCode, Arc<AtomicUsize>)>,
    Json(payload): Json<serde_json::Value>,
) -> (StatusCode, String) {
    assert!(
        payload["model"] == "mock-model"
            || payload["model"] == "fallback-model"
            || payload["model"] == "ratelimit-model"
            || payload["model"] == "ratelimit-fallback-model"
            || payload["model"] == "broken-model"
            || payload["model"] == "healthy-model"
    );
    hits.fetch_add(1, Ordering::SeqCst);
    (status, format!("mock status {}", status.as_u16()))
}

async fn success_handler(
    State((hits, content)): State<(Arc<AtomicUsize>, &'static str)>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    assert!(
        payload["model"] == "mock-model"
            || payload["model"] == "fallback-model"
            || payload["model"] == "ratelimit-model"
            || payload["model"] == "ratelimit-fallback-model"
            || payload["model"] == "healthy-model"
    );
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

async fn success_with_usage_handler(
    State(hits): State<Arc<AtomicUsize>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    assert_eq!(payload["model"], "metrics-model");
    hits.fetch_add(1, Ordering::SeqCst);
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    (
        headers,
        Json(serde_json::json!({
            "id": "metrics-test",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "metrics-ok"},
                "finish_reason": "stop"
            }],
            "usage": {
                "completion_tokens": 8
            }
        })),
    )
}
