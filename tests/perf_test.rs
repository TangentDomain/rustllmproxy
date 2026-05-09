use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use llmproxy::config::{ApiKey, AuthConfig, Backend, Config, ServerConfig};
use llmproxy::proxy::run_server_with_listener;
use reqwest::Client;
use tokio::net::TcpListener;
use tokio::time::timeout;

const TOTAL_REQUESTS: usize = 64;
const CONCURRENCY: usize = 16;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_handles_concurrent_mock_requests_without_shell_tools() {
    let mock_hits = Arc::new(AtomicUsize::new(0));
    let mock_listener = bind_random_listener().await;
    let mock_addr = mock_listener.local_addr().expect("mock addr");
    let mock_state = Arc::clone(&mock_hits);
    let mock_handle = tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/chat/completions", post(mock_chat_completions))
            .with_state(mock_state);
        axum::serve(mock_listener, app)
            .await
            .expect("mock server error");
    });

    let proxy_listener = bind_random_listener().await;
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    let config = make_config(mock_addr);
    let proxy_handle = tokio::spawn(async move {
        let extra_routes =
            Router::new().route("/openai/v1/{*path}", post(openai_passthrough_handler));
        run_server_with_listener(config, extra_routes, proxy_listener).await;
    });

    let result = timeout(Duration::from_secs(10), async {
        let client = Client::new();
        let url = format!("http://{proxy_addr}/openai/v1/chat/completions");
        let body = serde_json::json!({
            "model": "mock-model",
            "messages": [{"role": "user", "content": "perf smoke"}]
        });
        let body = serde_json::to_string(&body).expect("serialize body");

        for batch_start in (0..TOTAL_REQUESTS).step_by(CONCURRENCY) {
            let batch_size = CONCURRENCY.min(TOTAL_REQUESTS - batch_start);
            let mut tasks = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                let client = client.clone();
                let url = url.clone();
                let body = body.clone();
                tasks.push(tokio::spawn(async move {
                    let resp = client
                        .post(url)
                        .header("Authorization", "Bearer sk-proxy-default")
                        .header("Content-Type", "application/json")
                        .body(body)
                        .send()
                        .await
                        .expect("request should complete");
                    let status = resp.status();
                    let text = resp.text().await.expect("response body");
                    assert_eq!(status, StatusCode::OK, "unexpected response body: {text}");
                    assert!(text.contains("perf-ok"), "unexpected response body: {text}");
                }));
            }

            for task in tasks {
                task.await.expect("request task should not panic");
            }
        }
    })
    .await;

    assert!(result.is_ok(), "concurrent mock perf test timed out");
    assert_eq!(
        mock_hits.load(Ordering::SeqCst),
        TOTAL_REQUESTS,
        "proxy should forward exactly one backend request per client request"
    );

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
    State(hits): State<Arc<AtomicUsize>>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    assert_eq!(payload["model"], "mock-model");
    hits.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!({
        "id": "mock-perf",
        "object": "chat.completion",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "perf-ok"
            },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2
        }
    }))
}

fn make_config(mock_addr: SocketAddr) -> Config {
    let backend = Backend {
        name: "mock-backend".to_string(),
        url: format!("http://{mock_addr}"),
        api_key: "mock-key".to_string(),
        weight: 1,
        models: vec!["mock-model".to_string()],
        timeout_secs: 10,
        connect_timeout_secs: 1,
        model_mappings: HashMap::new(),
        protocol: "openai".to_string(),
        auth_header: "Bearer mock-key".to_string(),
        strip_params: vec![],
    };

    Config {
        server: ServerConfig {
            port: 0,
            timeout_secs: 10,
            log_dir: "logs-test".to_string(),
            stream_idle_timeout_secs: 10,
            stream_first_chunk_timeout_secs: 10,
            fallback_timeout_secs: 10,
            recovery_cooldown_secs: 60,
        },
        r#type: "openai".to_string(),
        auth: AuthConfig {
            enabled: true,
            keys: vec![ApiKey {
                key: "sk-proxy-default".to_string(),
                name: "default".to_string(),
                rate_limit: 1_000,
            }],
            key_index: HashMap::from([(String::from("sk-proxy-default"), 0usize)]),
        },
        backends: vec![backend],
        retry: 0,
        retry_delay_ms: 0,
        fallback: HashMap::new(),
        model_mapping: HashMap::new(),
    }
}

async fn bind_random_listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener")
}
