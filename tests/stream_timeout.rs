use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::post;
use axum::Router;
use futures::StreamExt;
use llmproxy::config::{ApiKey, AuthConfig, Backend, Config, ServerConfig};
use llmproxy::proxy::run_server_with_listener;
use reqwest::Client;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_idle_timeout_triggers_without_effective_chunks() {
    let mock_listener = bind_random_listener().await;
    let mock_addr = mock_listener.local_addr().expect("mock addr");
    let mock_handle = tokio::spawn(async move {
        let app = Router::new().route("/v1/chat/completions", post(mock_chat_completions));
        axum::serve(mock_listener, app)
            .await
            .expect("mock server error");
    });

    let proxy_listener = bind_random_listener().await;
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    let config = make_config(mock_addr);
    let proxy_handle = tokio::spawn(async move {
        let extra_routes = Router::new().route("/openai/v1/{*path}", post(openai_passthrough_handler));
        run_server_with_listener(config, extra_routes, proxy_listener).await;
    });

    let client = Client::new();
    let url = format!("http://{proxy_addr}/openai/v1/chat/completions");
    let body = serde_json::json!({
        "model": "mock-model",
        "stream": true,
        "messages": [{"role": "user", "content": "hi"}]
    });

    let resp = client
        .post(url)
        .header("Authorization", "Bearer sk-proxy-default")
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

    assert!(result.is_ok(), "stream did not terminate within timeout window");
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
            timeout_secs: 3,
            log_dir: "logs-test".to_string(),
            stream_idle_timeout_secs: 1,
            stream_first_chunk_timeout_secs: 1,
            fallback_timeout_secs: 3,
        },
        r#type: "openai".to_string(),
        auth: AuthConfig {
            enabled: true,
            keys: vec![ApiKey {
                key: "sk-proxy-default".to_string(),
                name: "default".to_string(),
                rate_limit: 100,
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
    TcpListener::bind("127.0.0.1:0").await.expect("bind listener")
}
