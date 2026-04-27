use std::sync::Arc;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Router, Json};
use serde_json::Value;
use tracing::info;

use llmproxy::config::Config;
use llmproxy::proxy::{self, Proxy};
use axum::http::{Request, Response};
use axum::body::Body;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/unified.toml".to_string());
    let config = Config::load(&config_path).expect("Failed to load config");

    let log_dir = config.server.log_dir.clone();
    std::fs::create_dir_all(&log_dir).ok();
    let log_name = format!("proxy-{}.log", config.server.port);
    let file_appender = tracing_appender::rolling::daily(&log_dir, log_name);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    std::mem::forget(guard);

    use tracing_subscriber::prelude::*;

    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(tracing_subscriber::fmt::Layer::new().with_writer(std::io::stdout).with_ansi(true).with_target(false))
        .with(tracing_subscriber::fmt::Layer::new().with_writer(non_blocking).with_ansi(false).with_target(false))
        .init();

    info!("Starting unified-proxy on :{}, logging to {}/", config.server.port, log_dir);
    info!("Loaded config: {}", config_path);

    let routes = Router::new()
        .route("/openai/v1/models", get(openai_models_handler))
        .route("/anthropic/v1/models", get(anthropic_models_handler))
        .route("/openai/v1/{*path}", post(openai_handler))
        .route("/anthropic/v1/{*path}", post(anthropic_handler));

    proxy::run_server(config, routes).await;
}

async fn openai_models_handler(State(proxy): State<Arc<Proxy>>) -> Json<Value> {
    let models: Vec<Value> = proxy.config().all_models().iter().map(|m| {
        serde_json::json!({
            "id": m,
            "object": "model",
            "owned_by": "llmproxy",
        })
    }).collect();
    Json(serde_json::json!({
        "object": "list",
        "data": models,
    }))
}

async fn anthropic_models_handler(State(proxy): State<Arc<Proxy>>) -> Json<Value> {
    let models: Vec<Value> = proxy.config().all_models().iter().map(|m| {
        serde_json::json!({
            "id": m,
            "name": m,
            "display_name": m,
        })
    }).collect();
    Json(serde_json::json!({
        "models": models,
    }))
}

async fn openai_handler(
    State(proxy): State<Arc<Proxy>>,
    mut req: Request<Body>,
) -> Response<Body> {
    // 重写路径：移除 /openai 前缀
    let uri = req.uri().to_string();
    let new_path = uri.replacen("/openai", "", 1);
    *req.uri_mut() = new_path.parse().unwrap();

    // 标记协议类型
    req.extensions_mut().insert("openai".to_string());

    proxy.handle(req).await
}

async fn anthropic_handler(
    State(proxy): State<Arc<Proxy>>,
    mut req: Request<Body>,
) -> Response<Body> {
    // 重写路径：移除 /anthropic 前缀
    let uri = req.uri().to_string();
    let new_path = uri.replacen("/anthropic", "", 1);
    *req.uri_mut() = new_path.parse().unwrap();

    // 标记协议类型
    req.extensions_mut().insert("anthropic".to_string());

    proxy.handle(req).await
}
