use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::Router;
use tracing::info;

use llmproxy::config::Config;
use llmproxy::proxy::{self, Proxy};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/unified.toml".to_string());
    let config = Config::load(&config_path).expect("Failed to load config");
    info!("Loaded config: type=unified, port={}", config.server.port);

    let routes = Router::new()
        .route("/v1/models", get(models_handler));

    proxy::run_server(config, routes).await;
}

async fn models_handler(State(proxy): State<Arc<Proxy>>) -> axum::Json<serde_json::Value> {
    let models: Vec<serde_json::Value> = proxy.config().all_models().iter().map(|m| {
        serde_json::json!({
            "id": m,
            "object": "model",
            "owned_by": "llmproxy",
        })
    }).collect();
    axum::Json(serde_json::json!({
        "object": "list",
        "data": models,
    }))
}
