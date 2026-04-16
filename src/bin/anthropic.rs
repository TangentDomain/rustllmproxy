use axum::Router;
use tracing::info;

use llmproxy::config::Config;
use llmproxy::proxy;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/anthropic.toml".to_string());
    let config = Config::load(&config_path).expect("Failed to load config");
    info!("Loaded config: type=anthropic, port={}", config.server.port);

    // Anthropic has no /models endpoint, just the proxy fallback
    let routes = Router::new();

    proxy::run_server(config, routes).await;
}
