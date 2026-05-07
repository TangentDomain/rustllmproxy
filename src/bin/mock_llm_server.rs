use std::sync::Arc;

use axum::{routing::post, Router};
use llmproxy::binlib::mock_llm::{chat_handler, MockConfig};

#[tokio::main]
async fn main() {
    let config = MockConfig::from_env();

    let port = mock_port_from_env_value(std::env::var("MOCK_PORT").ok().as_deref());

    let addr = mock_addr(port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("bind mock listener");
    println!("Mock LLM server listening on http://{addr}");
    println!("{}", config_summary(&config));
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_handler))
        .route("/v4/chat/completions", post(chat_handler))
        .with_state(Arc::new(config));

    axum::serve(listener, app)
        .await
        .expect("mock server exited with error")
}

fn mock_port_from_env_value(value: Option<&str>) -> u16 {
    value.and_then(|s| s.parse().ok()).unwrap_or(8766)
}

fn mock_addr(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn config_summary(config: &MockConfig) -> String {
    format!(
        "Config: delay={}ms, error_rate={:.2}, rate_limit_rate={:.2}",
        config.delay_ms, config.error_rate, config.rate_limit_rate
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_port_defaults_to_8766() {
        assert_eq!(mock_port_from_env_value(None), 8766);
        assert_eq!(mock_port_from_env_value(Some("not-a-port")), 8766);
    }

    #[test]
    fn mock_port_reads_valid_value() {
        assert_eq!(mock_port_from_env_value(Some("9000")), 9000);
    }

    #[test]
    fn mock_addr_binds_to_loopback_only() {
        assert_eq!(mock_addr(8766), "127.0.0.1:8766");
    }

    #[test]
    fn config_summary_matches_runtime_output() {
        let config = MockConfig {
            delay_ms: 50,
            error_rate: 0.25,
            rate_limit_rate: 0.5,
        };
        assert_eq!(
            config_summary(&config),
            "Config: delay=50ms, error_rate=0.25, rate_limit_rate=0.50"
        );
    }
}
