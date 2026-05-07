//! 协议路由构建器。
//!
//! 从 `src/bin/unified.rs` 和 `tests/common/mod.rs` 中提取的公共路由定义。
//! 保持路由路径和 protocol extension 值完全一致：
//! - `/openai/v1/models`, `/anthropic/v1/models`
//! - `/openai/v1/{*path}`, `/anthropic/v1/{*path}`
//! - protocol strings: `"openai"`, `"anthropic"`

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::response::{Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde_json::Value;

use crate::proxy::Proxy;

/// 构建带 OpenAI + Anthropic 前缀的协议路由。
///
/// 路径与 `src/bin/unified.rs` 完全一致：
/// ```text
/// /openai/v1/models       → GET  openai_models_handler
/// /anthropic/v1/models    → GET  anthropic_models_handler
/// /openai/v1/{*path}      → POST openai_handler
/// /anthropic/v1/{*path}   → POST anthropic_handler
/// ```
pub fn protocol_routes() -> Router<Arc<Proxy>> {
    Router::new()
        .route("/openai/v1/models", get(openai_models_handler))
        .route("/anthropic/v1/models", get(anthropic_models_handler))
        .route("/openai/v1/{*path}", post(openai_handler))
        .route("/anthropic/v1/{*path}", post(anthropic_handler))
}

/// 从请求 URI 中剥离 `prefix` 并在 extensions 中写入 `protocol` 字符串。
///
/// 对应原 `src/bin/unified.rs` 中的内联重写逻辑：
/// - OpenAI:  移除 `/openai`，写入 `"openai"`
/// - Anthropic: 移除 `/anthropic`，写入 `"anthropic"`
pub fn rewrite_prefixed_request(req: &mut Request<Body>, prefix: &str, protocol: &str) {
    let uri = req.uri().to_string();
    let new_path = uri.replacen(prefix, "", 1);
    *req.uri_mut() = new_path
        .parse()
        .expect("valid uri after prefix strip");
    req.extensions_mut().insert(protocol.to_string());
}

/// OpenAI 格式模型列表（`id` / `object` / `owned_by`）。
pub async fn openai_models_handler(State(proxy): State<Arc<Proxy>>) -> Json<Value> {
    let models: Vec<Value> = proxy
        .config()
        .all_models()
        .iter()
        .map(|m| {
            serde_json::json!({
                "id": m,
                "object": "model",
                "owned_by": "llmproxy",
            })
        })
        .collect();
    Json(serde_json::json!({
        "object": "list",
        "data": models,
    }))
}

/// Anthropic 格式模型列表（`id` / `name` / `display_name`）。
pub async fn anthropic_models_handler(State(proxy): State<Arc<Proxy>>) -> Json<Value> {
    let models: Vec<Value> = proxy
        .config()
        .all_models()
        .iter()
        .map(|m| {
            serde_json::json!({
                "id": m,
                "name": m,
                "display_name": m,
            })
        })
        .collect();
    Json(serde_json::json!({
        "models": models,
    }))
}

pub async fn openai_handler(State(proxy): State<Arc<Proxy>>, mut req: Request<Body>) -> Response<Body> {
    rewrite_prefixed_request(&mut req, "/openai", "openai");
    proxy.handle(req).await
}

pub async fn anthropic_handler(
    State(proxy): State<Arc<Proxy>>,
    mut req: Request<Body>,
) -> Response<Body> {
    rewrite_prefixed_request(&mut req, "/anthropic", "anthropic");
    proxy.handle(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_openai_prefix() {
        let mut req = Request::builder()
            .uri("/openai/v1/chat/completions")
            .body(Body::empty())
            .unwrap();

        rewrite_prefixed_request(&mut req, "/openai", "openai");

        assert_eq!(req.uri().to_string(), "/v1/chat/completions");
        let protocol = req.extensions().get::<String>().unwrap();
        assert_eq!(protocol, "openai");
    }

    #[test]
    fn test_rewrite_anthropic_prefix() {
        let mut req = Request::builder()
            .uri("/anthropic/v1/messages")
            .body(Body::empty())
            .unwrap();

        rewrite_prefixed_request(&mut req, "/anthropic", "anthropic");

        assert_eq!(req.uri().to_string(), "/v1/messages");
        let protocol = req.extensions().get::<String>().unwrap();
        assert_eq!(protocol, "anthropic");
    }

    #[test]
    fn test_prefix_replaced_once() {
        let mut req = Request::builder()
            .uri("/openai/openai/v1/chat")
            .body(Body::empty())
            .unwrap();

        rewrite_prefixed_request(&mut req, "/openai", "openai");

        // 只替换第一个 /openai
        assert_eq!(req.uri().to_string(), "/openai/v1/chat");
    }

    #[test]
    fn test_protocol_routes_builds_without_panic() {
        let _router: Router<Arc<Proxy>> = protocol_routes();
    }
}
