use crate::config::Config;
use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 从请求中提取客户端凭证（当前仅支持：Authorization: Bearer <key> 或 query api_key=<key>）。
///
/// 该函数是 auth/rate-limit 边界的显式耦合点：
/// - 只依赖 HeaderMap/Uri（通过 Request 访问），不携带 Proxy/AppState 等业务状态；
/// - 行为必须与历史一致（大小写/前缀/空白等均保持原样）；
/// - 方便在单元测试中直接验证解析行为。
pub fn extract_client_credential_from_request<B>(
    request: &axum::http::Request<B>,
) -> Option<String> {
    // 优先从 Authorization header 提取
    if let Some(auth) = request.headers().get("authorization") {
        if let Ok(v) = auth.to_str() {
            if let Some(key) = v.strip_prefix("Bearer ") {
                return Some(key.to_string());
            }
        }
    }
    // 从 query 参数提取
    if let Some(query) = request.uri().query() {
        for pair in query.split('&') {
            if let Some(val) = pair.strip_prefix("api_key=") {
                return Some(val.to_string());
            }
        }
    }
    None
}
/// 每个key的限流状态
struct RateLimitEntry {
    count: u32,
    window_start: Instant,
}

impl Default for RateLimitEntry {
    fn default() -> Self {
        Self {
            count: 0,
            window_start: Instant::now(),
        }
    }
}

#[derive(Default)]
pub struct RateLimiter {
    entries: DashMap<String, RateLimitEntry>,
    /// 每次清理最多扫描的 entry 数，避免 retain() 带来的周期性尖峰。
    cleanup_budget: usize,
    /// 按需惰性清理计数器（通过预算分摊）
    cleanup_cursor: AtomicU64,
}
impl RateLimiter {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            cleanup_budget: 64,
            cleanup_cursor: AtomicU64::new(0),
        }
    }

    /// 返回 true 表示允许，false 表示被限流
    pub fn check(&self, key: &str, limit: u32) -> bool {
        let now = Instant::now();

        // 惰性清理：将“全量 retain()”改为“按预算分摊扫描”。
        // 语义保持：过期条目最终会被删除；只是从“周期性尖峰”变为“平滑的渐进成本”。
        self.cleanup_some(now);

        // 先无分配查找已存在的 key
        if let Some(mut e) = self.entries.get_mut(key) {
            let entry = e.value_mut();
            if now.duration_since(entry.window_start).as_secs() >= 60 {
                entry.count = 0;
                entry.window_start = now;
            }
            entry.count += 1;
            return entry.count <= limit;
        }
        // 不存在才分配并插入
        self.entries.insert(
            key.to_string(),
            RateLimitEntry {
                count: 1,
                window_start: now,
            },
        );
        true
    }

    #[inline]
    fn cleanup_some(&self, now: Instant) {
        // 基于 window_start 过期逻辑：120s 内未被访问则可清理。
        const STALE_SECS: u64 = 120;
        let target = self.cleanup_budget.max(1);
        let mut scanned = 0usize;
        let mut expired_keys = Vec::new();
        // 先只收集过期 key，避免在 DashMap 迭代期间直接 remove() 放大锁竞争。
        for entry in self.entries.iter() {
            scanned += 1;
            if scanned > target {
                break;
            }
            if now.duration_since(entry.value().window_start).as_secs() >= STALE_SECS {
                expired_keys.push(entry.key().clone());
            }
        }
        for key in expired_keys {
            self.entries.remove(&key);
        }
        self.cleanup_cursor
            .fetch_add(scanned as u64, Ordering::Relaxed);
    }
}

/// Auth + 限流中间件
pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if !state.config.auth.enabled {
        return Ok(next.run(request).await);
    }

    let auth = authorize_request(&state, &request)?;
    request.extensions_mut().insert(auth);

    Ok(next.run(request).await)
}

fn authorize_request<B>(state: &AppState, request: &axum::http::Request<B>) -> Result<AuthInfo, StatusCode> {
    let key = match extract_client_credential_from_request(request) {
        Some(k) => k,
        None => {
            tracing::info!("请求缺少认证信息");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let api_key = match state.config.find_api_key(&key) {
        Some(k) => k,
        None => {
            tracing::warn!("无效的API key");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    if !state.limiter.check(&key, api_key.rate_limit) {
        tracing::info!("key [{}] 触发限流", api_key.name);
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    Ok(AuthInfo {
        key_name: api_key.name.clone(),
    })
}

// 兼容旧的私有函数名，避免未来局部回滚/对比时误改调用点。
#[cfg(test)]
fn extract_api_key<B>(request: &Request<B>) -> Option<String> {
    extract_client_credential_from_request(request)
}

#[cfg(not(test))]
#[allow(dead_code)]
fn extract_api_key<B>(request: &Request<B>) -> Option<String> {
    extract_client_credential_from_request(request)
}

#[cfg(test)]
mod tests {
    use super::{authorize_request, extract_api_key, AppState, RateLimitEntry, RateLimiter};
    use axum::http::Request;
    use std::collections::HashMap;
    use crate::config::{ApiKey, AuthConfig, Config, ServerConfig};
    use std::time::{Duration, Instant};

    #[test]
    fn extract_api_key_preserves_existing_bearer_and_query_behavior() {
        let header_request = Request::builder()
            .uri("/openai/v1/chat/completions?api_key=query-key")
            .header("authorization", "Bearer header-key")
            .body(())
            .expect("header request");
        assert_eq!(
            extract_api_key(&header_request).as_deref(),
            Some("header-key")
        );

        let query_request = Request::builder()
            .uri("/openai/v1/chat/completions?foo=1&api_key=query-key&bar=2")
            .body(())
            .expect("query request");
        assert_eq!(
            extract_api_key(&query_request).as_deref(),
            Some("query-key")
        );

        let lowercase_bearer_request = Request::builder()
            .uri("/openai/v1/chat/completions?api_key=query-key")
            .header("authorization", "bearer lower-key")
            .body(())
            .expect("lowercase bearer request");
        assert_eq!(
            extract_api_key(&lowercase_bearer_request).as_deref(),
            Some("query-key")
        );

        let spaced_bearer_request = Request::builder()
            .uri("/openai/v1/chat/completions")
            .header("authorization", "Bearer  spaced-key")
            .body(())
            .expect("spaced bearer request");
        assert_eq!(
            extract_api_key(&spaced_bearer_request).as_deref(),
            Some(" spaced-key")
        );

        let empty_query_key_request = Request::builder()
            .uri("/openai/v1/chat/completions?x=1&api_key=&y=2")
            .body(())
            .expect("empty query key request");
        assert_eq!(
            extract_api_key(&empty_query_key_request).as_deref(),
            Some("")
        );
    }
    #[test]
    fn cleanup_some_removes_expired_entries_after_iteration() {
        let limiter = RateLimiter::new();
        let stale_start = Instant::now() - Duration::from_secs(121);
        for i in 0..8 {
            limiter.entries.insert(
                format!("expired-{i}"),
                RateLimitEntry {
                    count: 1,
                    window_start: stale_start,
                },
            );
        }

        assert!(limiter.check("fresh-key", 10));

        for i in 0..8 {
            assert!(!limiter.entries.contains_key(&format!("expired-{i}")));
        }
        assert!(limiter.entries.contains_key("fresh-key"));
    }

    #[test]
    fn cleanup_some_keeps_rate_limit_check_fast_and_correct() {
        let limiter = RateLimiter::new();
        for i in 0..128 {
            assert!(limiter.check(&format!("key-{i}"), 2));
        }
        for i in 0..128 {
            assert!(limiter.check(&format!("key-{i}"), 2));
        }
    }

    #[test]
    fn extract_api_key_returns_none_without_supported_credentials() {
        let request = Request::builder()
            .uri("/openai/v1/chat/completions?foo=1")
            .header("authorization", "Token abc")
            .body(())
            .expect("request");
        assert_eq!(extract_api_key(&request), None);
    }

    #[test]
    fn rate_limiter_rejects_after_limit_and_keeps_independent_keys() {
        let limiter = RateLimiter::new();
        assert!(limiter.check("key-a", 2));
        assert!(limiter.check("key-a", 2));
        assert!(!limiter.check("key-a", 2));
        assert!(limiter.check("key-b", 1));
        assert!(!limiter.check("key-b", 1));
    }

    #[test]
    fn rate_limiter_resets_count_after_window_elapsed() {
        let limiter = RateLimiter::new();
        limiter.entries.insert(
            "stale-window-key".to_string(),
            RateLimitEntry {
                count: 99,
                window_start: Instant::now() - Duration::from_secs(60),
            },
        );

        assert!(limiter.check("stale-window-key", 1));
        assert!(!limiter.check("stale-window-key", 1));
    }

    fn test_state(rate_limit: u32) -> AppState {
        AppState {
            config: Config {
                server: ServerConfig {
                    port: 0,
                    timeout_secs: 1,
                    log_dir: "target/test-logs".to_string(),
                    stream_idle_timeout_secs: 1,
                    stream_first_chunk_timeout_secs: 1,
                    fallback_timeout_secs: 1,
                    recovery_cooldown_secs: 1,
                    body_read_timeout_secs: 30,
                    watchdog_enabled: false,
                    watchdog_check_interval_ms: 2_000,
                    watchdog_runtime_tick_stall_ms: 5_000,
                    watchdog_restart_cooldown_secs: 120,
                },
                r#type: "test".to_string(),
                auth: AuthConfig {
                    enabled: true,
                    keys: vec![ApiKey {
                        key: "valid-key".to_string(),
                        name: "valid-name".to_string(),
                        rate_limit,
                    }],
                    key_index: HashMap::from([("valid-key".to_string(), 0)]),
                },
                backends: Vec::new(),
                retry: 0,
                retry_delay_ms: 0,
                fallback: HashMap::new(),
                model_mapping: HashMap::new(),
            },
            limiter: RateLimiter::new(),
        }
    }

    #[test]
    fn authorize_request_rejects_missing_and_invalid_credentials() {
        let state = test_state(10);
        let missing = Request::builder()
            .uri("/openai/v1/chat/completions")
            .body(())
            .expect("missing credential request");
        let invalid = Request::builder()
            .uri("/openai/v1/chat/completions")
            .header("authorization", "Bearer invalid-key")
            .body(())
            .expect("invalid credential request");

        assert_eq!(
            authorize_request(&state, &missing).err(),
            Some(axum::http::StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            authorize_request(&state, &invalid).err(),
            Some(axum::http::StatusCode::UNAUTHORIZED)
        );
    }

    #[test]
    fn authorize_request_allows_valid_key_and_enforces_rate_limit() {
        let state = test_state(1);
        let request = Request::builder()
            .uri("/openai/v1/chat/completions")
            .header("authorization", "Bearer valid-key")
            .body(())
            .expect("valid credential request");

        let auth = authorize_request(&state, &request).expect("first request allowed");
        assert_eq!(auth.key_name, "valid-name");
        assert_eq!(
            authorize_request(&state, &request).err(),
            Some(axum::http::StatusCode::TOO_MANY_REQUESTS)
        );
    }
}
/// 认证信息，注入到 request extensions
#[derive(Clone)]
pub struct AuthInfo {
    pub key_name: String,
}

/// 共享的应用状态
pub struct AppState {
    pub config: Config,
    pub limiter: RateLimiter,
}
