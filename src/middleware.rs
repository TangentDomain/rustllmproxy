use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use crate::config::Config;
use dashmap::{DashMap, Entry};
use std::sync::Arc;
use std::time::Instant;

/// 每个key的限流状态
struct RateLimitEntry {
    count: u32,
    window_start: Instant,
}

impl Default for RateLimitEntry {
    fn default() -> Self {
        Self { count: 0, window_start: Instant::now() }
    }
}

#[derive(Default)]
pub struct RateLimiter {
    entries: DashMap<String, RateLimitEntry>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 返回 true 表示允许，false 表示被限流
    pub fn check(&self, key: &str, limit: u32) -> bool {
        let now = Instant::now();
        match self.entries.entry(key.to_string()) {
            Entry::Occupied(mut e) => {
                let entry = e.get_mut();
                if now.duration_since(entry.window_start).as_secs() >= 60 {
                    entry.count = 0;
                    entry.window_start = now;
                }
                entry.count += 1;
                entry.count <= limit
            }
            Entry::Vacant(e) => {
                e.insert(RateLimitEntry { count: 1, window_start: now });
                true
            }
        }
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

    // 提取 API key
    let key = extract_api_key(&request);
    let key = match key {
        Some(k) => k,
        None => {
            tracing::warn!("请求缺少认证信息");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // 校验 key
    let api_key = state.config.find_api_key(&key);
    let api_key = match api_key {
        Some(k) => k,
        None => {
            tracing::warn!("无效的API key");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // 限流检查
    if !state.limiter.check(&key, api_key.rate_limit) {
        tracing::warn!("key [{}] 触发限流", api_key.name);
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    // 存储认证信息供后续 handler 使用
    request.extensions_mut().insert(AuthInfo {
        key_name: api_key.name.clone(),
    });

    Ok(next.run(request).await)
}

fn extract_api_key(request: &Request) -> Option<String> {
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
