use crate::config::Config;
use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

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
        // DashMap::iter() 的遍历顺序不保证稳定；这里仅用于渐进式清理。
        for entry in self.entries.iter() {
            scanned += 1;
            if scanned > target {
                break;
            }
            // 仅在条目确实过期时尝试 remove，避免额外写锁竞争。
            if now.duration_since(entry.value().window_start).as_secs() >= STALE_SECS {
                let k = entry.key().clone();
                self.entries.remove(&k);
            }
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

    // 提取 API key
    let key = extract_api_key(&request);
    let key = match key {
        Some(k) => k,
        None => {
            tracing::info!("请求缺少认证信息");
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
        tracing::info!("key [{}] 触发限流", api_key.name);
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
