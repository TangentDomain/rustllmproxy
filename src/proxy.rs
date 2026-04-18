use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, Response, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use reqwest::Client;
use tokio::signal;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn, error};

use crate::balancer::WeightedRoundRobin;
use crate::config::{Config, Backend};
use crate::middleware::RateLimiter;

/// 转发错误分类，决定后续行为
enum ForwardError {
    /// 4xx 客户端错误 → 直接返回，不fallback
    ClientError(StatusCode, String),
    /// 429 Rate Limit → 立即切换下一个key
    RateLimited,
    /// 5xx/超时/连接失败 → 切换下一个后端
    ServerErr(String),
}

#[derive(Clone)]
pub struct Proxy {
    config: Arc<Config>,
    balancer: Arc<WeightedRoundRobin>,
    limiter: Arc<RateLimiter>,
    client: Client,
}

impl Proxy {
    pub fn new(config: Arc<Config>, balancer: Arc<WeightedRoundRobin>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(50)
            .pool_idle_timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(3))
            .build()
            .expect("failed to build reqwest client");
        Self { config, balancer, limiter: Arc::new(RateLimiter::new()), client }
    }

    pub fn balancer(&self) -> &Arc<WeightedRoundRobin> {
        &self.balancer
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// 检查后端是否健康（O(1) 查找）
    fn is_healthy(&self, b: &Backend) -> bool {
        self.balancer.is_healthy_by_name(&b.name)
    }

    /// Main proxy handler: follow fallback chain, route each model to matching backends.
    ///
    /// 效率策略:
    /// - 每个后端只试1次（多后端轮询，不重复重试同一个）
    /// - 429 Rate Limit → 立即切换下一个key
    /// - 4xx 客户端错误 → 直接返回，不fallback
    /// - 连接超时(5s)快速失败 → 切换下一个后端
    /// - 不健康的后端直接跳过
    pub async fn handle(&self, req: Request<Body>) -> Response<Body> {
        let (parts, body) = req.into_parts();
        let path = parts.uri.path().to_string();

        // 从扩展中获取协议类型（由 unified.rs 设置）
        let protocol = parts.extensions.get::<String>().cloned();

        let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read request body: {e}");
                return (StatusCode::BAD_REQUEST, "Failed to read body").into_response();
            }
        };

        // 延迟解析：只在需要修改 model 时才反序列化
        let model = extract_model_from_json(&bytes);
        info!("Request model={model}, path={path}, protocol={:?}", protocol);

        let chain = self.config.get_fallback_chain(&model);

        // 按fallback链顺序尝试每个模型
        let original_model = &model;
        for try_model in &chain {
            let all_backends = self.config.find_backends_for_model(try_model, protocol.as_deref());
            if all_backends.is_empty() {
                warn!("No backend supports model={try_model}, skipping");
                continue;
            }

            // 过滤出健康后端（不健康的直接跳过）
            let healthy: Vec<_> = all_backends.iter()
                .filter(|b| self.is_healthy(b))
                .collect();

            if healthy.is_empty() {
                warn!("No healthy backend for model={try_model}, skip to next model");
                continue;
            }

            info!("Trying model={}, healthy backends={}/{}", try_model, healthy.len(), all_backends.len());

            // 每个健康后端只试1次，轮询而非重试同一个
            for selected in &healthy {
                let resolved = selected.resolve_model(try_model);
                // model 未变时用 Bytes::clone（Arc 引用计数+1，零数据拷贝）
                // model 变更时用字节级 patch 替代完整反序列化
                let body_bytes: Vec<u8> = if resolved == *try_model && *original_model == *try_model {
                    bytes.to_vec()
                } else {
                    patch_json_model(&bytes, original_model, &resolved)
                };

                match self.do_forward(&path, &parts.headers, body_bytes, selected).await {
                    Ok(resp) => {
                        info!("Success: {} -> {} via {}", try_model, resolved, selected.name);
                        return resp;
                    }
                    Err(ForwardError::ClientError(status, body)) => {
                        warn!("Client error from {}: {} - returning directly", selected.name, status);
                        return (status, body).into_response();
                    }
                    Err(ForwardError::RateLimited) => {
                        warn!("{} rate limited (429), switching to next backend", selected.name);
                        continue;
                    }
                    Err(ForwardError::ServerErr(e)) => {
                        warn!("{} server error: {}, trying next", selected.name, e);
                        continue;
                    }
                }
            }
            warn!("All backends exhausted for model={try_model}, falling back");
        }

        error!("All models exhausted for original model={model}");
        (StatusCode::SERVICE_UNAVAILABLE, "All backends exhausted").into_response()
    }

    /// 转发请求到指定后端，返回分类后的错误
    async fn do_forward(
        &self,
        path: &str,
        orig_headers: &HeaderMap,
        body: Vec<u8>,
        backend: &Backend,
    ) -> Result<Response<Body>, ForwardError> {
        // Rewrite /v1/ -> /v4/ 仅对 OpenAI 协议 + bigmodel 域名（coding API）
        // Anthropic 格式保持原始路径
        let forward_path = if backend.protocol == "openai" && backend.url.contains("bigmodel") {
            path.replacen("/v1/", "/v4/", 1)
        } else {
            path.to_string()
        };
        let url = format!("{}{}", backend.url.trim_end_matches('/'), forward_path);
        info!("Forwarding to {} (backend={}, protocol={})", url, backend.name, backend.protocol);

        // 使用总超时（后端配置的timeout_secs）
        let total_timeout = Duration::from_secs(backend.timeout_secs);

        let mut req_builder = self.client
            .post(&url)
            .timeout(total_timeout)
            .body(body);

        // 使用预计算的 auth_header（启动时生成，避免每次 format! 分配）
        match backend.protocol.as_str() {
            "anthropic" => {
                req_builder = req_builder
                    .header("x-api-key", &backend.api_key)
                    .header("anthropic-version", "2023-06-01");
            }
            _ => {
                req_builder = req_builder
                    .header(header::AUTHORIZATION, &backend.auth_header);
            }
        }

        if let Some(ct) = orig_headers.get(header::CONTENT_TYPE) {
            req_builder = req_builder.header(header::CONTENT_TYPE, ct);
        }

        let resp = match req_builder.send().await {
            Ok(r) => r,
            Err(e) => {
                // 连接错误/超时 → ServerErr，快速切换下一个后端
                if e.is_timeout() || e.is_connect() {
                    warn!("{} connect/timeout error: {}", backend.name, e);
                } else {
                    warn!("{} request failed: {}", backend.name, e);
                }
                return Err(ForwardError::ServerErr(format!("{}: {}", backend.name, e)));
            }
        };

        let status = resp.status();
        let code = status.as_u16();

        if status.is_success() {
            Ok(self.stream_response(resp).await)
        } else if code == 429 {
            // Rate Limit → 立即换key，不重试当前
            Err(ForwardError::RateLimited)
        } else if code >= 400 && code < 500 {
            // 4xx 客户端错误 → 直接返回给调用方
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let status_axum = StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST);
            Err(ForwardError::ClientError(status_axum, String::from_utf8_lossy(&body_bytes).into_owned()))
        } else {
            // 5xx 服务端错误 → 切换下一个后端
            Err(ForwardError::ServerErr(format!("{} returned {}", backend.name, status)))
        }
    }

    async fn stream_response(&self, resp: reqwest::Response) -> Response<Body> {
        let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
        let mut builder = Response::builder()
            .status(status)
            .header("x-accel-buffering", "no")
            .header("cache-control", "no-cache");
        for (key, value) in resp.headers() {
            if matches!(key.as_str(), "content-type" | "content-encoding") {
                builder = builder.header(key, value);
            }
        }
        let body = Body::from_stream(resp.bytes_stream());
        builder.body(body).unwrap()
    }

    pub fn limiter(&self) -> &Arc<RateLimiter> {
        &self.limiter
    }
}

/// Auth middleware adapted for Proxy state.
pub async fn auth_layer(
    State(proxy): State<Arc<Proxy>>,
    mut request: Request<Body>,
    next: axum::middleware::Next,
) -> Result<Response<Body>, StatusCode> {
    if !proxy.config().auth.enabled {
        return Ok(next.run(request).await);
    }

    let key = extract_bearer_key(&request);
    let key = match key {
        Some(k) => k,
        None => {
            let method = request.method();
            let uri = request.uri();
            let path = uri.path();
            let query = uri.query().unwrap_or("");
            let user_agent = request.headers()
                .get("user-agent")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("unknown");
            let real_ip = request.headers()
                .get("x-real-ip")
                .or_else(|| request.headers().get("x-forwarded-for"))
                .and_then(|h| h.to_str().ok())
                .unwrap_or("unknown");

            warn!("Request missing auth: method={}, path={}, query={}, user_agent={}, real_ip={}",
                method, path, query, user_agent, real_ip);
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let api_key = proxy.config().find_api_key(&key);
    let api_key = match api_key {
        Some(k) => k,
        None => {
            let method = request.method();
            let path = request.uri().path();
            let key_preview = if key.len() > 8 { &key[..8] } else { &key };
            warn!("Invalid API key: method={}, path={}, key_prefix={}", method, path, key_preview);
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    if !proxy.limiter().check(&key, api_key.rate_limit) {
        warn!("Rate limited: {}", api_key.name);
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    request.extensions_mut().insert(crate::middleware::AuthInfo {
        key_name: api_key.name.clone(),
    });
    Ok(next.run(request).await)
}

/// 从 JSON bytes 中快速提取 "model" 字段值（手动扫描，避免完整反序列化）
fn extract_model_from_json(bytes: &[u8]) -> String {
    const PATTERN: &[u8] = b"\"model\"";
    if let Some(pos) = bytes.windows(PATTERN.len()).position(|w| w == PATTERN) {
        let after_key = &bytes[pos + PATTERN.len()..];
        // 跳过空白和冒号
        let after_colon = skip_whitespace(after_key);
        if !after_colon.is_empty() && after_colon[0] == b':' {
            let after_colon = skip_whitespace(&after_colon[1..]);
            if !after_colon.is_empty() && after_colon[0] == b'"' {
                if let Some(end) = after_colon[1..].iter().position(|&c| c == b'"') {
                    return String::from_utf8_lossy(&after_colon[1..1 + end]).into_owned();
                }
            }
        }
    }
    String::new()
}

/// 跳过空白字符（空格、tab、换行、回车）
fn skip_whitespace(s: &[u8]) -> &[u8] {
    s.iter().position(|&c| c != b' ' && c != b'\t' && c != b'\n' && c != b'\r')
        .map_or(&[], |i| &s[i..])
}

/// 字节级替换 JSON 中的 model 字段值，避免完整反序列化+序列化
fn patch_json_model(bytes: &[u8], old_model: &str, new_model: &str) -> Vec<u8> {
    // 搜索 "model":"old_value" 并替换为 "model":"new_value"
    let pattern = format!("\"model\":\"{old_model}\"");
    let replacement = format!("\"model\":\"{new_model}\"");
    let p = pattern.as_bytes();
    let r = replacement.as_bytes();
    // 手动字节替换
    let mut result = Vec::with_capacity(bytes.len() + r.len().saturating_sub(p.len()));
    let mut i = 0;
    while i <= bytes.len().saturating_sub(p.len()) {
        if bytes[i..].starts_with(p) {
            result.extend_from_slice(r);
            i += p.len();
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }
    result.extend_from_slice(&bytes[i..]);
    // 如果没替换到或 model 相同，直接返回原始 bytes 的拷贝
    if result == bytes || old_model == new_model {
        bytes.to_vec()
    } else {
        result
    }
}

fn extract_bearer_key(req: &Request<Body>) -> Option<String> {
    if let Some(auth) = req.headers().get("authorization") {
        if let Ok(v) = auth.to_str() {
            if let Some(key) = v.strip_prefix("Bearer ") {
                return Some(key.to_string());
            }
        }
    }
    if let Some(query) = req.uri().query() {
        for pair in query.split('&') {
            if let Some(val) = pair.strip_prefix("api_key=") {
                return Some(val.to_string());
            }
        }
    }
    None
}

// --- Shared server runner ---

pub async fn run_server(config: Config, extra_routes: Router<Arc<Proxy>>) {
    let config = Arc::new(config);
    let balancer = Arc::new(WeightedRoundRobin::new(
        config.backends.clone(),
        config.retry,
        Duration::from_millis(config.retry_delay_ms),
    ));
    let proxy = Arc::new(Proxy::new(config.clone(), balancer.clone()));
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.port));

    // Start health check
    balancer.start_health_check(Duration::from_secs(30));

    // 公共路由（不需要认证）
    let public_routes = Router::new()
        .route("/health", get(|| async { axum::Json(serde_json::json!({"status": "ok"})) }));

    // 受保护路由（需要认证）
    let protected_routes = Router::new()
        .route("/backends", get(backends_handler))
        .merge(extra_routes)
        .fallback(post(|state: State<Arc<Proxy>>, req: Request<Body>| async move {
            state.0.handle(req).await
        }))
        .layer(axum::middleware::from_fn_with_state(proxy.clone(), auth_layer));

    let app = public_routes
        .merge(protected_routes)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(proxy);

    info!("Listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn backends_handler(State(proxy): State<Arc<Proxy>>) -> axum::Json<serde_json::Value> {
    let backends: Vec<serde_json::Value> = proxy.balancer().all_backends().iter().map(|b| {
        serde_json::json!({
            "name": b.backend.name,
            "url": b.backend.url,
            "healthy": b.healthy.load(std::sync::atomic::Ordering::Relaxed),
            "fail_count": b.fail_count.load(std::sync::atomic::Ordering::Relaxed),
        })
    }).collect();
    axum::Json(serde_json::json!({"backends": backends}))
}

async fn shutdown_signal() {
    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    info!("Shutting down...");
}
