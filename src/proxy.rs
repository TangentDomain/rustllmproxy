use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
        let t_start = Instant::now();
        let (parts, body) = req.into_parts();
        let path = parts.uri.path().to_string();

        let protocol = parts.extensions.get::<String>().cloned();

        let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read request body: {e}");
                return (StatusCode::BAD_REQUEST, "Failed to read body").into_response();
            }
        };

        let model = extract_model_from_json(&bytes);
        let body_size = bytes.len();
        info!("Request model={model}, path={path}, protocol={:?}, body={body_size}B", protocol);

        let chain = self.config.get_fallback_chain(&model);
        let original_model = &model;

        for try_model in &chain {
            let all_backends = self.config.find_backends_for_model(try_model, protocol.as_deref());
            if all_backends.is_empty() {
                warn!("No backend supports model={try_model}, skipping");
                continue;
            }

            let healthy: Vec<_> = all_backends.iter()
                .filter(|b| self.is_healthy(b))
                .collect();

            if healthy.is_empty() {
                warn!("No healthy backend for model={try_model}, skip to next model");
                continue;
            }

            info!("Trying model={}, healthy backends={}/{}", try_model, healthy.len(), all_backends.len());

            for selected in &healthy {
                let resolved = selected.resolve_model(try_model);
                let body_bytes: Vec<u8> = if resolved == *try_model && *original_model == *try_model {
                    bytes.to_vec()
                } else {
                    patch_json_model(&bytes, original_model, &resolved)
                };

                match self.do_forward(&path, &parts.headers, body_bytes, selected).await {
                    Ok((resp, ttfb, is_stream)) => {
                        let ttfb_ms = ttfb.as_millis() as u64;
                        info!("Responding: {} -> {} via {} | ttfb={}ms, body={}B, stream={}",
                            try_model, resolved, selected.name, ttfb_ms, body_size, is_stream);

                        if is_stream {
                            // 包裹 stream，完成后打印完整指标
                            let (new_body, done_rx) = instrument_stream(resp.into_body(), model.clone(), resolved.clone(), selected.name.clone(), ttfb, t_start);
                            let mut resp = Response::new(new_body);
                            resp.headers_mut().insert("x-accel-buffering", "no".parse().unwrap());
                            resp.headers_mut().insert("cache-control", "no-cache".parse().unwrap());
                            // 后台等待 stream 结束打印日志
                            let _ = done_rx;
                            return resp;
                        } else {
                            // 非流式：直接返回，已经可以算总时间了
                            let total_ms = t_start.elapsed().as_millis() as u64;
                            info!("Done: {} -> {} via {} | total={}ms, ttfb={}ms", model, resolved, selected.name, total_ms, ttfb_ms);
                            return resp;
                        }
                    }
                    Err(ForwardError::ClientError(status, body)) => {
                        let total_ms = t_start.elapsed().as_millis() as u64;
                        warn!("Client error from {}: {} - returning directly ({}ms)", selected.name, status, total_ms);
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

        let total_ms = t_start.elapsed().as_millis() as u64;
        error!("All models exhausted for original model={model} ({}ms)", total_ms);
        (StatusCode::SERVICE_UNAVAILABLE, "All backends exhausted").into_response()
    }

    /// 转发请求到指定后端，返回分类后的错误
    async fn do_forward(
        &self,
        path: &str,
        orig_headers: &HeaderMap,
        body: Vec<u8>,
        backend: &Backend,
    ) -> Result<(Response<Body>, Duration, bool), ForwardError> {
        // Rewrite /v1/ -> /v4/ 仅对 OpenAI 协议 + bigmodel 域名（coding API）
        // Anthropic 格式保持原始路径
        let forward_path = if backend.protocol == "openai" && backend.url.contains("bigmodel") {
            path.replacen("/v1/", "/v4/", 1)
        } else {
            path.to_string()
        };
        let url = format!("{}{}", backend.url.trim_end_matches('/'), forward_path);
        info!("Forwarding to {} (backend={}, protocol={})", url, backend.name, backend.protocol);

        let total_timeout = Duration::from_secs(backend.timeout_secs);
        let t_backend = Instant::now();

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
            let ttfb = t_backend.elapsed();
            let is_stream = resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map_or(false, |v| v.contains("text/event-stream"));
            Ok((self.stream_response(resp).await, ttfb, is_stream))
        } else if code == 429 {
            Err(ForwardError::RateLimited)
        } else if code == 401 {
            // key 无效，重试也没用，直接返回
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let body_str = String::from_utf8_lossy(&body_bytes);
            warn!("{} auth failed (401): {}", backend.name, body_str);
            let status_axum = StatusCode::from_u16(code).unwrap_or(StatusCode::UNAUTHORIZED);
            Err(ForwardError::ClientError(status_axum, body_str.into_owned()))
        } else if code >= 400 && code < 500 {
            // 400/403/404 等 → 后端能力不匹配，尝试下一个
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let body_str = String::from_utf8_lossy(&body_bytes);
            warn!("{} returned {} (4xx), falling back: {}", backend.name, status, body_str);
            Err(ForwardError::ServerErr(format!("{} returned {}: {}", backend.name, status, body_str)))
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

/// 包裹 stream body，追踪 token 数量，完成后打印完整性能指标
fn instrument_stream(
    body: Body,
    model: String,
    resolved: String,
    backend_name: String,
    ttfb: Duration,
    t_start: Instant,
) -> (Body, tokio::task::JoinHandle<()>) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(64);

    let done = tokio::spawn(async move {
        use http_body_util::BodyExt;

        let mut body = std::pin::pin!(body);
        let mut output_tokens: u32 = 0;
        let mut first_text_token: Option<Instant> = None;
        let mut buffer = String::new();

        while let Some(frame_result) = body.frame().await {
            let frame = match frame_result {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.send(Err(std::io::Error::new(std::io::ErrorKind::Other, e))).await;
                    break;
                }
            };

            if let Some(data) = frame.into_data().ok() {
                let text = String::from_utf8_lossy(&data);
                buffer.push_str(&text);

                while let Some(pos) = buffer.find('\n') {
                    let line = buffer[..pos].to_string();
                    buffer = buffer[pos + 1..].to_string();

                    if let Some(sse_data) = line.strip_prefix("data: ") {
                        if sse_data == "[DONE]" { continue; }
                        if let Some(val) = extract_json_uint(sse_data, "output_tokens") {
                            output_tokens = output_tokens.max(val);
                        }
                        if first_text_token.is_none() && sse_data.contains("\"text_delta\"") {
                            first_text_token = Some(Instant::now());
                        }
                    }
                }

                if tx.send(Ok(data)).await.is_err() { break; }
            }
        }

        let total_ms = t_start.elapsed().as_millis() as u64;
        let ttfb_ms = ttfb.as_millis() as u64;
        let ttft_ms = first_text_token
            .map(|t| t.duration_since(t_start).as_millis() as u64)
            .unwrap_or(ttfb_ms);
        let streaming_ms = total_ms.saturating_sub(ttft_ms);
        let tokens_per_sec = if output_tokens > 0 && streaming_ms > 0 {
            output_tokens as f64 / (streaming_ms as f64 / 1000.0)
        } else { 0.0 };

        info!(
            "Done: {} -> {} via {} | total={}ms, ttfb={}ms, ttft={}ms, tokens={output_tokens}, {tokens_per_sec:.1}tok/s",
            model, resolved, backend_name, total_ms, ttfb_ms, ttft_ms
        );
    });

    let new_body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    (new_body, done)
}

/// 从 SSE JSON data 中提取 "output_tokens": 数字
fn extract_json_uint(json: &str, field: &str) -> Option<u32> {
    let search = format!("\"{field}\":");
    let pos = json.find(&search)?;
    let after = json[pos + search.len()..].trim_start();
    let num: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    num.parse().ok()
}
