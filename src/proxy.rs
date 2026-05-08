use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::request_prep::{extract_max_tokens, extract_model_from_json, prepare_request_body};
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, Request, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use reqwest::Client;
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info, info_span, warn};

use crate::backend_metrics::BackendMetrics;
use crate::balancer::WeightedRoundRobin;
use crate::config::{Backend, Config};
use crate::metrics::MetricsStore;
use crate::middleware::RateLimiter;
use crate::selection::{choose_weighted_index, WeightFill};
use crate::streaming::instrument_stream;

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
    metrics: Arc<BackendMetrics>,
    client: Client,
    store: Arc<MetricsStore>,
}

impl Proxy {
    pub fn new(config: Arc<Config>, balancer: Arc<WeightedRoundRobin>) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(50)
            .pool_idle_timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(3))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_keepalive_interval(Duration::from_secs(15))
            .tcp_keepalive_retries(3)
            .tcp_nodelay(true)
            .build()
            .expect("failed to build reqwest client");
        let log_dir = config.server.log_dir.clone();
        Self {
            config,
            balancer,
            limiter: Arc::new(RateLimiter::new()),
            client,
            metrics: Arc::new(BackendMetrics::new(10)),
            store: Arc::new(MetricsStore::new(&format!("{log_dir}/metrics"))),
        }
    }

    pub fn balancer(&self) -> &Arc<WeightedRoundRobin> {
        &self.balancer
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// 检查后端是否健康（O(1) 查找）
    fn is_healthy(&self, b: &Backend) -> bool {
        self.balancer.is_healthy(&b.name)
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
        let request_id = format!("{:016x}", rand::random::<u64>());
        let span = info_span!("req", id = %request_id);
        let _enter = span.enter();

        let t_start = Instant::now();
        let fallback_deadline =
            t_start + Duration::from_secs(self.config.server.fallback_timeout_secs);
        let (parts, body) = req.into_parts();
        let path = parts.uri.path();

        let protocol = parts.extensions.get::<String>().map(String::as_str);

        let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read request body: {e}");
                return (StatusCode::BAD_REQUEST, "Failed to read body").into_response();
            }
        };

        let model = extract_model_from_json(&bytes);
        if model.trim().is_empty() {
            info!(
                "Rejecting request with missing or empty model field: path={path}, protocol={:?}",
                protocol
            );
            return (StatusCode::BAD_REQUEST, "Missing or empty model field").into_response();
        }
        let body_size = bytes.len();
        let original_model = model.clone();
        let mut mapping_logged = false;
        let model = if let Some(group) = self.config.model_mapping.get(&model) {
            if group.len() == 1 {
                group[0].clone()
            } else {
                // Weighted random by observed model tok/s (aggregated across backends).
                // If no data yet for any model, fall back to uniform random.
                let raw: Vec<f64> = group.iter().map(|m| self.store.avg_for_model(m)).collect();
                let detail = choose_weighted_index(
                    group.len(),
                    |idx| raw[idx],
                    WeightFill::MissingEqualsOne,
                );
                let reason = if detail.used_uniform_fallback {
                    "uniform(no_data)"
                } else {
                    "weighted(tok/s)"
                };
                let chosen = group[detail.index].clone();
                mapping_logged = true;
                info!(
                    "Model mapping: {} -> {} | reason={}, candidates={:?}, raw_tok_s={:?}, weights={:?}, picked_index={}/{}, random_value={}, total_weight={}",
                    original_model,
                    chosen,
                    reason,
                    group,
                    raw,
                    detail.weights,
                    detail.index,
                    group.len(),
                    detail
                        .random_value
                        .map(|v| format!("{v:.2}"))
                        .unwrap_or_else(|| "N/A".to_string()),
                    detail.total_weight
                );
                chosen
            }
        } else {
            model
        };
        if model != original_model && !mapping_logged {
            info!("Model mapping: {original_model} -> {model}");
        }
        let max_tokens = extract_max_tokens(&bytes);
        info!("Request model={model}, path={path}, protocol={:?}, body={body_size}B, max_tokens={max_tokens:?}", protocol);

        let chain = self.config.get_fallback_chain(&model);

        for try_model in &chain {
            let all_backends = self.config.find_backends_for_model(try_model, protocol);
            if all_backends.is_empty() {
                info!("No backend supports model={try_model}, skipping");
                continue;
            }

            let healthy: Vec<_> = all_backends.iter().filter(|b| self.is_healthy(b)).collect();

            if healthy.is_empty() {
                info!("No healthy backend for model={try_model}, skip to next model");
                continue;
            }

            info!(
                "Trying model={}, healthy backends={}/{}",
                try_model,
                healthy.len(),
                all_backends.len()
            );

            if Instant::now() >= fallback_deadline {
                warn!(
                    "Fallback deadline exceeded ({}s), aborting for model={}",
                    self.config.server.fallback_timeout_secs, try_model
                );
                break;
            }

            // Adaptive: weighted random by recent tok/s, faster backends get more traffic
            // Backends with no data get the average of those with data (proportional, no starvation)
            let detail = choose_weighted_index(
                healthy.len(),
                |idx| self.metrics.avg(&healthy[idx].name),
                WeightFill::MissingLessOrEqualOne,
            );
            let start = detail.index;
            for i in 0..healthy.len() {
                let selected = &healthy[(start + i) % healthy.len()];
                let resolved = crate::model_resolution::resolved_model(selected, try_model);
                // Prepare body: model patch + backend-specific preprocessing
                let body_bytes =
                    prepare_request_body(&bytes, &original_model, try_model, &resolved, selected);

                match self
                    .do_forward(path, &parts.headers, body_bytes, selected)
                    .await
                {
                    Ok((mut resp, ttfb, is_stream)) => {
                        let ttfb_ms = ttfb.as_millis() as u64;
                        info!(
                            "Responding: {} -> {} via {} | ttfb={}ms, body={}B, stream={}",
                            try_model, resolved, selected.name, ttfb_ms, body_size, is_stream
                        );

                        if is_stream {
                            let idle_timeout =
                                Duration::from_secs(self.config.server.stream_idle_timeout_secs);
                            let first_chunk_timeout = Duration::from_secs(
                                self.config.server.stream_first_chunk_timeout_secs,
                            );
                            let stream_total_timeout =
                                Duration::from_secs(self.config.server.timeout_secs);
                            let (new_body, done_rx) = instrument_stream(
                                resp.into_body(),
                                model.clone(),
                                resolved.clone(),
                                selected.name.clone(),
                                ttfb,
                                t_start,
                                idle_timeout,
                                first_chunk_timeout,
                                stream_total_timeout,
                                self.metrics.clone(),
                                self.store.clone(),
                                request_id.clone(),
                            );
                            let mut resp = Response::new(new_body);
                            resp.headers_mut()
                                .insert("x-accel-buffering", "no".parse().unwrap());
                            resp.headers_mut()
                                .insert("cache-control", "no-cache".parse().unwrap());
                            resp.headers_mut()
                                .insert("x-request-id", request_id.parse().unwrap());
                            resp.headers_mut()
                                .insert("x-backend", selected.name.parse().unwrap());
                            resp.headers_mut()
                                .insert("x-ttfb-ms", ttfb_ms.to_string().parse().unwrap());
                            let stream_task_request_id = request_id.clone();
                            tokio::spawn(async move {
                                if let Err(err) = done_rx.await {
                                    error!(
                                        "[{}] stream instrumentation task failed: {}",
                                        stream_task_request_id, err
                                    );
                                }
                            });
                            return resp;
                        } else {
                            let total_ms = t_start.elapsed().as_millis() as u64;
                            info!(
                                "Done: {} -> {} via {} | total={}ms, ttfb={}ms",
                                model, resolved, selected.name, total_ms, ttfb_ms
                            );
                            resp.headers_mut()
                                .insert("x-request-id", request_id.parse().unwrap());
                            resp.headers_mut()
                                .insert("x-backend", selected.name.parse().unwrap());
                            resp.headers_mut()
                                .insert("x-ttfb-ms", ttfb_ms.to_string().parse().unwrap());
                            resp.headers_mut()
                                .insert("x-total-ms", total_ms.to_string().parse().unwrap());
                            return resp;
                        }
                    }
                    Err(ForwardError::ClientError(status, body)) => {
                        let total_ms = t_start.elapsed().as_millis() as u64;
                        info!(
                            "Client error from {}: {} - returning directly ({}ms)",
                            selected.name, status, total_ms
                        );
                        return (status, body).into_response();
                    }
                    Err(ForwardError::RateLimited) => {
                        info!(
                            "{} rate limited (429), switching to next backend",
                            selected.name
                        );
                        continue;
                    }
                    Err(ForwardError::ServerErr(e)) => {
                        info!("{} server error: {}, trying next", selected.name, e);
                        continue;
                    }
                }
            }
            info!("All backends exhausted for model={try_model}, falling back");
        }

        let total_ms = t_start.elapsed().as_millis() as u64;
        error!(
            "All models exhausted for original model={model} ({}ms)",
            total_ms
        );
        let mut resp = (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "error": "All backends exhausted",
                "original_model": original_model,
                "resolved_model": model,
                "elapsed_ms": total_ms,
                "request_id": request_id,
            })),
        )
            .into_response();
        resp.headers_mut()
            .insert("x-request-id", request_id.parse().unwrap());
        resp
    }

    /// 转发请求到指定后端，返回分类后的错误
    async fn do_forward(
        &self,
        path: &str,
        orig_headers: &HeaderMap,
        body: Bytes,
        backend: &Backend,
    ) -> Result<(Response<Body>, Duration, bool), ForwardError> {
        // Rewrite /v1/ -> /v4/ 仅对 OpenAI 协议 + bigmodel 域名（coding API）
        // Anthropic 格式保持原始路径
        let forward_path = if backend.protocol == "openai" && backend.url.contains("bigmodel") {
            path.replacen("/v1/", "/v4/", 1)
        } else {
            path.to_string()
        };
        let url = format!(
            "{}/{}",
            backend.url.trim_end_matches('/'),
            forward_path.trim_start_matches('/')
        );
        info!(
            "Forwarding to {} (backend={}, protocol={})",
            url, backend.name, backend.protocol
        );

        let t_backend = Instant::now();

        let mut req_builder = self
            .client
            .post(&url)
            .timeout(Duration::from_secs(backend.timeout_secs))
            .body(body);

        // 使用预计算的 auth_header（启动时生成，避免每次 format! 分配）
        match backend.protocol.as_str() {
            "anthropic" => {
                req_builder = req_builder
                    .header("x-api-key", &backend.api_key)
                    .header("anthropic-version", "2023-06-01");
            }
            _ => {
                req_builder = req_builder.header(header::AUTHORIZATION, &backend.auth_header);
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
            let is_stream = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.contains("text/event-stream"));
            Ok((self.stream_response(resp).await, ttfb, is_stream))
        } else if code == 429 {
            Err(ForwardError::RateLimited)
        } else if code == 401 {
            // key 无效，重试也没用，直接返回
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let body_str = String::from_utf8_lossy(&body_bytes);
            warn!("{} auth failed (401): {}", backend.name, body_str);
            let status_axum = StatusCode::from_u16(code).unwrap_or(StatusCode::UNAUTHORIZED);
            Err(ForwardError::ClientError(
                status_axum,
                body_str.into_owned(),
            ))
        } else if (400..500).contains(&code) {
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let body_str = String::from_utf8_lossy(&body_bytes);
            // 422 Unprocessable Entity: 通常是请求参数校验失败（max_tokens超限、格式错误等）
            // 换后端大概率一样失败，直接返回避免浪费时间
            if code == 422 {
                info!(
                    "{} returned 422 (param error), returning directly: {}",
                    backend.name, body_str
                );
                let status_axum =
                    StatusCode::from_u16(code).unwrap_or(StatusCode::UNPROCESSABLE_ENTITY);
                return Err(ForwardError::ClientError(
                    status_axum,
                    body_str.into_owned(),
                ));
            }
            // 400/403/404/415 等 → 不同 provider 可能行为不同，值得 fallback
            info!(
                "{} returned {} (4xx), falling back: {}",
                backend.name, status, body_str
            );
            Err(ForwardError::ServerErr(format!(
                "{} returned {}: {}",
                backend.name, status, body_str
            )))
        } else {
            // 5xx 服务端错误 → 切换下一个后端
            Err(ForwardError::ServerErr(format!(
                "{} returned {}",
                backend.name, status
            )))
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

    let key = crate::middleware::extract_client_credential_from_request(&request);
    let key = match key {
        Some(k) => k,
        None => {
            let method = request.method();
            let uri = request.uri();
            let path = uri.path();
            let query = uri.query().unwrap_or("");
            let user_agent = request
                .headers()
                .get("user-agent")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("unknown");
            let real_ip = request
                .headers()
                .get("x-real-ip")
                .or_else(|| request.headers().get("x-forwarded-for"))
                .and_then(|h| h.to_str().ok())
                .unwrap_or("unknown");

            info!(
                "Request missing auth: method={}, path={}, query={}, user_agent={}, real_ip={}",
                method, path, query, user_agent, real_ip
            );
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
            warn!(
                "Invalid API key: method={}, path={}, key_prefix={}",
                method, path, key_preview
            );
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    if !proxy.limiter().check(&key, api_key.rate_limit) {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    request
        .extensions_mut()
        .insert(crate::middleware::AuthInfo {
            key_name: api_key.name.clone(),
        });
    Ok(next.run(request).await)
}

// ---- Request body inspection / rewrite boundary moved to crate::request_prep ----

#[cfg(test)]
fn extract_bearer_key(req: &Request<Body>) -> Option<String> {
    crate::middleware::extract_client_credential_from_request(req)
}

#[cfg(not(test))]
#[allow(dead_code)]
fn extract_bearer_key(req: &Request<Body>) -> Option<String> {
    crate::middleware::extract_client_credential_from_request(req)
}

// --- Shared server runner ---

pub async fn run_server(config: Config, extra_routes: Router<Arc<Proxy>>) {
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.port));
    let listener = bind_listener(addr);
    run_server_with_listener(config, extra_routes, listener).await;
}

pub async fn run_server_with_listener(
    config: Config,
    extra_routes: Router<Arc<Proxy>>,
    listener: tokio::net::TcpListener,
) {
    let config = Arc::new(config);
    let balancer = Arc::new(WeightedRoundRobin::new(
        config.backends.clone(),
        config.retry,
        Duration::from_millis(config.retry_delay_ms),
    ));
    let proxy = Arc::new(Proxy::new(config.clone(), balancer.clone()));
    let addr = listener.local_addr().expect("failed to read listener addr");

    // Start health check
    balancer.start_health_check(Duration::from_secs(30));

    let app = build_app(proxy.clone(), extra_routes);
    start_metrics_maintenance_task(proxy.store.clone());

    info!("Listening on {addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

/// 构建 axum Router：public(/health) + protected(/backends + extra_routes + fallback) + layers + state
///
/// 注意：行为必须与 run_server_with_listener 之前保持一致：
/// - /health 必须公开（无 auth）
/// - protected_routes 必须挂载 auth_layer，并使用 Arc<Proxy> 作为 state
/// - TraceLayer/CorsLayer 的层级顺序保持不变
fn build_app(proxy: Arc<Proxy>, extra_routes: Router<Arc<Proxy>>) -> Router {
    // 公共路由（不需要认证）
    let public_routes = Router::new().route(
        "/health",
        get(|| async { axum::Json(serde_json::json!({"status": "ok"})) }),
    );

    // 受保护路由（需要认证）
    let protected_routes = Router::new()
        .route("/backends", get(backends_handler))
        .merge(extra_routes)
        .fallback(post(
            |state: State<Arc<Proxy>>, req: Request<Body>| async move { state.0.handle(req).await },
        ))
        .layer(axum::middleware::from_fn_with_state(
            proxy.clone(),
            auth_layer,
        ));

    public_routes
        .merge(protected_routes)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(proxy)
}

/// 周期性 metrics flush + 过期清理（每 30s）
///
/// - flush() 为同步磁盘 IO，必须放到 spawn_blocking
/// - 清理 24h 无新数据的 entry，防止 DashMap 键空间无限增长
fn start_metrics_maintenance_task(store: Arc<MetricsStore>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let store = store.clone();
            // flush() does synchronous disk IO; keep it off Tokio worker threads.
            let _ = tokio::task::spawn_blocking(move || {
                store.flush();
                store.evict_stale(24 * 3600);
            })
            .await;
        }
    });
}

fn bind_listener(addr: SocketAddr) -> tokio::net::TcpListener {
    use tokio::net::TcpSocket;

    let tcp_socket = TcpSocket::new_v4().expect("failed to create TCP socket");
    tcp_socket
        .set_reuseaddr(true)
        .expect("failed to set reuseaddr");
    tcp_socket
        .set_keepalive(true)
        .expect("failed to set keepalive");
    tcp_socket.bind(addr).expect("failed to bind");
    tcp_socket.listen(1024).expect("failed to listen")
}

async fn backends_handler(State(proxy): State<Arc<Proxy>>) -> axum::Json<serde_json::Value> {
    let backends: Vec<serde_json::Value> = proxy
        .balancer()
        .all_backends()
        .iter()
        .map(|b| {
            serde_json::json!({
                "name": b.backend.name,
                "url": b.backend.url,
                "healthy": b.healthy.load(std::sync::atomic::Ordering::Relaxed),
                "fail_count": b.fail_count.load(std::sync::atomic::Ordering::Relaxed),
            })
        })
        .collect();
    axum::Json(serde_json::json!({"backends": backends}))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to listen for ctrl+c");
        "ctrl_c"
    };

    #[cfg(unix)]
    let terminate = async {
        let mut sigterm =
            unix_signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
        sigterm.recv().await;
        "sigterm"
    };

    #[cfg(unix)]
    let signal_name = tokio::select! {
        name = ctrl_c => name,
        name = terminate => name,
    };

    #[cfg(not(unix))]
    let signal_name = ctrl_c.await;

    info!("Shutting down on {}...", signal_name);
}
#[cfg(test)]
mod tests {
    use super::extract_bearer_key;
    use axum::http::Request;

    #[test]
    fn extract_bearer_key_is_compatible_with_existing_behavior() {
        let header_request = Request::builder()
            .uri("/openai/v1/chat/completions?api_key=query-key")
            .header("authorization", "Bearer header-key")
            .body(axum::body::Body::empty())
            .expect("header request");
        assert_eq!(
            extract_bearer_key(&header_request).as_deref(),
            Some("header-key")
        );

        let query_request = Request::builder()
            .uri("/openai/v1/chat/completions?foo=1&api_key=query-key&bar=2")
            .body(axum::body::Body::empty())
            .expect("query request");
        assert_eq!(
            extract_bearer_key(&query_request).as_deref(),
            Some("query-key")
        );
    }
}
