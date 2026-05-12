use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::request_prep::{
    extract_max_tokens, extract_model_from_json, has_negative_max_tokens, prepare_request_body,
};
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
use crate::balancer::{BackendStatus, WeightedRoundRobin};

pub struct ReadinessSnapshot {
    pub status: &'static str,
    pub ready_backends: usize,
    pub total_backends: usize,
    pub recovery_cooldown_ms: u64,
    pub backends: Vec<BackendStatus>,
}
use crate::config::{Backend, Config};
use crate::metrics::MetricsStore;
use crate::middleware::RateLimiter;
use crate::response_meta::inject_non_stream_response_headers;
use crate::selection::{choose_weighted_index, WeightFill};
use crate::streaming::instrument_stream;
use crate::runtime_health::RuntimeHealthError;
/// 转发错误分类，决定后续行为
enum ForwardError {
    /// 4xx 客户端错误 → 直接返回，不fallback
    ClientError(StatusCode, String, String, u64),
    /// 429 Rate Limit → 立即切换下一个key
    RateLimited,
    /// 4xx provider/client-specific error → fallback but do not penalize backend health
    RetryableClientStatus(String),
    /// 5xx/超时/连接失败 → 切换下一个后端并标记不健康
    ServerErr(String),
}

#[derive(Clone)]
pub struct Proxy {
    config: Arc<Config>,
    balancer: Arc<WeightedRoundRobin>,
    limiter: Arc<RateLimiter>,
    metrics: Arc<BackendMetrics>,
    /// 按 connect_timeout_secs 分组缓存的 reqwest client（避免热路径重复 build）
    client_cache: Arc<std::collections::HashMap<u64, Client>>,
    store: Arc<MetricsStore>,
    /// 运行态健康度：供 /livez 与未来 watchdog 使用。
    ///
    /// 约束：必须是低开销、无锁、无阻塞。
    runtime_health: crate::runtime_health::RuntimeHealth,
}

impl Proxy {
    pub fn new(config: Arc<Config>, balancer: Arc<WeightedRoundRobin>) -> Self {
        let mut timeouts = std::collections::HashSet::<u64>::new();
        for b in &config.backends {
            timeouts.insert(b.connect_timeout_secs);
        }
        if timeouts.is_empty() {
            timeouts.insert(3);
        }
        let mut client_cache = std::collections::HashMap::new();
        for secs in timeouts {
            let client = Client::builder()
                .pool_max_idle_per_host(50)
                .pool_idle_timeout(Duration::from_secs(120))
                .connect_timeout(Duration::from_secs(secs))
                .tcp_keepalive(Duration::from_secs(60))
                .tcp_keepalive_interval(Duration::from_secs(15))
                .tcp_keepalive_retries(3)
                .tcp_nodelay(true)
                .build()
                .expect("failed to build reqwest client");
            client_cache.insert(secs, client);
        }
        let log_dir = config.server.log_dir.clone();
        Self {
            config,
            balancer,
            limiter: Arc::new(RateLimiter::new()),
            client_cache: Arc::new(client_cache),
            metrics: Arc::new(BackendMetrics::new(10)),
            store: Arc::new(MetricsStore::new(&format!("{log_dir}/metrics"))),
            runtime_health: crate::runtime_health::RuntimeHealth::new(),
        }
    }

    pub fn balancer(&self) -> &Arc<WeightedRoundRobin> {
        &self.balancer
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }
    pub fn runtime_health(&self) -> &crate::runtime_health::RuntimeHealth {
        &self.runtime_health
    }

    pub fn readiness_snapshot(&self) -> ReadinessSnapshot {
        let backends = self.balancer.backend_statuses();
        let ready_backends = backends.iter().filter(|backend| backend.ready).count();
        ReadinessSnapshot {
            status: if ready_backends > 0 { "ready" } else { "not_ready" },
            ready_backends,
            total_backends: backends.len(),
            recovery_cooldown_ms: self.balancer.recovery_cooldown().as_millis() as u64,
            backends,
        }
    }

    fn is_healthy(&self, b: &Backend) -> bool {
        self.balancer.is_healthy(&b.name)
    }

    fn mark_backend_healthy(&self, backend_name: &str) {
        self.balancer.mark_healthy_by_name(backend_name);
    }

    fn mark_backend_unhealthy(&self, backend_name: &str) {
        self.balancer.mark_unhealthy_by_name(backend_name);
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

        // ingress: 记录请求进入代理热路径（尽量靠前，且不跨 await 持有状态）。
        self.runtime_health.request_started(now_ms());
        struct RequestInflightGuard {
            runtime_health: crate::runtime_health::RuntimeHealth,
        }
        impl Drop for RequestInflightGuard {
            fn drop(&mut self) {
                self.runtime_health.request_finished();
            }
        }
        let _inflight_guard = RequestInflightGuard {
            runtime_health: self.runtime_health.clone(),
        };

        // body read tracking + timeout
        let body_read_start_ms = now_ms();
        self.runtime_health.body_read_started(body_read_start_ms);
        let bytes = match tokio::time::timeout(
            Duration::from_secs(self.config.server.body_read_timeout_secs),
            axum::body::to_bytes(body, 10 * 1024 * 1024),
        )
        .await
        {
            Ok(Ok(b)) => {
                self.runtime_health.body_read_finished(now_ms());
                b
            }
            Ok(Err(e)) => {
                self.runtime_health.body_read_finished(now_ms());
                error!("Failed to read request body: {e}");
                return (StatusCode::BAD_REQUEST, "Failed to read body").into_response();
            }
            Err(_) => {
                self.runtime_health.body_read_finished(now_ms());
                self.runtime_health.record_error(RuntimeHealthError::BodyReadTimeout, now_ms());
                warn!("Request body read timed out (>{}s)", self.config.server.body_read_timeout_secs);
                return (StatusCode::REQUEST_TIMEOUT, "Request body read timed out").into_response();
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
        if has_negative_max_tokens(&bytes) {
            info!(
                "Rejecting request with negative max_tokens: path={path}, protocol={:?}",
                protocol
            );
            return (StatusCode::BAD_REQUEST, "max_tokens must be non-negative").into_response();
        }
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
                    .do_forward(path, &parts.headers, body_bytes, selected, &request_id)
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
                                self.runtime_health.clone(),
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
                            let stream_task_backend_name = selected.name.clone();
                            let stream_task_balancer = self.balancer.clone();
                            tokio::spawn(async move {
                                match done_rx.await {
                                    Ok(completion) if completion.timed_out => {
                                        warn!(
                                            "[{}] stream timed out; marking backend unhealthy",
                                            stream_task_request_id
                                        );
                                        stream_task_balancer
                                            .mark_unhealthy_by_name(&stream_task_backend_name);
                                    }
                                    Ok(_) => {
                                        stream_task_balancer
                                            .mark_healthy_by_name(&stream_task_backend_name);
                                    }
                                    Err(err) => {
                                        error!(
                                            "[{}] stream instrumentation task failed: {}",
                                            stream_task_request_id, err
                                        );
                                    }
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
                    Err(ForwardError::ClientError(status, body, backend_name, ttfb_ms)) => {
                        let total_ms = t_start.elapsed().as_millis() as u64;
                        info!(
                            "Client error from {}: {} - returning directly ({}ms)",
                            selected.name, status, total_ms
                        );
                        let mut resp = (status, body).into_response();
                        resp.headers_mut()
                            .insert("x-request-id", request_id.parse().unwrap());
                        resp.headers_mut()
                            .insert("x-backend", backend_name.parse().unwrap());
                        resp.headers_mut()
                            .insert("x-ttfb-ms", ttfb_ms.to_string().parse().unwrap());
                        resp.headers_mut()
                            .insert("x-total-ms", total_ms.to_string().parse().unwrap());
                        return resp;
                    }
                    Err(ForwardError::RateLimited) => {
                        info!(
                            "{} rate limited (429), switching to next backend",
                            selected.name
                        );
                        continue;
                    }
                    Err(ForwardError::RetryableClientStatus(e)) => {
                        info!("{} retryable 4xx: {}, trying next", selected.name, e);
                        continue;
                    }
                    Err(ForwardError::ServerErr(e)) => {
                        self.mark_backend_unhealthy(&selected.name);
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
        resp.headers_mut()
            .insert("x-backend", "exhausted".parse().unwrap());
        resp.headers_mut().insert("x-ttfb-ms", "0".parse().unwrap());
        resp.headers_mut()
            .insert("x-total-ms", total_ms.to_string().parse().unwrap());
        resp
    }

    /// 转发请求到指定后端，返回分类后的错误
    async fn do_forward(
        &self,
        path: &str,
        orig_headers: &HeaderMap,
        body: Bytes,
        backend: &Backend,
        request_id: &str,
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

        let connect_timeout_secs = backend.connect_timeout_secs;
        let client = self
            .client_cache
            .get(&connect_timeout_secs)
            .or_else(|| self.client_cache.values().next())
            .ok_or_else(|| ForwardError::ServerErr("client cache is empty".to_string()))?;

        let resolved_model =
            crate::model_resolution::resolved_model(backend, &extract_model_from_json(&body));

        let mut req_builder = client
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
            if is_stream {
                Ok((self.stream_response(resp).await, ttfb, true))
            } else {
                self.mark_backend_healthy(&backend.name);
                let response = self
                    .non_stream_response(
                        resp,
                        &backend.name,
                        &resolved_model,
                        request_id,
                        t_backend,
                        ttfb.as_millis() as u64,
                    )
                    .await;
                Ok((response, ttfb, false))
            }
        } else if code == 429 {
            Err(ForwardError::RateLimited)
        } else if code == 401 {
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let body_str = String::from_utf8_lossy(&body_bytes);
            warn!("{} auth failed (401): {}", backend.name, body_str);
            let status_axum = StatusCode::from_u16(code).unwrap_or(StatusCode::UNAUTHORIZED);
            Err(ForwardError::ClientError(
                status_axum,
                body_str.into_owned(),
                backend.name.clone(),
                t_backend.elapsed().as_millis() as u64,
            ))
        } else if (400..500).contains(&code) {
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let body_str = String::from_utf8_lossy(&body_bytes);
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
                    backend.name.clone(),
                    t_backend.elapsed().as_millis() as u64,
                ));
            }
            info!(
                "{} returned {} (4xx), falling back: {}",
                backend.name, status, body_str
            );
            Err(ForwardError::RetryableClientStatus(format!(
                "{} returned {}: {}",
                backend.name, status, body_str
            )))
        } else {
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

    async fn non_stream_response(
        &self,
        resp: reqwest::Response,
        backend_name: &str,
        resolved_model: &str,
        request_id: &str,
        started_at: Instant,
        ttfb_ms: u64,
    ) -> Response<Body> {
        let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
        let content_type = resp.headers().get(header::CONTENT_TYPE).cloned();
        let content_encoding = resp.headers().get(header::CONTENT_ENCODING).cloned();
        let body_bytes = resp.bytes().await.unwrap_or_default();
        let total_ms = started_at.elapsed().as_millis() as u64;

        if let Some(tokens) = extract_completion_tokens_from_json(&body_bytes)
            .or_else(|| estimate_tokens_from_body_json(&body_bytes))
        {
            let tok_per_sec = if tokens > 0 && total_ms > 0 {
                tokens as f64 / (total_ms as f64 / 1000.0)
            } else {
                0.0
            };
            self.metrics.record(backend_name, tok_per_sec);
            self.store.record(
                backend_name,
                resolved_model,
                tok_per_sec,
                ttfb_ms,
                ttfb_ms,
                total_ms,
                tokens,
            );
        }

        let mut resp = Response::new(Body::from(body_bytes));
        *resp.status_mut() = status;
        if let Some(ct) = content_type {
            resp.headers_mut().insert(header::CONTENT_TYPE, ct);
        }
        if let Some(ce) = content_encoding {
            resp.headers_mut().insert(header::CONTENT_ENCODING, ce);
        }
        inject_non_stream_response_headers(&mut resp, request_id, backend_name, ttfb_ms, total_ms);
        resp
    }

    pub fn limiter(&self) -> &Arc<RateLimiter> {
        &self.limiter
    }

    pub fn backend_avg_tok_per_sec(&self, backend_name: &str) -> f64 {
        self.metrics.avg(backend_name)
    }

    pub fn model_avg_tok_per_sec(&self, model: &str) -> f64 {
        self.store.avg_for_model(model)
    }
}

fn extract_completion_tokens_from_json(body: &[u8]) -> Option<u32> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    value
        .get("usage")?
        .get("completion_tokens")?
        .as_u64()
        .map(|v| v as u32)
}

fn estimate_tokens_from_body_json(body: &[u8]) -> Option<u32> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let mut text = String::new();
    collect_text_fields(&value, &mut text);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(count_tokens(trimmed))
    }
}

fn collect_text_fields(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::String(s) => out.push_str(s),
        serde_json::Value::Array(items) => {
            for item in items {
                collect_text_fields(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            if let Some(content) = map.get("content") {
                collect_content_value(content, out);
            }
            if let Some(message) = map.get("message") {
                collect_text_fields(message, out);
            }
            if let Some(text) = map.get("text").and_then(serde_json::Value::as_str) {
                out.push_str(text);
            }
        }
        _ => {}
    }
}

fn collect_content_value(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::String(s) => out.push_str(s),
        serde_json::Value::Array(items) => {
            for item in items {
                if let Some(text) = item.get("text").and_then(serde_json::Value::as_str) {
                    out.push_str(text);
                } else {
                    collect_content_value(item, out);
                }
            }
        }
        serde_json::Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(serde_json::Value::as_str) {
                out.push_str(text);
            }
        }
        _ => {}
    }
}

fn count_tokens(text: &str) -> u32 {
    static BPE: std::sync::OnceLock<tiktoken_rs::CoreBPE> = std::sync::OnceLock::new();
    let bpe = BPE.get_or_init(|| tiktoken_rs::cl100k_base().expect("tiktoken init failed"));
    bpe.encode_ordinary(text).len() as u32
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
    run_server_with_listener_and_hook(config, extra_routes, listener, |_| {} ).await;
}

/// 运行 server 并允许在 Proxy 构建完成后执行一个 hook。
///
/// 约束：hook 只用于非热路径初始化（例如 watchdog），禁止在其中做阻塞 I/O。
pub async fn run_server_with_listener_and_hook<F>(
    config: Config,
    extra_routes: Router<Arc<Proxy>>,
    listener: tokio::net::TcpListener,
    on_proxy_ready: F,
 )
where
    F: FnOnce(Arc<Proxy>) + Send + 'static,
{
    let config = Arc::new(config);
    let balancer = Arc::new(WeightedRoundRobin::with_recovery_cooldown(
        config.backends.clone(),
        config.retry,
        Duration::from_millis(config.retry_delay_ms),
        Duration::from_secs(config.server.recovery_cooldown_secs),
    ));
    let proxy = Arc::new(Proxy::new(config.clone(), balancer.clone()));
    let addr = listener.local_addr().expect("failed to read listener addr");

    // Start health check
    balancer.start_health_check(Duration::from_secs(30));

    // Tokio runtime tick：用于观测 runtime 是否在持续推进。
    // 注意：这里只做轻量 Atomic 写入；不要引入 I/O 或锁。
    start_runtime_tick_task(proxy.runtime_health.clone());

    // hook: 允许 unified.rs 启动 watchdog 等非热路径逻辑。
    on_proxy_ready(proxy.clone());

    let app = build_app(proxy.clone(), extra_routes);
    start_metrics_maintenance_task(proxy.store.clone(), proxy.runtime_health.clone());
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
        )
        .route("/ready", get(readiness_handler))
        .route("/livez", get(livez_handler));
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
fn start_metrics_maintenance_task(
    store: Arc<MetricsStore>,
    runtime_health: crate::runtime_health::RuntimeHealth,
 ) {
    tokio::spawn(async move {
        loop {
            let store = store.clone();
            let runtime_health = runtime_health.clone();
            // flush() does synchronous disk IO; keep it off Tokio worker threads.
            let ok = tokio::task::spawn_blocking(move || {
                store.flush();
                store.evict_stale(24 * 3600);
            })
            .await
            .is_ok();
            if ok {
                runtime_health.tick_metrics_flush_ok(now_ms());
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });
}

/// 每秒更新一次 runtime tick，供 /livez 与未来 watchdog 读取。
fn start_runtime_tick_task(runtime_health: crate::runtime_health::RuntimeHealth) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            runtime_health.tick_runtime(now_ms());
        }
    });
}

/// 绑定 TCP listener（共享给 binary 入口与测试）。
pub fn bind_listener(addr: SocketAddr) -> tokio::net::TcpListener {
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn backends_handler(State(proxy): State<Arc<Proxy>>) -> axum::Json<serde_json::Value> {
    let backends: Vec<serde_json::Value> = proxy
        .balancer()
        .backend_statuses()
        .into_iter()
        .map(|backend| {
            serde_json::json!({
                "name": backend.name,
                "url": backend.url,
                "healthy": backend.healthy,
                "ready": backend.ready,
                "fail_count": backend.fail_count,
                "circuit_state": backend.circuit_state.as_str(),
                "opened_since_ms": backend.opened_since_ms,
            })
        })
        .collect();
    axum::Json(serde_json::json!({"backends": backends}))
}

async fn readiness_handler(State(proxy): State<Arc<Proxy>>) -> impl IntoResponse {
    let snapshot = proxy.readiness_snapshot();
    let status = if snapshot.ready_backends > 0 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let backends: Vec<serde_json::Value> = snapshot
        .backends
        .into_iter()
        .map(|backend| {
            serde_json::json!({
                "name": backend.name,
                "url": backend.url,
                "healthy": backend.healthy,
                "ready": backend.ready,
                "fail_count": backend.fail_count,
                "circuit_state": backend.circuit_state.as_str(),
                "opened_since_ms": backend.opened_since_ms,
            })
        })
        .collect();
    (
        status,
        axum::Json(serde_json::json!({
            "status": snapshot.status,
            "ready_backends": snapshot.ready_backends,
            "total_backends": snapshot.total_backends,
            "recovery_cooldown_ms": snapshot.recovery_cooldown_ms,
            "backends": backends,
        })),
    )
}

/// /livez：运行时活性探针（必须 public）。
///
/// 设计目标：轻量、无阻塞、无锁跨 await。
async fn livez_handler(State(proxy): State<Arc<Proxy>>) -> impl IntoResponse {
    let now_ms = now_ms();
    let snapshot = proxy.runtime_health.snapshot(now_ms);
    let decision = crate::runtime_health::decide(&snapshot);
    let decision_str = match decision.decision {
        crate::runtime_health::Decision::Live => "Live",
        crate::runtime_health::Decision::Suspect => "Suspect",
        crate::runtime_health::Decision::Stalled => "Stalled",
    };
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "snapshot": snapshot,
            "decision": {
                "decision": decision_str,
                "reason": decision.reason,
            }
        })),
    )
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
    use super::{
        backends_handler, estimate_tokens_from_body_json, extract_bearer_key,
        extract_completion_tokens_from_json, livez_handler, readiness_handler, Proxy,
    };
    use crate::balancer::WeightedRoundRobin;
    use crate::config::{AuthConfig, Config, ServerConfig};
    use axum::extract::State;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

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

    fn empty_proxy() -> Arc<Proxy> {
        let config = Arc::new(Config {
            server: ServerConfig {
                port: 0,
                timeout_secs: 1,
                log_dir: "target/test-proxy-empty".to_string(),
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
                enabled: false,
                keys: Vec::new(),
                key_index: HashMap::new(),
            },
            backends: Vec::new(),
            retry: 0,
            retry_delay_ms: 0,
            fallback: HashMap::new(),
            model_mapping: HashMap::new(),
        });
        let balancer = Arc::new(WeightedRoundRobin::new(
            Vec::new(),
            0,
            Duration::from_millis(0),
        ));
        Arc::new(Proxy::new(config, balancer))
    }

    #[test]
    fn proxy_with_no_backends_reports_not_ready_and_has_default_client_cache() {
        let proxy = empty_proxy();
        let snapshot = proxy.readiness_snapshot();

        assert_eq!(snapshot.status, "not_ready");
        assert_eq!(snapshot.ready_backends, 0);
        assert_eq!(snapshot.total_backends, 0);
        assert_eq!(snapshot.recovery_cooldown_ms, 60_000);
        assert!(proxy.client_cache.contains_key(&3));
    }

    #[tokio::test]
    async fn backends_handler_returns_empty_backend_list_without_consuming_state() {
        let response = backends_handler(State(empty_proxy())).await;

        assert_eq!(response.0["backends"].as_array().expect("backends array").len(), 0);
    }

    #[tokio::test]
    async fn readiness_handler_returns_service_unavailable_when_no_backends_are_ready() {
        let response = readiness_handler(State(empty_proxy())).await.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn livez_handler_returns_snapshot_and_live_decision_after_tick() {
        let proxy = empty_proxy();
        proxy.runtime_health.tick_runtime(super::now_ms());

        let response = livez_handler(State(proxy)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn proxy_token_helpers_extract_usage_and_estimate_nested_content() {
        let usage_body = br#"{"usage":{"completion_tokens":17}}"#;
        assert_eq!(extract_completion_tokens_from_json(usage_body), Some(17));

        let nested_body = br#"{
            "message":{"content":[{"text":"hello"},{"content":{"text":" world"}}]},
            "text":"!"
        }"#;
        assert!(estimate_tokens_from_body_json(nested_body).is_some_and(|tokens| tokens > 0));
    }

    #[test]
    fn proxy_token_estimator_rejects_invalid_or_empty_json() {
        assert_eq!(extract_completion_tokens_from_json(b"not json"), None);
        assert_eq!(estimate_tokens_from_body_json(br#"{"message":{"content":"   "}}"#), None);
        assert_eq!(estimate_tokens_from_body_json(b"not json"), None);
    }
}
