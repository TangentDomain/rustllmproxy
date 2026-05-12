use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// RuntimeHealth：在热路径维护的低开销运行态健康度指标。
///
/// 设计目标：
/// - **低开销**：仅 Atomic 读写，不做锁与 blocking I/O
/// - **可快照**：watchdog / /livez 可在任意线程读取快照
///
/// 说明：本文件仅实现 Task1 需要的最小 API（快照 + 判定逻辑）。
#[derive(Debug, Default, Clone)]
pub struct RuntimeHealth {
    inner: Arc<RuntimeHealthInner>,
}

#[derive(Debug, Default)]
struct RuntimeHealthInner {
    last_runtime_tick_ms: AtomicU64,
    last_http_seen_ms: AtomicU64,
    inflight_requests: AtomicU32,
    inflight_streams: AtomicU32,
    body_read_inflight: AtomicU32,
    oldest_inflight_ms: AtomicU64,
    last_body_read_start_ms: AtomicU64,
    last_body_read_done_ms: AtomicU64,
    last_error_code: AtomicU64,
    last_error_at_ms: AtomicU64,
    last_metrics_flush_ok_ms: AtomicU64,
    last_effective_stream_chunk_ms: AtomicU64,
}

impl RuntimeHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Tokio runtime 内部 tick 打点。
    pub fn tick_runtime(&self, now_ms: u64) {
        self.inner.last_runtime_tick_ms.store(now_ms, Ordering::Relaxed);
    }

    /// 收到 HTTP 请求的打点（不区分是否鉴权通过）。
    pub fn tick_http_seen(&self, now_ms: u64) {
        self.inner.last_http_seen_ms.store(now_ms, Ordering::Relaxed);
    }

    /// 请求进入 proxy 热路径。
    pub fn request_started(&self, now_ms: u64) {
        self.inner.last_http_seen_ms.store(now_ms, Ordering::Relaxed);
        let prev = self.inner.inflight_requests.fetch_add(1, Ordering::Relaxed);
        if prev == 0 {
            self.inner.oldest_inflight_ms.store(now_ms, Ordering::Relaxed);
        }
    }

    /// 请求离开 proxy 热路径。
    pub fn request_finished(&self) {
        let prev = saturating_fetch_sub_u32(&self.inner.inflight_requests, Ordering::Relaxed);
        if prev <= 1 {
            self.inner.oldest_inflight_ms.store(0, Ordering::Relaxed);
        }
    }

    pub fn inflight_requests_inc(&self) {
        self.inner.inflight_requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inflight_requests_dec(&self) {
        saturating_fetch_sub_u32(&self.inner.inflight_requests, Ordering::Relaxed);
    }

    pub fn inflight_streams_inc(&self) {
        self.inner.inflight_streams.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inflight_streams_dec(&self) {
        saturating_fetch_sub_u32(&self.inner.inflight_streams, Ordering::Relaxed);
    }

    /// 记录一次“有效 SSE chunk”（内容/token），用于 /livez 观测流式活性。
    pub fn tick_effective_stream_chunk(&self, now_ms: u64) {
        self.inner
            .last_effective_stream_chunk_ms
            .store(now_ms, Ordering::Relaxed);
    }

    /// 记录一次 metrics maintenance flush 成功（磁盘落盘/清理完成）。
    pub fn tick_metrics_flush_ok(&self, now_ms: u64) {
        self.inner
            .last_metrics_flush_ok_ms
            .store(now_ms, Ordering::Relaxed);
    }
    /// 开始读取请求 body。
    pub fn body_read_started(&self, now_ms: u64) {
        self.inner.body_read_inflight.fetch_add(1, Ordering::Relaxed);
        self.inner.last_body_read_start_ms.store(now_ms, Ordering::Relaxed);
    }

    /// 完成（成功或失败）请求 body 读取。
    pub fn body_read_finished(&self, now_ms: u64) {
        saturating_fetch_sub_u32(&self.inner.body_read_inflight, Ordering::Relaxed);
        self.inner.last_body_read_done_ms.store(now_ms, Ordering::Relaxed);
    }

    pub fn body_read_inflight_inc(&self) {
        self.inner.body_read_inflight.fetch_add(1, Ordering::Relaxed);
    }
    pub fn body_read_inflight_dec(&self) {
        saturating_fetch_sub_u32(&self.inner.body_read_inflight, Ordering::Relaxed);
    }

    /// 记录最近一次错误。
    pub fn record_error(&self, code: RuntimeHealthError, now_ms: u64) {
        self.inner.last_error_code.store(code as u64, Ordering::Relaxed);
        self.inner.last_error_at_ms.store(now_ms, Ordering::Relaxed);
    }

    /// 生成一个一致性要求较低（Relaxed）的快照。
    pub fn snapshot(&self, now_ms: u64) -> RuntimeHealthSnapshot {
        let oldest_inflight_raw = self.inner.oldest_inflight_ms.load(Ordering::Relaxed);
        let last_error_code = self.inner.last_error_code.load(Ordering::Relaxed);
        let last_error_at_ms = self.inner.last_error_at_ms.load(Ordering::Relaxed);
        RuntimeHealthSnapshot {
            now_ms,
            last_runtime_tick_ms: self.inner.last_runtime_tick_ms.load(Ordering::Relaxed),
            last_http_seen_ms: self.inner.last_http_seen_ms.load(Ordering::Relaxed),
            inflight_requests: self.inner.inflight_requests.load(Ordering::Relaxed),
            inflight_streams: self.inner.inflight_streams.load(Ordering::Relaxed),
            body_read_inflight: self.inner.body_read_inflight.load(Ordering::Relaxed),
            oldest_inflight_ms: non_zero_to_option(oldest_inflight_raw),
            last_body_read_start_ms: non_zero_to_option(
                self.inner.last_body_read_start_ms.load(Ordering::Relaxed),
            ),
            last_body_read_done_ms: non_zero_to_option(
                self.inner.last_body_read_done_ms.load(Ordering::Relaxed),
            ),
            last_error: RuntimeHealthError::from_code(last_error_code).map(|code| {
                RuntimeHealthErrorState {
                    code: code.as_str().to_string(),
                    at_ms: last_error_at_ms,
                }
            }),
            last_effective_stream_chunk_ms: non_zero_to_option(
                self.inner
                    .last_effective_stream_chunk_ms
                    .load(Ordering::Relaxed),
            ),
            last_metrics_flush_ok_ms: non_zero_to_option(
                self.inner.last_metrics_flush_ok_ms.load(Ordering::Relaxed),
            ),
        }
    }
}

/// 运行态快照：用于 /livez 输出、watchdog 判定与测试。
///
/// 中文说明：字段保持“纯数据”形态，避免在快照层引入复杂计算。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeHealthSnapshot {
    pub now_ms: u64,
    pub last_runtime_tick_ms: u64,
    pub last_http_seen_ms: u64,
    pub inflight_requests: u32,
    pub inflight_streams: u32,
    pub body_read_inflight: u32,
    pub oldest_inflight_ms: Option<u64>,
    pub last_body_read_start_ms: Option<u64>,
    pub last_body_read_done_ms: Option<u64>,
    pub last_error: Option<RuntimeHealthErrorState>,
    pub last_effective_stream_chunk_ms: Option<u64>,
    pub last_metrics_flush_ok_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeHealthErrorState {
    pub code: String,
    pub at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum RuntimeHealthError {
    BodyReadTimeout = 1,
}

impl RuntimeHealthError {
    fn from_code(code: u64) -> Option<Self> {
        match code {
            1 => Some(Self::BodyReadTimeout),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::BodyReadTimeout => "body_read_timeout",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// 运行态正常（可继续对外服务）。
    Live,
    /// 运行态疑似异常（可用于未来扩展）。
    Suspect,
    /// 运行态已卡死/不可恢复（watchdog 应触发 self-reexec）。
    Stalled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionOutput {
    pub decision: Decision,
    pub reason: String,
}

/// 判定逻辑：仅依赖快照，不做 I/O。
///
/// 当前最小实现：
/// - runtime tick 超过阈值未更新 => Stalled（reason 必含 "runtime" 子串，供测试断言）
/// - 否则 => Live
pub fn decide(snap: &RuntimeHealthSnapshot) -> DecisionOutput {
    // 经验阈值：后续会由 config 提供；Task1 先用常量。
    const RUNTIME_TICK_STALL_MS: u64 = 5_000;

    let runtime_age = snap.now_ms.saturating_sub(snap.last_runtime_tick_ms);
    if runtime_age > RUNTIME_TICK_STALL_MS {
        return DecisionOutput {
            decision: Decision::Stalled,
            reason: format!("runtime tick stalled: age_ms={runtime_age}"),
        };
    }

    DecisionOutput {
        decision: Decision::Live,
        reason: "ok".to_string(),
    }
}

fn non_zero_to_option(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

fn saturating_fetch_sub_u32(counter: &AtomicU32, ordering: Ordering) -> u32 {
    let mut current = counter.load(ordering);
    loop {
        if current == 0 {
            return 0;
        }
        match counter.compare_exchange_weak(
            current,
            current - 1,
            ordering,
            ordering,
        ) {
            Ok(prev) => return prev,
            Err(observed) => current = observed,
        }
    }
}