use crate::config::Backend;
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 默认熔断冷却时间（毫秒）
const DEFAULT_RECOVERY_COOLDOWN_MS: u64 = 60_000;
const FAILURE_THRESHOLD: u64 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

impl CircuitState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Open,
            2 => Self::HalfOpen,
            _ => Self::Closed,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BackendStatus {
    pub name: String,
    pub url: String,
    pub healthy: bool,
    pub ready: bool,
    pub fail_count: u64,
    pub circuit_state: CircuitState,
    pub opened_since_ms: u64,
}

#[derive(Debug)]
pub struct BackendState {
    pub backend: Backend,
    pub healthy: AtomicBool,
    pub fail_count: AtomicU64,
    /// 熔断状态：closed/open/half_open
    circuit_state: AtomicU8,
    /// 标记为 open 的时间戳（毫秒），用于冷却后进入 half-open 试探
    opened_since: AtomicU64,
    /// half-open 阶段是否已有探测请求在飞行中
    probe_in_flight: AtomicBool,
}

pub struct WeightedRoundRobin {
    backends: Vec<Arc<BackendState>>,
    retry: u32,
    retry_delay: Duration,
    // 加权随机用到的累积权重前缀和（缓存：只有健康集合变化时才重建）
    selector: RwLock<Vec<(usize, u32)>>, // (backend_index, cumulative_weight)
    // rebuild 节流：用“健康后端数量”做轻量级版本，避免无意义 rebuild 抢写锁
    healthy_count: AtomicUsize,
    name_index: HashMap<String, usize>, // name → backends Vec index，O(1) 健康查找
    recovery_cooldown: Duration,
}

impl WeightedRoundRobin {
    pub fn new(backends: Vec<Backend>, retry: u32, retry_delay: Duration) -> Self {
        Self::with_recovery_cooldown(
            backends,
            retry,
            retry_delay,
            Duration::from_millis(DEFAULT_RECOVERY_COOLDOWN_MS),
        )
    }

    pub fn with_recovery_cooldown(
        backends: Vec<Backend>,
        retry: u32,
        retry_delay: Duration,
        recovery_cooldown: Duration,
    ) -> Self {
        let states: Vec<Arc<BackendState>> = backends
            .into_iter()
            .map(|b| {
                Arc::new(BackendState {
                    backend: b,
                    healthy: AtomicBool::new(true),
                    fail_count: AtomicU64::new(0),
                    circuit_state: AtomicU8::new(CircuitState::Closed as u8),
                    opened_since: AtomicU64::new(0),
                    probe_in_flight: AtomicBool::new(false),
                })
            })
            .collect();
        let name_index: HashMap<String, usize> = states
            .iter()
            .enumerate()
            .map(|(i, s)| (s.backend.name.clone(), i))
            .collect();
        let selector = Self::build_selector(&states);
        let healthy_count = selector.len();
        Self {
            backends: states,
            retry,
            retry_delay,
            selector: RwLock::new(selector),
            name_index,
            healthy_count: AtomicUsize::new(healthy_count),
            recovery_cooldown,
        }
    }

    fn build_selector(backends: &[Arc<BackendState>]) -> Vec<(usize, u32)> {
        let mut sel = Vec::new();
        let mut cum = 0u32;
        for (i, s) in backends.iter().enumerate() {
            if s.healthy.load(Ordering::Relaxed) {
                cum += s.backend.weight;
                sel.push((i, cum));
            }
        }
        sel
    }

    fn rebuild_selector_if_needed(&self) {
        let current_healthy = self
            .backends
            .iter()
            .filter(|s| s.healthy.load(Ordering::Relaxed))
            .count();
        let last_healthy = self.healthy_count.load(Ordering::Relaxed);
        if current_healthy == last_healthy {
            return;
        }
        if self
            .healthy_count
            .compare_exchange(
                last_healthy,
                current_healthy,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }
        let new_sel = Self::build_selector(&self.backends);
        let mut sel = self.selector.write();
        *sel = new_sel;
    }

    /// 加权随机选择健康后端
    pub fn select(&self) -> Result<Arc<BackendState>> {
        let sel = self.selector.read();
        if sel.is_empty() {
            return Err(anyhow!("无可用后端"));
        }
        let total = sel.last().map(|(_, w)| *w).unwrap_or(0);
        let r = rand::random::<u64>() % total as u64;
        let pos = sel.partition_point(|(_, cum)| *cum as u64 <= r);
        let idx = if pos < sel.len() {
            sel[pos].0
        } else {
            sel[0].0
        };
        Ok(self.backends[idx].clone())
    }

    /// 带重试的选择
    pub fn select_with_retry(&self) -> Result<Arc<BackendState>> {
        let mut last_err = None;
        for _ in 0..self.retry {
            match self.select() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    tracing::warn!("选择后端失败: {e}, 等待重试...");
                    last_err = Some(e);
                    std::thread::sleep(self.retry_delay);
                    self.rebuild_selector_if_needed();
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("无可用后端")))
    }

    pub fn mark_healthy(&self, state: &BackendState) {
        let was_unhealthy = !state.healthy.load(Ordering::Relaxed);
        state.fail_count.store(0, Ordering::Relaxed);
        state.healthy.store(true, Ordering::Relaxed);
        state
            .circuit_state
            .store(CircuitState::Closed as u8, Ordering::Relaxed);
        state.opened_since.store(0, Ordering::Relaxed);
        state.probe_in_flight.store(false, Ordering::Relaxed);
        if was_unhealthy {
            tracing::info!("后端恢复: {}", state.backend.name);
            self.rebuild_selector_if_needed();
        }
    }

    pub fn mark_unhealthy(&self, state: &BackendState) {
        let previous_state = CircuitState::from_u8(state.circuit_state.load(Ordering::Relaxed));
        let fails = if previous_state == CircuitState::HalfOpen {
            FAILURE_THRESHOLD
        } else {
            state.fail_count.fetch_add(1, Ordering::Relaxed) + 1
        };
        tracing::warn!(
            "后端失败 ({fails}/{FAILURE_THRESHOLD}, state={}): {}",
            previous_state.as_str(),
            state.backend.name
        );
        if fails >= FAILURE_THRESHOLD {
            state.fail_count.store(fails, Ordering::Relaxed);
            state.healthy.store(false, Ordering::Relaxed);
            state
                .circuit_state
                .store(CircuitState::Open as u8, Ordering::Relaxed);
            state.opened_since.store(now_ms(), Ordering::Relaxed);
            state.probe_in_flight.store(false, Ordering::Relaxed);
            tracing::error!("后端熔断打开: {}", state.backend.name);
            self.rebuild_selector_if_needed();
        }
    }

    pub fn mark_healthy_by_name(&self, backend_name: &str) {
        if let Some(&idx) = self.name_index.get(backend_name) {
            let state = &self.backends[idx];
            self.mark_healthy(state);
        }
    }

    pub fn mark_unhealthy_by_name(&self, backend_name: &str) {
        if let Some(&idx) = self.name_index.get(backend_name) {
            let state = &self.backends[idx];
            self.mark_unhealthy(state);
        }
    }

    pub fn all_backends(&self) -> Vec<Arc<BackendState>> {
        self.backends.clone()
    }

    /// O(1) 按名称查询后端健康状态
    pub fn is_healthy_by_name(&self, name: &str) -> bool {
        self.name_index.get(name).is_some_and(|&idx| {
            let state = &self.backends[idx];
            if state.healthy.load(Ordering::Relaxed) {
                return true;
            }
            self.maybe_allow_probe(state)
        })
    }

    /// 返回指定 backend 的健康状态（会触发被动恢复）。
    ///
    /// 该方法是 balancer 与 proxy 编排之间的最小耦合点：
    /// - proxy 只关心“是否可以尝试该 backend”
    /// - 被动恢复的时序与状态转移由 balancer 负责
    pub fn is_healthy(&self, backend_name: &str) -> bool {
        self.is_healthy_by_name(backend_name)
    }

    fn is_ready_without_probe(&self, state: &BackendState) -> bool {
        match CircuitState::from_u8(state.circuit_state.load(Ordering::Relaxed)) {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => false,
            CircuitState::Open => {
                let opened_since = state.opened_since.load(Ordering::Relaxed);
                if opened_since == 0 {
                    return false;
                }
                let cooldown_ms = self.recovery_cooldown.as_millis() as u64;
                now_ms().saturating_sub(opened_since) >= cooldown_ms
            }
        }
    }

    fn maybe_allow_probe(&self, state: &BackendState) -> bool {
        if CircuitState::from_u8(state.circuit_state.load(Ordering::Relaxed)) != CircuitState::Open {
            return false;
        }
        let opened_since = state.opened_since.load(Ordering::Relaxed);
        if opened_since == 0 {
            return false;
        }
        let cooldown_ms = self.recovery_cooldown.as_millis() as u64;
        if now_ms().saturating_sub(opened_since) < cooldown_ms {
            return false;
        }
        if state
            .circuit_state
            .compare_exchange(
                CircuitState::Open as u8,
                CircuitState::HalfOpen as u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            if state
                .probe_in_flight
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                tracing::info!(
                    "后端熔断进入 half-open 试探: {} (cooldown={}ms)",
                    state.backend.name,
                    cooldown_ms
                );
                state.fail_count.store(0, Ordering::Relaxed);
                return true;
            }
            return false;
        }
        false
    }

    pub fn recovery_cooldown(&self) -> Duration {
        self.recovery_cooldown
    }

    pub fn backend_statuses(&self) -> Vec<BackendStatus> {
        self.backends
            .iter()
            .map(|state| BackendStatus {
                name: state.backend.name.clone(),
                url: state.backend.url.clone(),
                healthy: state.healthy.load(Ordering::Relaxed),
                ready: self.is_ready_without_probe(state),
                fail_count: state.fail_count.load(Ordering::Relaxed),
                circuit_state: CircuitState::from_u8(
                    state.circuit_state.load(Ordering::Relaxed),
                ),
                opened_since_ms: state.opened_since.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// 启动后台健康检查（已禁用 - 不同 provider API 格式不统一）
    pub fn start_health_check(self: &Arc<Self>, _interval: Duration) {
        tracing::info!("健康检查已禁用（所有后端保持健康状态）");
        // 不启动后台健康检查，所有后端始终保持健康
        // 失败由请求时的快速失败机制处理
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn backend(name: &str, weight: u32) -> Backend {
        Backend {
            name: name.to_string(),
            url: format!("https://{name}.example.com"),
            api_key: "key".to_string(),
            weight,
            models: vec!["glm-5.1".to_string()],
            timeout_secs: 30,
            connect_timeout_secs: 5,
            model_mappings: HashMap::new(),
            protocol: "openai".to_string(),
            auth_header: "Bearer key".to_string(),
            strip_params: vec![],
        }
    }

    #[test]
    fn circuit_opens_after_threshold_and_status_reports_not_ready_before_cooldown() {
        let balancer = WeightedRoundRobin::with_recovery_cooldown(
            vec![backend("a", 1)],
            1,
            Duration::from_millis(0),
            Duration::from_secs(60),
        );
        let state = balancer.select().unwrap();

        balancer.mark_unhealthy(&state);
        balancer.mark_unhealthy(&state);
        assert!(state.healthy.load(Ordering::Relaxed));

        balancer.mark_unhealthy(&state);
        let status = balancer.backend_statuses().remove(0);
        assert!(!status.healthy);
        assert!(!status.ready);
        assert_eq!(status.fail_count, FAILURE_THRESHOLD);
        assert_eq!(status.circuit_state, CircuitState::Open);
        assert!(status.opened_since_ms > 0);
        assert!(balancer.select().is_err());
    }

    #[test]
    fn half_open_probe_is_single_use_and_mark_healthy_closes_circuit() {
        let balancer = WeightedRoundRobin::with_recovery_cooldown(
            vec![backend("a", 1)],
            1,
            Duration::from_millis(0),
            Duration::from_millis(0),
        );
        let state = balancer.select().unwrap();
        for _ in 0..FAILURE_THRESHOLD {
            balancer.mark_unhealthy(&state);
        }

        assert!(balancer.is_healthy_by_name("a"));
        assert!(!balancer.is_healthy_by_name("a"));
        let status = balancer.backend_statuses().remove(0);
        assert_eq!(status.circuit_state, CircuitState::HalfOpen);
        assert!(!status.ready);

        balancer.mark_healthy_by_name("a");
        let status = balancer.backend_statuses().remove(0);
        assert!(status.healthy);
        assert!(status.ready);
        assert_eq!(status.fail_count, 0);
        assert_eq!(status.circuit_state, CircuitState::Closed);
        assert_eq!(status.opened_since_ms, 0);
    }

    #[test]
    fn half_open_failure_reopens_circuit_and_unknown_backend_is_unhealthy() {
        let balancer = WeightedRoundRobin::with_recovery_cooldown(
            vec![backend("a", 1)],
            1,
            Duration::from_millis(0),
            Duration::from_millis(0),
        );
        let state = balancer.select().unwrap();
        for _ in 0..FAILURE_THRESHOLD {
            balancer.mark_unhealthy(&state);
        }
        assert!(balancer.is_healthy("a"));
        balancer.mark_unhealthy_by_name("a");
        let status = balancer.backend_statuses().remove(0);
        assert!(!status.healthy);
        assert_eq!(status.circuit_state, CircuitState::Open);
        assert!(!balancer.is_healthy_by_name("missing"));
    }

    #[test]
    fn select_with_retry_reports_error_when_no_backends_exist() {
        let balancer = WeightedRoundRobin::new(vec![], 2, Duration::from_millis(0));
        assert!(balancer.select_with_retry().is_err());
        assert!(balancer.all_backends().is_empty());
        assert_eq!(balancer.recovery_cooldown(), Duration::from_millis(DEFAULT_RECOVERY_COOLDOWN_MS));
    }
}
