use crate::config::Backend;
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Unhealthy 后端被动恢复冷却时间（毫秒）
const RECOVERY_COOLDOWN_MS: u64 = 60_000;

#[derive(Debug)]
pub struct BackendState {
    pub backend: Backend,
    pub healthy: AtomicBool,
    pub fail_count: AtomicU64,
    /// 标记为 unhealthy 的时间戳（毫秒），用于被动恢复
    unhealthy_since: AtomicU64,
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
}

impl WeightedRoundRobin {
    pub fn new(backends: Vec<Backend>, retry: u32, retry_delay: Duration) -> Self {
        let states: Vec<Arc<BackendState>> = backends
            .into_iter()
            .map(|b| {
                Arc::new(BackendState {
                    backend: b,
                    healthy: AtomicBool::new(true),
                    fail_count: AtomicU64::new(0),
                    unhealthy_since: AtomicU64::new(0),
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
        // 只有当健康集合规模变化时才需要重建 selector。
        // 这能显著降低 select_with_retry/is_healthy_by_name 在高并发下触发的写锁争用。
        let current_healthy = self
            .backends
            .iter()
            .filter(|s| s.healthy.load(Ordering::Relaxed))
            .count();
        let last_healthy = self.healthy_count.load(Ordering::Relaxed);
        if current_healthy == last_healthy {
            return;
        }
        // CAS 成功者负责重建，其他并发调用者直接返回，避免 rebuild storm。
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
        state.unhealthy_since.store(0, Ordering::Relaxed);
        if was_unhealthy {
            tracing::info!("后端恢复: {}", state.backend.name);
            self.rebuild_selector_if_needed();
        }
    }

    pub fn mark_unhealthy(&self, state: &BackendState) {
        let fails = state.fail_count.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::warn!("后端失败 ({fails}/3): {}", state.backend.name);
        if fails >= 3 {
            state.healthy.store(false, Ordering::Relaxed);
            state.unhealthy_since.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                Ordering::Relaxed,
            );
            tracing::error!("后端标记不健康: {}", state.backend.name);
            self.rebuild_selector_if_needed();
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
            // 被动恢复：unhealthy 超过 60s 后自动恢复，允许试探
            let since = state.unhealthy_since.load(Ordering::Relaxed);
            if since == 0 {
                return false;
            }
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if now_ms.saturating_sub(since) > RECOVERY_COOLDOWN_MS {
                tracing::info!("后端被动恢复（超时 60s）: {}", state.backend.name);
                state.fail_count.store(0, Ordering::Relaxed);
                state.healthy.store(true, Ordering::Relaxed);
                state.unhealthy_since.store(0, Ordering::Relaxed);
                self.rebuild_selector_if_needed();
                true
            } else {
                false
            }
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

    /// 启动后台健康检查（已禁用 - 不同 provider API 格式不统一）
    pub fn start_health_check(self: &Arc<Self>, _interval: Duration) {
        tracing::info!("健康检查已禁用（所有后端保持健康状态）");
        // 不启动后台健康检查，所有后端始终保持健康
        // 失败由请求时的快速失败机制处理
    }
}
