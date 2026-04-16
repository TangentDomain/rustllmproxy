use crate::config::Backend;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use anyhow::{anyhow, Result};

#[derive(Debug)]
pub struct BackendState {
    pub backend: Backend,
    pub healthy: AtomicBool,
    pub fail_count: AtomicU64,
}

pub struct WeightedRoundRobin {
    backends: Vec<Arc<BackendState>>,
    retry: u32,
    retry_delay: Duration,
    // 加权随机用到的累积权重前缀和（缓存在 RwLock 里，unhealthy 变化时重建）
    selector: RwLock<Vec<(usize, u32)>>, // (backend_index, cumulative_weight)
    rand_counter: AtomicU64,
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
                })
            })
            .collect();
        let selector = Self::build_selector(&states);
        Self {
            backends: states,
            retry,
            retry_delay,
            selector: RwLock::new(selector),
            rand_counter: AtomicU64::new(0),
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

    fn rebuild_selector(&self) {
        let mut sel = self.selector.write().unwrap();
        *sel = Self::build_selector(&self.backends);
    }

    /// 加权随机选择健康后端
    pub fn select(&self) -> Result<Arc<BackendState>> {
        let sel = self.selector.read().unwrap();
        if sel.is_empty() {
            return Err(anyhow!("无可用后端"));
        }
        let total = sel.last().map(|(_, w)| *w).unwrap_or(0);
        let r = self.next_random() % total as u64;

        let pos = sel.partition_point(|(_, cum)| *cum as u64 <= r);
        let idx = if pos < sel.len() { sel[pos].0 } else { sel[0].0 };
        Ok(self.backends[idx].clone())
    }

    /// 返回一个伪随机数（用于外部加权选择）
    pub fn next_random(&self) -> u64 {
        self.rand_counter.fetch_add(1, Ordering::Relaxed)
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
                    self.rebuild_selector();
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("无可用后端")))
    }

    pub fn mark_healthy(&self, state: &BackendState) {
        let was_unhealthy = !state.healthy.load(Ordering::Relaxed);
        state.fail_count.store(0, Ordering::Relaxed);
        state.healthy.store(true, Ordering::Relaxed);
        if was_unhealthy {
            tracing::info!("后端恢复: {}", state.backend.name);
            self.rebuild_selector();
        }
    }

    pub fn mark_unhealthy(&self, state: &BackendState) {
        let fails = state.fail_count.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::warn!("后端失败 ({fails}/3): {}", state.backend.name);
        if fails >= 3 {
            state.healthy.store(false, Ordering::Relaxed);
            tracing::error!("后端标记不健康: {}", state.backend.name);
            self.rebuild_selector();
        }
    }

    pub fn all_backends(&self) -> Vec<Arc<BackendState>> {
        self.backends.clone()
    }

    /// 启动后台健康检查（已禁用 - 不同 provider API 格式不统一）
    pub fn start_health_check(self: &Arc<Self>, _interval: Duration) {
        tracing::info!("健康检查已禁用（所有后端保持健康状态）");
        // 不启动后台健康检查，所有后端始终保持健康
        // 失败由请求时的快速失败机制处理
    }
}
