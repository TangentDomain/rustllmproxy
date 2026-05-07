use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};

/// Per-backend tok/s rolling window for adaptive load balancing.
/// Faster backends get more traffic automatically.
pub struct BackendMetrics {
    tok_per_sec: RwLock<HashMap<String, VecDeque<f64>>>,
    window: usize,
}

impl BackendMetrics {
    pub fn new(window: usize) -> Self {
        Self {
            tok_per_sec: RwLock::new(HashMap::new()),
            window,
        }
    }

    /// Record a tok/s sample for a backend.
    pub fn record(&self, backend: &str, tps: f64) {
        if tps <= 0.0 {
            return;
        }
        let mut map = self.tok_per_sec.write();
        let v = map.entry(backend.to_string()).or_default();
        v.push_back(tps);
        if v.len() > self.window {
            v.pop_front();
        }
    }

    /// Rolling average tok/s for a backend. Returns 1.0 if no data (equal default weight).
    pub fn avg(&self, backend: &str) -> f64 {
        let map = self.tok_per_sec.read();
        map.get(backend)
            .filter(|v| !v.is_empty())
            .map(|v| v.iter().sum::<f64>() / v.len() as f64)
            .unwrap_or(1.0)
    }
}
