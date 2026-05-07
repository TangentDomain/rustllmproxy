use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Single request metric sample
#[derive(Serialize, Deserialize, Clone)]
struct Sample {
    time: String, // ISO8601
    tok_per_sec: f64,
    ttfb_ms: u64,
    ttft_ms: u64,
    total_ms: u64,
    tokens: u32,
}

/// Persistent per-model metrics file
#[derive(Serialize, Deserialize, Default)]
struct ModelFile {
    total_requests: u64,
    avg_tok_per_sec: f64,
    min_tok_per_sec: f64,
    max_tok_per_sec: f64,
    p50_tok_per_sec: f64,
    p95_tok_per_sec: f64,
    avg_ttfb_ms: f64,
    avg_ttft_ms: f64,
    avg_total_ms: f64,
    avg_tokens: f64,
    last_updated: String,
    recent: Vec<Sample>, // last 100 requests
}

impl ModelFile {
    fn compute(samples: &[Sample]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        let n = samples.len() as f64;
        let tps: Vec<f64> = samples.iter().map(|s| s.tok_per_sec).collect();
        let mut tps_sorted = tps.clone();
        tps_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let p50 = tps_sorted[(n * 0.50) as usize];
        let p95 = tps_sorted[((n * 0.95) as usize).min(samples.len() - 1)];

        Self {
            total_requests: samples.len() as u64,
            avg_tok_per_sec: tps.iter().sum::<f64>() / n,
            min_tok_per_sec: tps.iter().cloned().fold(f64::INFINITY, f64::min),
            max_tok_per_sec: tps.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            p50_tok_per_sec: p50,
            p95_tok_per_sec: p95,
            avg_ttfb_ms: samples.iter().map(|s| s.ttfb_ms as f64).sum::<f64>() / n,
            avg_ttft_ms: samples.iter().map(|s| s.ttft_ms as f64).sum::<f64>() / n,
            avg_total_ms: samples.iter().map(|s| s.total_ms as f64).sum::<f64>() / n,
            avg_tokens: samples.iter().map(|s| s.tokens as f64).sum::<f64>() / n,
            last_updated: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
            recent: samples.iter().rev().take(100).cloned().collect(),
        }
    }
}

/// Thread-safe metrics store. Key = (backend_name, model_name)
pub struct MetricsStore {
    dir: PathBuf,
    // (backend, model) → samples
    data: DashMap<(String, String), Vec<Sample>>,
    // model → (tok/s sum, sample count)，用于请求路径 O(1) 读取模型均值
    model_totals: DashMap<String, (f64, usize)>,
}

impl MetricsStore {
    pub fn new(base_dir: &str) -> Self {
        let dir = PathBuf::from(base_dir);
        fs::create_dir_all(&dir).ok();
        Self {
            dir,
            data: DashMap::new(),
            model_totals: DashMap::new(),
        }
    }

    /// Record a completed request
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        backend: &str,
        model: &str,
        tok_per_sec: f64,
        ttfb_ms: u64,
        ttft_ms: u64,
        total_ms: u64,
        tokens: u32,
    ) {
        if tok_per_sec <= 0.0 {
            return;
        }
        let sample = Sample {
            time: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
            tok_per_sec,
            ttfb_ms,
            ttft_ms,
            total_ms,
            tokens,
        };
        let key = (backend.to_string(), model.to_string());
        let mut samples = self.data.entry(key).or_default();
        samples.push(sample);
        let mut removed_sum = 0.0;
        let mut removed_count = 0usize;
        if samples.len() > 1000 {
            let drain_from = samples.len() - 1000;
            for removed in samples.drain(0..drain_from) {
                removed_sum += removed.tok_per_sec;
                removed_count += 1;
            }
        }
        self.update_model_total(model, tok_per_sec, 1, removed_sum, removed_count);
    }

    /// Average tok/s for a model aggregated across all backends.
    /// Returns 1.0 when there is no data so callers can use equal default weights.
    pub fn avg_for_model(&self, model: &str) -> f64 {
        self.model_totals
            .get(model)
            .and_then(|total| {
                let (sum, count) = *total;
                if count == 0 {
                    None
                } else {
                    Some(sum / count as f64)
                }
            })
            .unwrap_or(1.0)
    }

    /// Flush all metrics to disk (call periodically)
    pub fn flush(&self) {
        for entry in self.data.iter() {
            let ((backend, model), samples) = entry.pair();
            if samples.is_empty() {
                continue;
            }
            let model_file = ModelFile::compute(samples);
            let backend_dir = self.dir.join(sanitize_filename(backend));
            fs::create_dir_all(&backend_dir).ok();
            let file_path = backend_dir.join(format!("{}.json", sanitize_filename(model)));
            if let Ok(json) = serde_json::to_string_pretty(&model_file) {
                fs::write(&file_path, json).ok();
            }
        }
    }

    /// 清理长时间无新数据的 entry，防止 DashMap 键空间无限增长。
    /// 保留最近 max_age_secs 秒内有数据的 entry，删除其余的。
    pub fn evict_stale(&self, max_age_secs: u64) {
        let cutoff = chrono::Local::now() - chrono::Duration::seconds(max_age_secs as i64);
        let cutoff_str = cutoff.format("%Y-%m-%dT%H:%M:%S").to_string();
        self.data.retain(|key, samples| {
            // 保留最后一个 sample 时间在 cutoff 之后的 entry
            let keep = samples.last().map(|s| s.time > cutoff_str).unwrap_or(false);
            if keep {
                return true;
            }
            let removed_sum = samples.iter().map(|s| s.tok_per_sec).sum::<f64>();
            let removed_count = samples.len();
            self.update_model_total(&key.1, 0.0, 0, removed_sum, removed_count);
            false
        });
    }

    fn update_model_total(
        &self,
        model: &str,
        added_sum: f64,
        added_count: usize,
        removed_sum: f64,
        removed_count: usize,
    ) {
        if added_count == 0 && removed_count == 0 {
            return;
        }
        let mut total = self
            .model_totals
            .entry(model.to_string())
            .or_insert((0.0, 0));
        total.0 += added_sum;
        total.1 += added_count;
        total.0 -= removed_sum;
        total.1 = total.1.saturating_sub(removed_count);
        if total.1 == 0 {
            drop(total);
            self.model_totals.remove(model);
        }
    }
}
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avg_for_model_uses_incremental_totals_across_backends() {
        let store = MetricsStore::new(
            "target/test-metrics/avg_for_model_uses_incremental_totals_across_backends",
        );

        assert_eq!(store.avg_for_model("glm-5.1"), 1.0);

        store.record("backend-a", "glm-5.1", 10.0, 1, 1, 1, 1);
        store.record("backend-b", "glm-5.1", 20.0, 1, 1, 1, 1);
        store.record("backend-a", "glm-4.7", 100.0, 1, 1, 1, 1);

        assert_eq!(store.avg_for_model("glm-5.1"), 15.0);
        assert_eq!(store.avg_for_model("glm-4.7"), 100.0);
        assert_eq!(store.avg_for_model("unknown"), 1.0);
    }

    #[test]
    fn record_keeps_model_totals_in_sync_with_window_eviction() {
        let store = MetricsStore::new(
            "target/test-metrics/record_keeps_model_totals_in_sync_with_window_eviction",
        );

        for _ in 0..1000 {
            store.record("backend-a", "glm-5.1", 10.0, 1, 1, 1, 1);
        }
        store.record("backend-a", "glm-5.1", 20.0, 1, 1, 1, 1);

        let expected = (999.0 * 10.0 + 20.0) / 1000.0;
        assert!((store.avg_for_model("glm-5.1") - expected).abs() < f64::EPSILON);
    }

    #[test]
    fn evict_stale_updates_incremental_totals() {
        let store = MetricsStore::new("target/test-metrics/evict_stale_updates_incremental_totals");

        store.record("backend-a", "glm-5.1", 10.0, 1, 1, 1, 1);
        store.record("backend-b", "glm-5.1", 20.0, 1, 1, 1, 1);
        assert_eq!(store.avg_for_model("glm-5.1"), 15.0);

        store.evict_stale(0);

        assert_eq!(store.avg_for_model("glm-5.1"), 1.0);
    }
    #[test]
    fn concurrent_record_keeps_model_totals_consistent() {
        let store = std::sync::Arc::new(MetricsStore::new(
            "target/test-metrics/concurrent_record_keeps_model_totals_consistent",
        ));
        let mut handles = Vec::new();

        for backend_idx in 0..8 {
            let store = store.clone();
            handles.push(std::thread::spawn(move || {
                let backend = format!("backend-{backend_idx}");
                for _ in 0..200 {
                    store.record(&backend, "glm-5.1", 16.0, 1, 1, 1, 1);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("record thread panicked");
        }

        assert!((store.avg_for_model("glm-5.1") - 16.0).abs() < f64::EPSILON);
    }
}
