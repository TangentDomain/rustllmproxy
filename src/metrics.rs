use dashmap::DashMap;
use std::fs;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Single request metric sample
#[derive(Serialize, Deserialize, Clone)]
struct Sample {
    time: String,       // ISO8601
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
    recent: Vec<Sample>,  // last 100 requests
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
}

impl MetricsStore {
    pub fn new(base_dir: &str) -> Self {
        let dir = PathBuf::from(base_dir);
        fs::create_dir_all(&dir).ok();
        Self { dir, data: DashMap::new() }
    }

    /// Record a completed request
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
        if tok_per_sec <= 0.0 { return; }
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
        // Keep last 1000 in memory
        if samples.len() > 1000 {
            let drain_from = samples.len() - 1000;
            samples.drain(0..drain_from);
        }
    }

    /// Average tok/s for a model aggregated across all backends.
    /// Returns 1.0 when there is no data so callers can use equal default weights.
    pub fn avg_for_model(&self, model: &str) -> f64 {
        let mut sum = 0.0;
        let mut count = 0usize;
        for entry in self.data.iter() {
            let ((_, sample_model), samples) = entry.pair();
            if sample_model != model || samples.is_empty() {
                continue;
            }
            sum += samples.iter().map(|s| s.tok_per_sec).sum::<f64>();
            count += samples.len();
        }
        if count == 0 { 1.0 } else { sum / count as f64 }
    }

    /// Flush all metrics to disk (call periodically)
    pub fn flush(&self) {
        for entry in self.data.iter() {
            let ((backend, model), samples) = entry.pair();
            if samples.is_empty() { continue; }
            let model_file = ModelFile::compute(samples);
            let backend_dir = self.dir.join(sanitize_filename(backend));
            fs::create_dir_all(&backend_dir).ok();
            let file_path = backend_dir.join(format!("{}.json", sanitize_filename(model)));
            if let Ok(json) = serde_json::to_string_pretty(&model_file) {
                fs::write(&file_path, json).ok();
            }
        }
    }
}

fn sanitize_filename(s: &str) -> String {
    s.chars().map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect()
}
