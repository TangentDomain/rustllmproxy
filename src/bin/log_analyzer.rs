use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{BufRead, BufReader};
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};

#[derive(Debug, Default)]
struct RequestRecord {
    timestamp: DateTime<Utc>,
    model: String,
    path: String,
    protocol: Option<String>,
    body_size: usize,
    backend: Option<String>,
    ttfb: Option<u64>,
    ttft: Option<u64>,
    total: Option<u64>,
    tokens: Option<u32>,
    tok_per_sec: Option<f64>,
    stream: Option<bool>,
}

fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    // Format: "2026-04-20T04:01:47.623580Z" before the space
    let timestamp_end = s.find(' ')?;
    let naive = NaiveDateTime::parse_from_str(&s[..timestamp_end], "%Y-%m-%dT%H:%M:%S%.6fZ").ok()?;
    Some(DateTime::from_naive_utc_and_offset(naive, Utc))
}

fn extract_field(line: &str, key: &str) -> Option<String> {
    let pattern = format!("{}=", key);
    let start = line.find(&pattern)? + pattern.len();
    let remaining = &line[start..];
    let end = remaining.find(&[',', '}', '|', ')'][..]).unwrap_or(remaining.len());
    Some(remaining[..end].to_string())
}

fn extract_field_u64(line: &str, key: &str) -> Option<u64> {
    extract_field(line, key)?.replace("ms", "").parse().ok()
}

fn extract_field_usize(line: &str, key: &str) -> Option<usize> {
    extract_field(line, key)?.replace("B", "").parse().ok()
}

fn parse_log_line(line: &str) -> Option<RequestRecord> {
    let mut record = RequestRecord::default();

    // Parse timestamp
    let timestamp_end = line.find(" INFO ")?;
    record.timestamp = parse_timestamp(&line[..timestamp_end])?;

    // Parse request line
    if line.contains("Request model=") {
        record.model = extract_field(line, "model")?;
        record.path = extract_field(line, "path")?;
        record.protocol = extract_field(line, "protocol")
            .map(|p| p.trim_start_matches("Some(\"").trim_end_matches("\")").to_string());
        record.body_size = extract_field_usize(line, "body").unwrap_or(0);
    }

    // Parse responding line
    if line.contains("Responding:") {
        let model_part = line.split("Responding: ").nth(1)?;
        record.backend = model_part.split(" via ").nth(1).map(|s| s.split('|').next().unwrap_or(s).trim().to_string());
        record.ttfb = extract_field_u64(line, "ttfb");
        let stream_val = extract_field(line, "stream");
        record.stream = Some(stream_val.as_deref() == Some("true"));
    }

    // Parse done line
    if line.contains("Done:") {
        record.total = extract_field_u64(line, "total");
        record.ttfb = extract_field_u64(line, "ttfb").or(record.ttfb);
        record.ttft = extract_field_u64(line, "ttft");
        record.tokens = extract_field(line, "tokens").and_then(|s| s.parse().ok());
        record.tok_per_sec = extract_field(line, "tok/s").and_then(|s| s.parse().ok());
    }

    if record.model.is_empty() {
        None
    } else {
        Some(record)
    }
}

#[derive(Debug, Default)]
struct LogStats {
    total_requests: usize,
    total_tokens: u32,
    total_duration_ms: u64,
    total_ttfb_ms: u64,
    total_ttft_ms: u64,
    requests_by_model: HashMap<String, usize>,
    requests_by_backend: HashMap<String, usize>,
    requests_by_protocol: HashMap<String, usize>,
    requests_by_hour: BTreeMap<String, usize>,
    durations: Vec<u64>,
    ttfbs: Vec<u64>,
    ttfts: Vec<u64>,
    tokens_per_req: Vec<u32>,
    body_sizes: Vec<usize>,
    stream_count: usize,
    non_stream_count: usize,
}

impl LogStats {
    fn add_record(&mut self, record: &RequestRecord) {
        self.total_requests += 1;
        if let Some(tokens) = record.tokens {
            self.total_tokens += tokens;
            self.tokens_per_req.push(tokens);
        }
        if let Some(total) = record.total {
            self.total_duration_ms += total;
            self.durations.push(total);
        }
        if let Some(ttfb) = record.ttfb {
            self.total_ttfb_ms += ttfb;
            self.ttfbs.push(ttfb);
        }
        if let Some(ttft) = record.ttft {
            self.total_ttft_ms += ttft;
            self.ttfts.push(ttft);
        }
        self.body_sizes.push(record.body_size);

        *self.requests_by_model.entry(record.model.clone()).or_insert(0) += 1;
        if let Some(ref backend) = record.backend {
            *self.requests_by_backend.entry(backend.clone()).or_insert(0) += 1;
        }
        if let Some(ref protocol) = record.protocol {
            *self.requests_by_protocol.entry(protocol.clone()).or_insert(0) += 1;
        }

        let hour_key = format!("{:02}", record.timestamp.naive_utc().hour());
        *self.requests_by_hour.entry(hour_key).or_insert(0) += 1;

        match record.stream {
            Some(true) => self.stream_count += 1,
            Some(false) => self.non_stream_count += 1,
            None => {}
        }
    }

    fn percentile(vec: &mut [u64], p: f64) -> u64 {
        if vec.is_empty() { return 0; }
        vec.sort_unstable();
        let idx = (vec.len() as f64 * p / 100.0).floor() as usize;
        vec[idx.min(vec.len() - 1)]
    }

    fn print_summary(&self, log_file: &str) {
        println!("╔════════════════════════════════════════════════════════════════════════════╗");
        println!("║                    LLM Proxy 日志分析报告                                        ║");
        println!("╚════════════════════════════════════════════════════════════════════════════╝");
        println!();
        println!("📄 日志文件: {}", log_file);
        println!();

        // 总览
        println!("┌─ 概览 ─────────────────────────────────────────────────────────────────────┐");
        println!("│  总请求数: {:>12}                                                        │", self.total_requests);
        println!("│  总Token数: {:>12}                                                        │", self.total_tokens);
        println!("│  流式请求: {:>12}  ({:5.1}%)                                             │",
            self.stream_count, (self.stream_count as f64 / self.total_requests as f64 * 100.0));
        println!("│  非流式:   {:>12}  ({:5.1}%)                                             │",
            self.non_stream_count, (self.non_stream_count as f64 / self.total_requests as f64 * 100.0));
        println!("└────────────────────────────────────────────────────────────────────────────┘");
        println!();

        // 性能指标
        println!("┌─ 性能指标 ────────────────────────────────────────────────────────────────────┐");
        if !self.durations.is_empty() {
            let mut durs = self.durations.clone();
            let avg_dur = self.total_duration_ms / self.total_requests as u64;
            let p50 = Self::percentile(&mut durs, 50.0);
            let p90 = Self::percentile(&mut durs, 90.0);
            let p99 = Self::percentile(&mut durs, 99.0);

            println!("│  总延迟 (total):                                                             │");
            println!("│    平均: {:>8} ms   P50: {:>8} ms   P90: {:>8} ms   P99: {:>8} ms   │",
                avg_dur, p50, p90, p99);
        }

        if !self.ttfbs.is_empty() {
            let mut ttfb = self.ttfbs.clone();
            let avg_ttfb = self.total_ttfb_ms / self.ttfbs.len() as u64;
            let p50 = Self::percentile(&mut ttfb, 50.0);
            let p90 = Self::percentile(&mut ttfb, 90.0);
            let p99 = Self::percentile(&mut ttfb, 99.0);

            println!("│  首字节延迟 (ttfb):                                                          │");
            println!("│    平均: {:>8} ms   P50: {:>8} ms   P90: {:>8} ms   P99: {:>8} ms   │",
                avg_ttfb, p50, p90, p99);
        }

        if !self.ttfts.is_empty() {
            let mut ttft = self.ttfts.clone();
            let avg_ttft = self.total_ttft_ms / self.ttfts.len() as u64;
            let p50 = Self::percentile(&mut ttft, 50.0);
            let p90 = Self::percentile(&mut ttft, 90.0);
            let p99 = Self::percentile(&mut ttft, 99.0);

            println!("│  首Token延迟 (ttft):                                                         │");
            println!("│    平均: {:>8} ms   P50: {:>8} ms   P90: {:>8} ms   P99: {:>8} ms   │",
                avg_ttft, p50, p90, p99);
        }

        if !self.tokens_per_req.is_empty() {
            let avg_tokens = self.total_tokens as f64 / self.tokens_per_req.len() as f64;
            println!("│  Token/请求: 平均 {:.1}                                                    │", avg_tokens);
        }

        if !self.body_sizes.is_empty() {
            let mut sizes = self.body_sizes.clone();
            let avg_size = self.body_sizes.iter().sum::<usize>() / self.body_sizes.len();
            let p50 = Self::percentile(&mut sizes.iter().map(|&v| v as u64).collect::<Vec<_>>().as_mut(), 50.0);
            let p90 = Self::percentile(&mut sizes.iter().map(|&v| v as u64).collect::<Vec<_>>().as_mut(), 90.0);
            let p99 = Self::percentile(&mut sizes.iter().map(|&v| v as u64).collect::<Vec<_>>().as_mut(), 99.0);

            println!("│  Body大小 (bytes):                                                          │");
            println!("│    平均: {:>8} B    P50: {:>8} B    P90: {:>8} B    P99: {:>8} B    │",
                avg_size, p50, p90, p99);
        }
        println!("└────────────────────────────────────────────────────────────────────────────┘");
        println!();

        // 模型分布
        println!("┌─ 模型使用分布 ──────────────────────────────────────────────────────────────┐");
        let mut models: Vec<_> = self.requests_by_model.iter().collect();
        models.sort_by(|a, b| b.1.cmp(a.1));
        for (model, count) in models.iter().take(10) {
            let pct = (**count as f64 / self.total_requests as f64 * 100.0);
            println!("│  {:20} {:>8} ({:5.1}%)                                              │", model, count, pct);
        }
        println!("└────────────────────────────────────────────────────────────────────────────┘");
        println!();

        // 后端分布
        println!("┌─ 后端使用分布 ────────────────────────────────────────────────────────────────┐");
        let mut backends: Vec<_> = self.requests_by_backend.iter().collect();
        backends.sort_by(|a, b| b.1.cmp(a.1));
        for (backend, count) in backends.iter().take(10) {
            let pct = (**count as f64 / self.total_requests as f64 * 100.0);
            println!("│  {:30} {:>8} ({:5.1}%)                                     │", backend, count, pct);
        }
        println!("└────────────────────────────────────────────────────────────────────────────┘");
        println!();

        // 时间分布
        println!("┌─ 小时分布 ────────────────────────────────────────────────────────────────────┐");
        for (hour, count) in &self.requests_by_hour {
            let bar_len = (*count as f64 / self.total_requests as f64 * 50.0) as usize;
            let bar = "█".repeat(bar_len);
            println!("│  {}h {:>5} {}{:>52}│", hour, count, bar, "");
        }
        println!("└────────────────────────────────────────────────────────────────────────────┘");
    }
}

fn main() -> Result<()> {
    let log_path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: log_analyzer <log_file>");
        std::process::exit(1);
    });

    let file = File::open(&log_path)?;
    let reader = BufReader::new(file);

    let mut stats = LogStats::default();
    let mut current_record: Option<RequestRecord> = None;

    for line in reader.lines() {
        let line = line?;

        if line.contains("Request model=") {
            let mut record = RequestRecord::default();
            record.model = extract_field(&line, "model").unwrap_or_default();
            record.path = extract_field(&line, "path").unwrap_or_default();
            record.protocol = extract_field(&line, "protocol")
                .map(|p| p.trim_start_matches("Some(\"").trim_end_matches("\")").to_string());
            record.body_size = extract_field_usize(&line, "body").unwrap_or(0);
            current_record = Some(record);
        }

        if let Some(ref mut record) = current_record {
            if line.contains("Responding:") {
                let parts: Vec<&str> = line.split("Responding: ").nth(1).unwrap_or("")
                    .split(" via ").collect();
                if parts.len() >= 2 {
                    record.backend = Some(parts[1].split('|').next().unwrap_or(parts[1]).trim().to_string());
                }
                record.ttfb = extract_field_u64(&line, "ttfb").or(record.ttfb);
                let stream_val = extract_field(&line, "stream");
                record.stream = Some(stream_val.as_deref() == Some("true"));
            }

            if line.contains("Done:") {
                record.total = extract_field_u64(&line, "total");
                record.tokens = extract_field(&line, "tokens").and_then(|s| s.parse().ok());
                record.tok_per_sec = extract_field(&line, "tok/s").and_then(|s| s.parse().ok());

                stats.add_record(record);
                current_record = None;
            }
        }
    }

    stats.print_summary(&log_path);
    Ok(())
}
