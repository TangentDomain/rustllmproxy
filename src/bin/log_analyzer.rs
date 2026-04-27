use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};

#[derive(Debug, Default, Clone)]
struct RequestRecord {
    timestamp: DateTime<Utc>,
    requested_model: String,
    resolved_model: String,
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
    error: Option<String>,
}

fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
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

#[derive(Debug, Default)]
struct ModelStats {
    count: usize,
    fallback_count: usize,
    total_tokens: u32,
    total_duration_ms: u64,
    total_ttfb_ms: u64,
    total_ttft_ms: u64,
    durations: Vec<u64>,
    ttfbs: Vec<u64>,
    ttfts: Vec<u64>,
    tok_per_secs: Vec<f64>,
    error_count: usize,
}

impl ModelStats {
    fn percentile_u64(vec: &mut [u64], p: f64) -> u64 {
        if vec.is_empty() { return 0; }
        vec.sort_unstable();
        let idx = (vec.len() as f64 * p / 100.0).floor() as usize;
        vec[idx.min(vec.len() - 1)]
    }

    fn percentile_f64(vec: &mut [f64], p: f64) -> f64 {
        if vec.is_empty() { return 0.0; }
        let mut v = vec.to_vec();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (v.len() as f64 * p / 100.0).floor() as usize;
        v[idx.min(v.len() - 1)]
    }
}

#[derive(Debug, Default)]
struct LogStats {
    total_requests: usize,
    total_tokens: u32,
    fallback_count: usize,
    error_count: usize,
    stream_count: usize,
    non_stream_count: usize,
    requests_by_requested_model: HashMap<String, usize>,
    requests_by_resolved_model: HashMap<String, usize>,
    fallback_chains: HashMap<String, usize>,
    model_stats: HashMap<String, ModelStats>,
    requests_by_hour: BTreeMap<String, usize>,
    errors_by_type: HashMap<String, usize>,
    backend_stats: HashMap<String, usize>,
    all_durations: Vec<u64>,
    all_ttfbs: Vec<u64>,
    all_ttfts: Vec<u64>,
}

fn percentile_u64(vec: &mut [u64], p: f64) -> u64 {
    if vec.is_empty() { return 0; }
    vec.sort_unstable();
    let idx = (vec.len() as f64 * p / 100.0).floor() as usize;
    vec[idx.min(vec.len() - 1)]
}


fn main() -> Result<()> {
    let log_path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: log_analyzer <log_file>");
        std::process::exit(1);
    });

    let output_path = format!("{}.md", log_path);
    let mut stats = LogStats::default();
    let mut current_record: Option<RequestRecord> = None;

    let file = File::open(&log_path)?;
    for line in BufReader::new(file).lines() {
        let line = line?;

        if line.contains("Request model=") {
            let mut record = RequestRecord::default();
            record.requested_model = extract_field(&line, "model").unwrap_or_default();
            record.resolved_model = record.requested_model.clone();
            record.path = extract_field(&line, "path").unwrap_or_default();
            record.protocol = extract_field(&line, "protocol")
                .map(|p| p.trim_start_matches("Some(\"").trim_end_matches("\")").to_string());
            record.body_size = extract_field_usize(&line, "body").unwrap_or(0);
            record.timestamp = parse_timestamp(&line).unwrap_or_else(Utc::now);
            current_record = Some(record);
        }

        if let Some(ref mut record) = current_record {
            if line.contains("Responding:") {
                let responding_part = line.split("Responding: ").nth(1).unwrap_or("");
                let model_part = responding_part.split(" via ").next().unwrap_or("");
                let parts: Vec<&str> = model_part.split(" -> ").collect();
                if parts.len() >= 2 {
                    record.resolved_model = parts[1].trim().to_string();
                }
                record.backend = line.split(" via ").nth(1)
                    .map(|s| s.split('|').next().unwrap_or(s).trim().to_string());
                record.ttfb = extract_field_u64(&line, "ttfb");
                let stream_val = extract_field(&line, "stream");
                record.stream = Some(stream_val.as_deref() == Some("true"));
            }

            if line.contains("Done:") {
                record.total = extract_field_u64(&line, "total");
                record.tokens = extract_field(&line, "tokens").and_then(|s| s.parse().ok());
                record.tok_per_sec = extract_field(&line, "tok/s").and_then(|s| s.parse().ok());

                if line.contains("ERROR") {
                    if line.contains("exhausted") {
                        record.error = Some("all_backends_exhausted".to_string());
                    } else if line.contains("timeout") {
                        record.error = Some("timeout".to_string());
                    } else {
                        record.error = Some("error".to_string());
                    }
                }

                // 统计
                let is_fallback = record.requested_model != record.resolved_model;
                if is_fallback {
                    stats.fallback_count += 1;
                    let chain = format!("{} -> {}", record.requested_model, record.resolved_model);
                    *stats.fallback_chains.entry(chain).or_insert(0) += 1;
                }

                stats.total_requests += 1;
                *stats.requests_by_requested_model.entry(record.requested_model.clone()).or_insert(0) += 1;
                *stats.requests_by_resolved_model.entry(record.resolved_model.clone()).or_insert(0) += 1;

                if let Some(ref backend) = record.backend {
                    *stats.backend_stats.entry(backend.clone()).or_insert(0) += 1;
                }

                if let Some(tokens) = record.tokens {
                    stats.total_tokens += tokens;
                }

                if let Some(total) = record.total {
                    stats.all_durations.push(total);
                }
                if let Some(ttfb) = record.ttfb {
                    stats.all_ttfbs.push(ttfb);
                }
                if let Some(ttft) = record.ttft {
                    stats.all_ttfts.push(ttft);
                }

                if let Some(ref error) = record.error {
                    stats.error_count += 1;
                    *stats.errors_by_type.entry(error.clone()).or_insert(0) += 1;
                }

                match record.stream {
                    Some(true) => stats.stream_count += 1,
                    Some(false) => stats.non_stream_count += 1,
                    None => {}
                }

                // 按模型统计
                let model_key = record.requested_model.clone();
                let m = stats.model_stats.entry(model_key).or_insert_with(ModelStats::default);
                m.count += 1;
                if is_fallback { m.fallback_count += 1; }
                if let Some(tokens) = record.tokens { m.total_tokens += tokens; }
                if let Some(total) = record.total {
                    m.total_duration_ms += total;
                    m.durations.push(total);
                }
                if let Some(ttfb) = record.ttfb {
                    m.total_ttfb_ms += ttfb;
                    m.ttfbs.push(ttfb);
                }
                if let Some(ttft) = record.ttft {
                    m.total_ttft_ms += ttft;
                    m.ttfts.push(ttft);
                }
                if let Some(tps) = record.tok_per_sec {
                    m.tok_per_secs.push(tps);
                }
                if record.error.is_some() { m.error_count += 1; }

                // 按小时
                let hour = format!("{:02}", record.timestamp.hour());
                *stats.requests_by_hour.entry(hour).or_insert(0) += 1;

                current_record = None;
            }
        }
    }

    // 写入 Markdown
    let mut out = File::create(&output_path)?;
    writeln!(out, "# LLM Proxy 日志分析报告").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**日志文件**: `{}`", log_path).unwrap();
    writeln!(out, "**生成时间**: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S")).unwrap();
    writeln!(out).unwrap();

    // 总览
    writeln!(out, "## 📊 概览").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| 指标 | 数值 | 占比 |").unwrap();
    writeln!(out, "|------|------|------|").unwrap();
    writeln!(out, "| 总请求数 | {} | 100% |", stats.total_requests).unwrap();
    writeln!(out, "| 总Token数 | {} | - |", stats.total_tokens).unwrap();
    writeln!(out, "| Fallback请求 | {} | {:.1}% |",
        stats.fallback_count, stats.fallback_count as f64 / stats.total_requests as f64 * 100.0).unwrap();
    writeln!(out, "| 错误请求 | {} | {:.1}% |",
        stats.error_count, stats.error_count as f64 / stats.total_requests as f64 * 100.0).unwrap();
    writeln!(out, "| 流式请求 | {} | {:.1}% |",
        stats.stream_count, stats.stream_count as f64 / stats.total_requests as f64 * 100.0).unwrap();
    writeln!(out, "| 非流式 | {} | {:.1}% |",
        stats.non_stream_count, stats.non_stream_count as f64 / stats.total_requests as f64 * 100.0).unwrap();
    writeln!(out).unwrap();

    // 总体性能
    writeln!(out, "## 📈 总体性能统计").unwrap();
    writeln!(out).unwrap();
    if !stats.all_durations.is_empty() {
        let mut d = stats.all_durations.clone();
        let avg = stats.all_durations.iter().sum::<u64>() / stats.all_durations.len() as u64;
        writeln!(out, "**总延迟 (total)** - 平均: {}ms, P50: {}ms, P90: {}ms, P99: {}ms",
            avg, percentile_u64(&mut d, 50.0), percentile_u64(&mut d, 90.0), percentile_u64(&mut d, 99.0)).unwrap();
    }
    if !stats.all_ttfbs.is_empty() {
        let mut t = stats.all_ttfbs.clone();
        let avg = stats.all_ttfbs.iter().sum::<u64>() / stats.all_ttfbs.len() as u64;
        writeln!(out, "**TTFB** - 平均: {}ms, P50: {}ms, P90: {}ms, P99: {}ms",
            avg, percentile_u64(&mut t, 50.0), percentile_u64(&mut t, 90.0), percentile_u64(&mut t, 99.0)).unwrap();
    }
    if !stats.all_ttfts.is_empty() {
        let mut t = stats.all_ttfts.clone();
        let avg = stats.all_ttfts.iter().sum::<u64>() / stats.all_ttfts.len() as u64;
        writeln!(out, "**TTFT** - 平均: {}ms, P50: {}ms, P90: {}ms, P99: {}ms",
            avg, percentile_u64(&mut t, 50.0), percentile_u64(&mut t, 90.0), percentile_u64(&mut t, 99.0)).unwrap();
    } else {
        writeln!(out, "**TTFT** - 无数据").unwrap();
    }
    writeln!(out).unwrap();

    // Fallback
    writeln!(out, "## 🔄 Fallback分析").unwrap();
    writeln!(out).unwrap();
    let mut chains: Vec<_> = stats.fallback_chains.iter().collect();
    chains.sort_by(|a, b| b.1.cmp(a.1));
    if chains.is_empty() {
        writeln!(out, "无Fallback记录").unwrap();
    } else {
        writeln!(out, "| 请求模型 -> 实际模型 | 次数 | 占比 |").unwrap();
        writeln!(out, "|---------------------|------|------|").unwrap();
        for (chain, count) in &chains {
            writeln!(out, "| {} | {} | {:.1}% |", chain, count,
                **count as f64 / stats.total_requests as f64 * 100.0).unwrap();
        }
    }
    writeln!(out).unwrap();

    // 按模型
    writeln!(out, "## 📊 按模型分组统计").unwrap();
    writeln!(out).unwrap();
    let mut models: Vec<_> = stats.model_stats.iter().collect();
    models.sort_by(|a, b| b.1.count.cmp(&a.1.count));

    for (model, m) in &models {
        writeln!(out, "### {}", model).unwrap();
        writeln!(out).unwrap();
        writeln!(out, "| 指标 | 数值 |").unwrap();
        writeln!(out, "|------|------|").unwrap();
        writeln!(out, "| 请求数 | {} |", m.count).unwrap();
        writeln!(out, "| Fallback数 | {} |", m.fallback_count).unwrap();
        writeln!(out, "| 错误数 | {} |", m.error_count).unwrap();

        if !m.durations.is_empty() {
            let mut d = m.durations.clone();
            let avg = m.total_duration_ms / m.durations.len() as u64;
            writeln!(out, "| 总延迟(平均) | {}ms |", avg).unwrap();
            writeln!(out, "| 总延迟 P50/P90/P99 | {}ms / {}ms / {}ms |",
                ModelStats::percentile_u64(&mut d.clone(), 50.0),
                ModelStats::percentile_u64(&mut d.clone(), 90.0),
                ModelStats::percentile_u64(&mut d, 99.0)).unwrap();
        }

        if !m.ttfbs.is_empty() {
            let mut t = m.ttfbs.clone();
            let avg = m.total_ttfb_ms / m.ttfbs.len() as u64;
            writeln!(out, "| TTFB(平均) | {}ms |", avg).unwrap();
            writeln!(out, "| TTFB P50/P90/P99 | {}ms / {}ms / {}ms |",
                ModelStats::percentile_u64(&mut t.clone(), 50.0),
                ModelStats::percentile_u64(&mut t.clone(), 90.0),
                ModelStats::percentile_u64(&mut t, 99.0)).unwrap();
        } else {
            writeln!(out, "| TTFB | 无数据 |").unwrap();
        }

        if !m.ttfts.is_empty() {
            let mut t = m.ttfts.clone();
            let avg = m.total_ttft_ms / m.ttfts.len() as u64;
            writeln!(out, "| TTFT(平均) | {}ms |", avg).unwrap();
            writeln!(out, "| TTFT P50/P90/P99 | {}ms / {}ms / {}ms |",
                ModelStats::percentile_u64(&mut t.clone(), 50.0),
                ModelStats::percentile_u64(&mut t.clone(), 90.0),
                ModelStats::percentile_u64(&mut t, 99.0)).unwrap();
        }

        if !m.tok_per_secs.is_empty() {
            writeln!(out, "| Token速度 P50/P90/P99 | {:.1}/{:.1}/{:.1} tok/s |",
                ModelStats::percentile_f64(&mut m.tok_per_secs.clone(), 50.0),
                ModelStats::percentile_f64(&mut m.tok_per_secs.clone(), 90.0),
                ModelStats::percentile_f64(&mut m.tok_per_secs.clone(), 99.0)).unwrap();
        }

        if m.total_tokens > 0 {
            writeln!(out, "| 总Token | {} |", m.total_tokens).unwrap();
            writeln!(out, "| 平均Token/请求 | {:.1} |", m.total_tokens as f64 / m.count as f64).unwrap();
        }
        writeln!(out).unwrap();
    }

    // 后端
    writeln!(out, "## 🖥️ 后端使用分布").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| 后端 | 请求数 | 占比 |").unwrap();
    writeln!(out, "|------|--------|------|").unwrap();
    let mut backends: Vec<_> = stats.backend_stats.iter().collect();
    backends.sort_by(|a, b| b.1.cmp(a.1));
    for (backend, count) in &backends {
        writeln!(out, "| {} | {} | {:.1}% |", backend, count,
            **count as f64 / stats.total_requests as f64 * 100.0).unwrap();
    }
    writeln!(out).unwrap();

    // 时间分布
    writeln!(out, "## ⏰ 时间分布").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| 小时 | 请求数 | 占比 |").unwrap();
    writeln!(out, "|------|--------|------|").unwrap();
    for (hour, count) in &stats.requests_by_hour {
        writeln!(out, "| {}h | {} | {:.1}% |", hour, count,
            *count as f64 / stats.total_requests as f64 * 100.0).unwrap();
    }
    writeln!(out).unwrap();

    // 错误
    if !stats.errors_by_type.is_empty() {
        writeln!(out, "## ❌ 错误统计").unwrap();
        writeln!(out).unwrap();
        writeln!(out, "| 错误类型 | 次数 | 占比 |").unwrap();
        writeln!(out, "|----------|------|------|").unwrap();
        let mut errors: Vec<_> = stats.errors_by_type.iter().collect();
        errors.sort_by(|a, b| b.1.cmp(a.1));
        for (error, count) in &errors {
            writeln!(out, "| {} | {} | {:.1}% |", error, count,
                **count as f64 / stats.total_requests as f64 * 100.0).unwrap();
        }
    }

    eprintln!("✅ 分析报告已生成: {}", output_path);
    Ok(())
}