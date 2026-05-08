//! `log_analyzer` 的可测试纯逻辑实现。
//!
//! 原始 `src/bin/log_analyzer.rs` 负责 CLI glue：参数读取、文件 IO、输出路径约定。
//! 本模块提供：
//! - 日志行解析（尽量保持与原行为一致）
//! - 统计聚合
//! - Markdown 生成

use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct RequestRecord {
    pub timestamp: DateTime<Utc>,
    pub requested_model: String,
    pub resolved_model: String,
    pub path: String,
    pub protocol: Option<String>,
    pub body_size: usize,
    pub backend: Option<String>,
    pub ttfb: Option<u64>,
    pub ttft: Option<u64>,
    pub total: Option<u64>,
    pub tokens: Option<u32>,
    pub tok_per_sec: Option<f64>,
    pub stream: Option<bool>,
    pub error: Option<String>,
}

pub fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    let timestamp_end = s.find(' ')?;
    let naive =
        NaiveDateTime::parse_from_str(&s[..timestamp_end], "%Y-%m-%dT%H:%M:%S%.6fZ").ok()?;
    Some(DateTime::from_naive_utc_and_offset(naive, Utc))
}

pub fn extract_field(line: &str, key: &str) -> Option<String> {
    let pattern = format!("{}=", key);
    let start = line.find(&pattern)? + pattern.len();
    let remaining = &line[start..];
    let end = remaining
        .find(&[',', '}', '|', ')'][..])
        .unwrap_or(remaining.len());
    Some(remaining[..end].to_string())
}

pub fn extract_field_u64(line: &str, key: &str) -> Option<u64> {
    extract_field(line, key)?.replace("ms", "").parse().ok()
}

pub fn extract_field_usize(line: &str, key: &str) -> Option<usize> {
    extract_field(line, key)?.replace("B", "").parse().ok()
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ModelStats {
    pub count: usize,
    pub fallback_count: usize,
    pub total_tokens: u32,
    pub total_duration_ms: u64,
    pub total_ttfb_ms: u64,
    pub total_ttft_ms: u64,
    pub durations: Vec<u64>,
    pub ttfbs: Vec<u64>,
    pub ttfts: Vec<u64>,
    pub tok_per_secs: Vec<f64>,
    pub error_count: usize,
}

impl ModelStats {
    pub fn percentile_u64(vec: &mut [u64], p: f64) -> u64 {
        if vec.is_empty() {
            return 0;
        }
        vec.sort_unstable();
        let idx = (vec.len() as f64 * p / 100.0).floor() as usize;
        vec[idx.min(vec.len() - 1)]
    }

    pub fn percentile_f64(vec: &[f64], p: f64) -> f64 {
        if vec.is_empty() {
            return 0.0;
        }
        let mut v = vec.to_vec();
        v.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
        let idx = (v.len() as f64 * p / 100.0).floor() as usize;
        v[idx.min(v.len() - 1)]
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct LogStats {
    pub total_requests: usize,
    pub total_tokens: u32,
    pub fallback_count: usize,
    pub error_count: usize,
    pub stream_count: usize,
    pub non_stream_count: usize,
    pub requests_by_requested_model: HashMap<String, usize>,
    pub requests_by_resolved_model: HashMap<String, usize>,
    pub fallback_chains: HashMap<String, usize>,
    pub model_stats: HashMap<String, ModelStats>,
    pub requests_by_hour: BTreeMap<String, usize>,
    pub errors_by_type: HashMap<String, usize>,
    pub backend_stats: HashMap<String, usize>,
    pub all_durations: Vec<u64>,
    pub all_ttfbs: Vec<u64>,
    pub all_ttfts: Vec<u64>,
}

pub fn percentile_u64(vec: &mut [u64], p: f64) -> u64 {
    if vec.is_empty() {
        return 0;
    }
    vec.sort_unstable();
    let idx = (vec.len() as f64 * p / 100.0).floor() as usize;
    vec[idx.min(vec.len() - 1)]
}

/// 从 log lines 构建统计信息。
///
/// 约定：
/// - 以 `Request model=` 开始一条记录
/// - 记录在遇到对应 `Done:` 时落地
pub fn analyze_lines<I>(lines: I, now_fallback: DateTime<Utc>) -> LogStats
where
    I: IntoIterator<Item = String>,
{
    let mut stats = LogStats::default();
    let mut current_record: Option<RequestRecord> = None;

    for line in lines {
        if line.contains("Request model=") {
            let mut record = RequestRecord::default();
            record.requested_model = extract_field(&line, "model").unwrap_or_default();
            record.resolved_model = record.requested_model.clone();
            record.path = extract_field(&line, "path").unwrap_or_default();
            record.protocol = extract_field(&line, "protocol").map(|p| {
                p.trim_start_matches("Some(\"")
                    .trim_end_matches("\")")
                    .to_string()
            });
            record.body_size = extract_field_usize(&line, "body").unwrap_or(0);
            record.timestamp = parse_timestamp(&line).unwrap_or(now_fallback);
            current_record = Some(record);
            continue;
        }

        if let Some(ref mut record) = current_record {
            if line.contains("Responding:") {
                let responding_part = line.split("Responding: ").nth(1).unwrap_or("");
                let model_part = responding_part.split(" via ").next().unwrap_or("");
                let parts: Vec<&str> = model_part.split(" -> ").collect();
                if parts.len() >= 2 {
                    record.resolved_model = parts[1].trim().to_string();
                }
                record.backend = line
                    .split(" via ")
                    .nth(1)
                    .map(|s| s.split('|').next().unwrap_or(s).trim().to_string());
                record.ttfb = extract_field_u64(&line, "ttfb");
                let stream_val = extract_field(&line, "stream");
                record.stream = Some(stream_val.as_deref() == Some("true"));
                continue;
            }

            if line.contains("Done:") {
                record.total = extract_field_u64(&line, "total");
                record.ttft = extract_field_u64(&line, "ttft");
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

                apply_record(&mut stats, record);
                current_record = None;
            }
        }
    }

    stats
}

fn apply_record(stats: &mut LogStats, record: &RequestRecord) {
    let is_fallback = record.requested_model != record.resolved_model;
    if is_fallback {
        stats.fallback_count += 1;
        let chain = format!("{} -> {}", record.requested_model, record.resolved_model);
        *stats.fallback_chains.entry(chain).or_insert(0) += 1;
    }

    stats.total_requests += 1;
    *stats
        .requests_by_requested_model
        .entry(record.requested_model.clone())
        .or_insert(0) += 1;
    *stats
        .requests_by_resolved_model
        .entry(record.resolved_model.clone())
        .or_insert(0) += 1;

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

    let model_key = record.requested_model.clone();
    let m = stats.model_stats.entry(model_key).or_default();
    m.count += 1;
    if is_fallback {
        m.fallback_count += 1;
    }
    if let Some(tokens) = record.tokens {
        m.total_tokens += tokens;
    }
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
    if record.error.is_some() {
        m.error_count += 1;
    }

    let hour = format!("{:02}", record.timestamp.hour());
    *stats.requests_by_hour.entry(hour).or_insert(0) += 1;
}

/// 生成 Markdown 报告内容。
///
/// `generated_at` 由调用方决定（保持 bin 的显示语义）。
pub fn render_markdown(log_path: &str, generated_at: &str, stats: &LogStats) -> String {
    let mut out = String::new();
    use std::fmt::Write as _;

    writeln!(&mut out, "# LLM Proxy 日志分析报告").ok();
    writeln!(&mut out).ok();
    writeln!(&mut out, "**日志文件**: `{}`", log_path).ok();
    writeln!(&mut out, "**生成时间**: {}", generated_at).ok();
    writeln!(&mut out).ok();

    // 总览
    writeln!(&mut out, "## 📊 概览").ok();
    writeln!(&mut out).ok();
    writeln!(&mut out, "| 指标 | 数值 | 占比 |").ok();
    writeln!(&mut out, "|------|------|------|").ok();
    writeln!(&mut out, "| 总请求数 | {} | 100% |", stats.total_requests).ok();
    writeln!(&mut out, "| 总Token数 | {} | - |", stats.total_tokens).ok();

    let fallback_pct = if stats.total_requests == 0 {
        0.0
    } else {
        stats.fallback_count as f64 / stats.total_requests as f64 * 100.0
    };
    let error_pct = if stats.total_requests == 0 {
        0.0
    } else {
        stats.error_count as f64 / stats.total_requests as f64 * 100.0
    };
    let stream_pct = if stats.total_requests == 0 {
        0.0
    } else {
        stats.stream_count as f64 / stats.total_requests as f64 * 100.0
    };
    let non_stream_pct = if stats.total_requests == 0 {
        0.0
    } else {
        stats.non_stream_count as f64 / stats.total_requests as f64 * 100.0
    };

    writeln!(
        &mut out,
        "| Fallback请求 | {} | {:.1}% |",
        stats.fallback_count, fallback_pct
    )
    .ok();
    writeln!(
        &mut out,
        "| 错误请求 | {} | {:.1}% |",
        stats.error_count, error_pct
    )
    .ok();
    writeln!(
        &mut out,
        "| 流式请求 | {} | {:.1}% |",
        stats.stream_count, stream_pct
    )
    .ok();
    writeln!(
        &mut out,
        "| 非流式 | {} | {:.1}% |",
        stats.non_stream_count, non_stream_pct
    )
    .ok();
    writeln!(&mut out).ok();

    // 总体性能
    writeln!(&mut out, "## 📈 总体性能统计").ok();
    writeln!(&mut out).ok();

    if !stats.all_durations.is_empty() {
        let mut d = stats.all_durations.clone();
        let avg = stats.all_durations.iter().sum::<u64>() / stats.all_durations.len() as u64;
        writeln!(
            &mut out,
            "**总延迟 (total)** - 平均: {}ms, P50: {}ms, P90: {}ms, P99: {}ms",
            avg,
            percentile_u64(&mut d, 50.0),
            percentile_u64(&mut d, 90.0),
            percentile_u64(&mut d, 99.0)
        )
        .ok();
    }

    if !stats.all_ttfbs.is_empty() {
        let mut t = stats.all_ttfbs.clone();
        let avg = stats.all_ttfbs.iter().sum::<u64>() / stats.all_ttfbs.len() as u64;
        writeln!(
            &mut out,
            "**TTFB** - 平均: {}ms, P50: {}ms, P90: {}ms, P99: {}ms",
            avg,
            percentile_u64(&mut t, 50.0),
            percentile_u64(&mut t, 90.0),
            percentile_u64(&mut t, 99.0)
        )
        .ok();
    }

    if !stats.all_ttfts.is_empty() {
        let mut t = stats.all_ttfts.clone();
        let avg = stats.all_ttfts.iter().sum::<u64>() / stats.all_ttfts.len() as u64;
        writeln!(
            &mut out,
            "**TTFT** - 平均: {}ms, P50: {}ms, P90: {}ms, P99: {}ms",
            avg,
            percentile_u64(&mut t, 50.0),
            percentile_u64(&mut t, 90.0),
            percentile_u64(&mut t, 99.0)
        )
        .ok();
    } else {
        writeln!(&mut out, "**TTFT** - 无数据").ok();
    }
    writeln!(&mut out).ok();

    // Fallback
    writeln!(&mut out, "## 🔄 Fallback分析").ok();
    writeln!(&mut out).ok();
    let mut chains: Vec<_> = stats.fallback_chains.iter().collect();
    chains.sort_by(|a, b| b.1.cmp(a.1));
    if chains.is_empty() {
        writeln!(&mut out, "无Fallback记录").ok();
    } else {
        writeln!(&mut out, "| 请求模型 -> 实际模型 | 次数 | 占比 | ").ok();
        writeln!(&mut out, "|---------------------|------|------|").ok();
        for (chain, count) in &chains {
            let pct = if stats.total_requests == 0 {
                0.0
            } else {
                **count as f64 / stats.total_requests as f64 * 100.0
            };
            writeln!(&mut out, "| {} | {} | {:.1}% |", chain, count, pct).ok();
        }
    }
    writeln!(&mut out).ok();

    // 按模型
    writeln!(&mut out, "## 📊 按模型分组统计").ok();
    writeln!(&mut out).ok();
    let mut models: Vec<_> = stats.model_stats.iter().collect();
    models.sort_by(|a, b| b.1.count.cmp(&a.1.count));

    for (model, m) in &models {
        writeln!(&mut out, "### {}", model).ok();
        writeln!(&mut out).ok();
        writeln!(&mut out, "| 指标 | 数值 |").ok();
        writeln!(&mut out, "|------|------|").ok();
        writeln!(&mut out, "| 请求数 | {} |", m.count).ok();
        writeln!(&mut out, "| Fallback数 | {} |", m.fallback_count).ok();
        writeln!(&mut out, "| 错误数 | {} |", m.error_count).ok();

        if !m.durations.is_empty() {
            let mut d = m.durations.clone();
            let avg = m.total_duration_ms / m.durations.len() as u64;
            writeln!(&mut out, "| 总延迟(平均) | {}ms |", avg).ok();
            writeln!(
                &mut out,
                "| 总延迟 P50/P90/P99 | {}ms / {}ms / {}ms |",
                ModelStats::percentile_u64(&mut d.clone(), 50.0),
                ModelStats::percentile_u64(&mut d.clone(), 90.0),
                ModelStats::percentile_u64(&mut d, 99.0)
            )
            .ok();
        }

        if !m.ttfbs.is_empty() {
            let mut t = m.ttfbs.clone();
            let avg = m.total_ttfb_ms / m.ttfbs.len() as u64;
            writeln!(&mut out, "| TTFB(平均) | {}ms |", avg).ok();
            writeln!(
                &mut out,
                "| TTFB P50/P90/P99 | {}ms / {}ms / {}ms |",
                ModelStats::percentile_u64(&mut t.clone(), 50.0),
                ModelStats::percentile_u64(&mut t.clone(), 90.0),
                ModelStats::percentile_u64(&mut t, 99.0)
            )
            .ok();
        } else {
            writeln!(&mut out, "| TTFB | 无数据 |").ok();
        }

        if !m.ttfts.is_empty() {
            let mut t = m.ttfts.clone();
            let avg = m.total_ttft_ms / m.ttfts.len() as u64;
            writeln!(&mut out, "| TTFT(平均) | {}ms |", avg).ok();
            writeln!(
                &mut out,
                "| TTFT P50/P90/P99 | {}ms / {}ms / {}ms |",
                ModelStats::percentile_u64(&mut t.clone(), 50.0),
                ModelStats::percentile_u64(&mut t.clone(), 90.0),
                ModelStats::percentile_u64(&mut t, 99.0)
            )
            .ok();
        }

        if !m.tok_per_secs.is_empty() {
            writeln!(
                &mut out,
                "| Token速度 P50/P90/P99 | {:.1}/{:.1}/{:.1} tok/s |",
                ModelStats::percentile_f64(&m.tok_per_secs, 50.0),
                ModelStats::percentile_f64(&m.tok_per_secs, 90.0),
                ModelStats::percentile_f64(&m.tok_per_secs, 99.0)
            )
            .ok();
        }

        if m.total_tokens > 0 {
            writeln!(&mut out, "| 总Token | {} |", m.total_tokens).ok();
            writeln!(
                &mut out,
                "| 平均Token/请求 | {:.1} |",
                m.total_tokens as f64 / m.count as f64
            )
            .ok();
        }
        writeln!(&mut out).ok();
    }

    // 后端
    writeln!(&mut out, "## 🖥️ 后端使用分布").ok();
    writeln!(&mut out).ok();
    writeln!(&mut out, "| 后端 | 请求数 | 占比 |").ok();
    writeln!(&mut out, "|------|--------|------|").ok();
    let mut backends: Vec<_> = stats.backend_stats.iter().collect();
    backends.sort_by(|a, b| b.1.cmp(a.1));
    for (backend, count) in &backends {
        let pct = if stats.total_requests == 0 {
            0.0
        } else {
            **count as f64 / stats.total_requests as f64 * 100.0
        };
        writeln!(&mut out, "| {} | {} | {:.1}% |", backend, count, pct).ok();
    }
    writeln!(&mut out).ok();

    // 时间分布
    writeln!(&mut out, "## ⏰ 时间分布").ok();
    writeln!(&mut out).ok();
    writeln!(&mut out, "| 小时 | 请求数 | 占比 |").ok();
    writeln!(&mut out, "|------|--------|------|").ok();
    for (hour, count) in &stats.requests_by_hour {
        let pct = if stats.total_requests == 0 {
            0.0
        } else {
            *count as f64 / stats.total_requests as f64 * 100.0
        };
        writeln!(&mut out, "| {}h | {} | {:.1}% |", hour, count, pct).ok();
    }
    writeln!(&mut out).ok();

    // 错误
    if !stats.errors_by_type.is_empty() {
        writeln!(&mut out, "## ❌ 错误统计").ok();
        writeln!(&mut out).ok();
        writeln!(&mut out, "| 错误类型 | 次数 | 占比 |").ok();
        writeln!(&mut out, "|----------|------|------|").ok();
        let mut errors: Vec<_> = stats.errors_by_type.iter().collect();
        errors.sort_by(|a, b| b.1.cmp(a.1));
        for (error, count) in &errors {
            let pct = if stats.total_requests == 0 {
                0.0
            } else {
                **count as f64 / stats.total_requests as f64 * 100.0
            };
            writeln!(&mut out, "| {} | {} | {:.1}% |", error, count, pct).ok();
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_field_stops_on_delimiters() {
        let line = "foo model=glm-5.1, path=/v1/messages | rest";
        assert_eq!(extract_field(line, "model").as_deref(), Some("glm-5.1"));
        assert_eq!(
            extract_field(line, "path").as_deref(),
            Some("/v1/messages ")
        );
    }

    #[test]
    fn test_analyze_lines_fallback_and_error_classification() {
        let now = DateTime::from_timestamp(0, 0).unwrap();
        let lines = vec![
            "2026-01-01T00:00:00.000000Z Request model=glm-5.1, path=/v1/messages, protocol=Some(\"anthropic\"), body=10B".to_string(),
            "2026-01-01T00:00:00.000000Z Responding: glm-5.1 -> glm-4.7 via zhipu-anthropic | ttfb=5ms, stream=true".to_string(),
            "2026-01-01T00:00:00.000000Z Done: glm-5.1 -> glm-4.7 via zhipu-anthropic | total=100ms, ttfb=5ms, ttft=80ms, tokens=7, tok/s=70.0 ERROR exhausted".to_string(),
        ];

        let stats = analyze_lines(lines, now);
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.fallback_count, 1);
        assert_eq!(stats.error_count, 1);
        assert_eq!(stats.stream_count, 1);
        assert_eq!(stats.non_stream_count, 0);

        assert_eq!(stats.total_tokens, 7);
        assert_eq!(stats.all_durations, vec![100]);
        assert_eq!(stats.all_ttfbs, vec![5]);
        assert_eq!(stats.all_ttfts, vec![80]);

        assert_eq!(
            stats.fallback_chains.get("glm-5.1 -> glm-4.7").copied(),
            Some(1)
        );
        assert_eq!(
            stats.errors_by_type.get("all_backends_exhausted").copied(),
            Some(1)
        );

        let m = stats.model_stats.get("glm-5.1").unwrap();
        assert_eq!(m.count, 1);
        assert_eq!(m.fallback_count, 1);
        assert_eq!(m.error_count, 1);
        assert_eq!(m.total_tokens, 7);
        assert_eq!(m.ttfts, vec![80]);
    }

    #[test]
    fn test_render_markdown_contains_sections() {
        let stats = LogStats {
            total_requests: 1,
            ..Default::default()
        };
        let md = render_markdown("foo.log", "2026-01-01 00:00:00", &stats);
        assert!(md.contains("# LLM Proxy 日志分析报告"));
        assert!(md.contains("## 📊 概览"));
        assert!(md.contains("## 📈 总体性能统计"));
        assert!(md.contains("## 🔄 Fallback分析"));
        assert!(md.contains("## 📊 按模型分组统计"));
        assert!(md.contains("## 🖥️ 后端使用分布"));
        assert!(md.contains("## ⏰ 时间分布"));
    }
}
