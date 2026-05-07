use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use llmproxy::config::Backend;
use std::collections::HashMap;

fn bench_extract_model_old(c: &mut Criterion) {
    let json =
        br#"{"model":"glm-5.1","messages":[{"role":"user","content":"test"}],"stream":true}"#;

    c.bench_function("extract_model_old_serde", |b| {
        b.iter(|| {
            let v: serde_json::Value = serde_json::from_slice(black_box(json)).unwrap();
            v["model"]
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_default()
        })
    });
}

fn bench_extract_model_new(c: &mut Criterion) {
    let json =
        br#"{"model":"glm-5.1","messages":[{"role":"user","content":"test"}],"stream":true}"#;

    c.bench_function("extract_model_new_byte_scan", |b| {
        b.iter(|| {
            const PATTERN: &[u8] = b"\"model\"";
            let bytes = black_box(json);
            if let Some(pos) = bytes.windows(PATTERN.len()).position(|w| w == PATTERN) {
                let after_key = &bytes[pos + PATTERN.len()..];
                if let Some(start) = after_key.iter().position(|&c| c == b'"') {
                    if let Some(end) = after_key[start + 1..].iter().position(|&c| c == b'"') {
                        return String::from_utf8_lossy(&after_key[start + 1..start + 1 + end])
                            .into_owned();
                    }
                }
            }
            String::new()
        })
    });
}

fn bench_supports_model_old(c: &mut Criterion) {
    let backend = Backend {
        name: "test".to_string(),
        url: "https://test.com".to_string(),
        api_key: "key".to_string(),
        weight: 10,
        models: vec!["glm-5.1".to_string(), "glm-4.7".to_string()],
        timeout_secs: 30,
        connect_timeout_secs: 5,
        model_mappings: HashMap::new(),
        protocol: "openai".to_string(),
        auth_header: "Bearer key".to_string(),
        strip_params: vec![],
    };

    c.bench_function("supports_model_old_to_string", |b| {
        b.iter(|| backend.models.contains(&black_box("glm-5.1").to_string()))
    });
}

fn bench_supports_model_new(c: &mut Criterion) {
    let backend = Backend {
        name: "test".to_string(),
        url: "https://test.com".to_string(),
        api_key: "key".to_string(),
        weight: 10,
        models: vec!["glm-5.1".to_string(), "glm-4.7".to_string()],
        timeout_secs: 30,
        connect_timeout_secs: 5,
        model_mappings: HashMap::new(),
        protocol: "openai".to_string(),
        auth_header: "Bearer key".to_string(),
        strip_params: vec![],
    };

    c.bench_function("supports_model_new_iter_any", |b| {
        b.iter(|| backend.models.iter().any(|m| m == black_box("glm-5.1")))
    });
}

// --- SSE parsing hot path benchmarks ---

/// Simulates the SSE buffer parsing loop from instrument_stream
fn bench_sse_line_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("sse_line_parsing");

    // Typical SSE chunk from an LLM API
    let sse_lines: Vec<&[u8]> = vec![
        b"data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1234567890,\"model\":\"glm-5.1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}],\"output_tokens\":42}\n",
        b"data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1234567890,\"model\":\"glm-5.1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n",
        b"data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1234567890,\"model\":\"glm-5.1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"},\"finish_reason\":null}]}\n",
        b"\n",
        b"data: [DONE]\n",
    ];

    // Build a combined buffer simulating one chunk from the stream
    let mut combined: Vec<u8> = Vec::new();
    for line in &sse_lines {
        combined.extend_from_slice(line);
    }

    group.throughput(Throughput::Bytes(combined.len() as u64));

    // Benchmark: current implementation (copy_within approach)
    group.bench_function("copy_within_buffer", |b| {
        b.iter(|| {
            let mut buffer = combined.clone();
            let mut output_tokens: u32 = 0;
            let mut newline_idx = 0;

            while newline_idx < buffer.len() {
                if let Some(pos) = buffer[newline_idx..].iter().position(|&b| b == b'\n') {
                    let line = &buffer[newline_idx..newline_idx + pos];
                    newline_idx += pos + 1;

                    if let Some(sse_data) = line.strip_prefix(b"data: ") {
                        if sse_data != b"[DONE]" {
                            if let Ok(text) = std::str::from_utf8(sse_data) {
                                if let Some(val) = extract_json_uint_fast_bench(text) {
                                    output_tokens = output_tokens.max(val);
                                }
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            if newline_idx > 0 {
                buffer.copy_within(newline_idx.., 0);
                buffer.truncate(buffer.len() - newline_idx);
            }

            black_box(output_tokens);
            black_box(buffer);
        });
    });

    // Benchmark: old split_off approach for comparison
    group.bench_function("split_off_buffer", |b| {
        b.iter(|| {
            let mut buffer = combined.clone();
            let mut output_tokens: u32 = 0;
            let mut processed = 0;

            while processed < buffer.len() {
                if let Some(pos) = buffer[processed..].iter().position(|&b| b == b'\n') {
                    let line = &buffer[processed..processed + pos];
                    processed += pos + 1;

                    if let Some(sse_data) = line.strip_prefix(b"data: ") {
                        if sse_data != b"[DONE]" {
                            if let Ok(text) = std::str::from_utf8(sse_data) {
                                if let Some(val) = extract_json_uint_fast_bench(text) {
                                    output_tokens = output_tokens.max(val);
                                }
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            if processed > 0 {
                buffer = buffer.split_off(processed);
            }

            black_box(output_tokens);
            black_box(buffer);
        });
    });

    group.finish();
}

/// Benchmark extract_json_uint variants
fn bench_extract_json_uint(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_json_uint");

    let json1 =
        r#"{"id":"chatcmpl-123","choices":[{"delta":{"content":"Hello"}}],"output_tokens":42}"#;
    let json2 =
        r#"{"id":"chatcmpl-123","choices":[{"delta":{"content":"Hello"}}],"usage":{"tokens":99}}"#;
    let json_no_tokens = r#"{"id":"chatcmpl-123","choices":[{"delta":{"content":"Hello"}}]}"#;

    group.bench_function("fast_with_output_tokens", |b| {
        b.iter(|| black_box(extract_json_uint_fast_bench(black_box(json1))))
    });

    group.bench_function("fast_with_tokens", |b| {
        b.iter(|| black_box(extract_json_uint_fast_bench(black_box(json2))))
    });

    group.bench_function("fast_no_tokens", |b| {
        b.iter(|| black_box(extract_json_uint_fast_bench(black_box(json_no_tokens))))
    });

    // Old format!-based version for comparison
    group.bench_function("old_format_based", |b| {
        b.iter(|| {
            let field = "output_tokens";
            let search = format!("\"{field}\":");
            let pos = json1.find(&search);
            if let Some(p) = pos {
                let after = json1[p + search.len()..].trim_start();
                let num: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
                num.parse::<u32>().ok()
            } else {
                None
            }
        })
    });

    group.finish();
}

/// Benchmark extract_model_from_json with different body sizes
fn bench_extract_model_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_model_scaling");

    for size_kb in [1, 10, 100, 1000] {
        // Build a JSON body of approximately size_kb KB
        let filler = "x".repeat(size_kb * 1024 / 2);
        let json = format!(
            r#"{{"model":"glm-5.1","messages":[{{"role":"user","content":"{}"}}],"stream":true}}"#,
            filler
        );

        group.throughput(Throughput::Bytes(json.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("byte_scan", format!("{}KB", size_kb)),
            &json,
            |b, json| {
                b.iter(|| {
                    const PATTERN: &[u8] = b"\"model\"";
                    let bytes = json.as_bytes();
                    let mut result = String::new();
                    if let Some(pos) = bytes.windows(PATTERN.len()).position(|w| w == PATTERN) {
                        let after_key = &bytes[pos + PATTERN.len()..];
                        let after_colon = skip_ws(after_key);
                        if !after_colon.is_empty() && after_colon[0] == b':' {
                            let after_colon = skip_ws(&after_colon[1..]);
                            if !after_colon.is_empty() && after_colon[0] == b'"' {
                                if let Some(end) = after_colon[1..].iter().position(|&c| c == b'"')
                                {
                                    result = String::from_utf8_lossy(&after_colon[1..1 + end])
                                        .into_owned();
                                }
                            }
                        }
                    }
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark bytes.to_vec() allocation cost
fn bench_body_to_vec(c: &mut Criterion) {
    let mut group = c.benchmark_group("body_copy");
    let sizes = [1024, 10 * 1024, 100 * 1024, 1024 * 1024, 10 * 1024 * 1024];

    for size in sizes {
        let data = bytes::Bytes::from(vec![0u8; size]);
        let label = if size >= 1024 * 1024 {
            format!("{}MB", size / (1024 * 1024))
        } else {
            format!("{}KB", size / 1024)
        };

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::new("bytes_to_vec", &label),
            &data,
            |b, data| b.iter(|| black_box(data.to_vec())),
        );
    }

    group.finish();
}

// Helpers (replicated from proxy.rs to avoid visibility issues)

fn extract_json_uint_fast_bench(json: &str) -> Option<u32> {
    const PAT1: &[u8] = b"\"output_tokens\":";
    const PAT2: &[u8] = b"\"tokens\":";

    let bytes = json.as_bytes();
    let pos = if let Some(p) = bytes.windows(PAT1.len()).position(|w| w == PAT1) {
        p + PAT1.len()
    } else if let Some(p) = bytes.windows(PAT2.len()).position(|w| w == PAT2) {
        p + PAT2.len()
    } else {
        return None;
    };

    let after = &bytes[pos..];
    let start = after.iter().position(|&b| b.is_ascii_digit())?;
    let mut val: u32 = 0;
    for &b in &after[start..] {
        if b.is_ascii_digit() {
            val = val * 10 + (b - b'0') as u32;
        } else {
            break;
        }
    }
    Some(val)
}

fn skip_ws(s: &[u8]) -> &[u8] {
    s.iter()
        .position(|&c| c != b' ' && c != b'\t' && c != b'\n' && c != b'\r')
        .map_or(&[], |i| &s[i..])
}

criterion_group!(
    benches,
    bench_extract_model_old,
    bench_extract_model_new,
    bench_supports_model_old,
    bench_supports_model_new,
    bench_sse_line_parsing,
    bench_extract_json_uint,
    bench_extract_model_scaling,
    bench_body_to_vec
);
criterion_main!(benches);
