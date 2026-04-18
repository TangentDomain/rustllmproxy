use criterion::{black_box, criterion_group, criterion_main, Criterion};
use llmproxy::config::Backend;
use std::collections::HashMap;

fn bench_extract_model_old(c: &mut Criterion) {
    let json = br#"{"model":"glm-5.1","messages":[{"role":"user","content":"test"}],"stream":true}"#;

    c.bench_function("extract_model_old_serde", |b| {
        b.iter(|| {
            let v: serde_json::Value = serde_json::from_slice(black_box(json)).unwrap();
            v["model"].as_str().map(|s| s.to_string()).unwrap_or_default()
        })
    });
}

fn bench_extract_model_new(c: &mut Criterion) {
    let json = br#"{"model":"glm-5.1","messages":[{"role":"user","content":"test"}],"stream":true}"#;

    c.bench_function("extract_model_new_byte_scan", |b| {
        b.iter(|| {
            const PATTERN: &[u8] = b"\"model\"";
            let bytes = black_box(json);
            if let Some(pos) = bytes.windows(PATTERN.len()).position(|w| w == PATTERN) {
                let after_key = &bytes[pos + PATTERN.len()..];
                // 简化版本：直接跳过已知格式
                if let Some(start) = after_key.iter().position(|&c| c == b'"') {
                    if let Some(end) = after_key[start+1..].iter().position(|&c| c == b'"') {
                        return String::from_utf8_lossy(&after_key[start+1..start+1+end]).into_owned();
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
    };

    c.bench_function("supports_model_old_to_string", |b| {
        b.iter(|| {
            backend.models.contains(&black_box("glm-5.1").to_string())
        })
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
    };

    c.bench_function("supports_model_new_iter_any", |b| {
        b.iter(|| {
            backend.models.iter().any(|m| m == black_box("glm-5.1"))
        })
    });
}

criterion_group!(
    benches,
    bench_extract_model_old,
    bench_extract_model_new,
    bench_supports_model_old,
    bench_supports_model_new
);
criterion_main!(benches);
