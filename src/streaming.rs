use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::backend_metrics::BackendMetrics;
use crate::metrics::MetricsStore;

#[derive(Debug)]
pub struct StreamCompletion {
    pub timed_out: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn instrument_stream(
    body: Body,
    model: String,
    resolved: String,
    backend_name: String,
    ttfb: Duration,
    t_start: Instant,
    stream_idle_timeout: Duration,
    stream_first_chunk_timeout: Duration,
    stream_total_timeout: Duration,
    metrics: Arc<BackendMetrics>,
    store: Arc<MetricsStore>,
    request_id: String,
) -> (Body, JoinHandle<StreamCompletion>) {
    let rid = request_id.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);

    let done = tokio::spawn(async move {
        use http_body_util::BodyExt;

        let mut body = std::pin::pin!(body);
        let mut content_text = String::with_capacity(8192);
        let mut backend_tokens: Option<u32> = None;
        let mut first_text_token: Option<Instant> = None;
        let mut in_thinking = false;
        let mut last_speed_check = Instant::now();
        let mut tokens_at_last_check: usize = 0;
        let mut slow_warning_sent = false;
        let mut buffer = Vec::with_capacity(8192);
        let mut effective_chunk_seen = false;
        let mut last_effective_activity = tokio::time::Instant::now();
        let stream_total_deadline = tokio::time::Instant::now() + stream_total_timeout;

        loop {
            let idle_timeout = if effective_chunk_seen {
                stream_idle_timeout
            } else {
                stream_first_chunk_timeout
            };
            let idle_deadline = last_effective_activity + idle_timeout;
            let deadline = std::cmp::min(idle_deadline, stream_total_deadline);

            let frame_result = match tokio::time::timeout_at(deadline, body.frame()).await {
                Ok(Some(result)) => result,
                Ok(None) => break,
                Err(_) => {
                    let now = tokio::time::Instant::now();
                    if now >= stream_total_deadline {
                        warn!(
                            "[{}] Stream total timeout ({}s) on {} via {}, sending error to client",
                            rid,
                            stream_total_timeout.as_secs(),
                            model,
                            backend_name
                        );
                        let _ = tx
                            .send(Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                format!(
                                    "Stream total timeout after {}s",
                                    stream_total_timeout.as_secs()
                                ),
                            )))
                            .await;
                    } else {
                        warn!(
                            "[{}] Stream idle timeout ({}s) on {} via {}, sending error to client",
                            rid,
                            idle_timeout.as_secs(),
                            model,
                            backend_name
                        );
                        let _ = tx
                            .send(Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                format!("Stream idle timeout after {}s", idle_timeout.as_secs()),
                            )))
                            .await;
                    }
                    return StreamCompletion { timed_out: true };
                }
            };

            let frame = match frame_result {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.send(Err(std::io::Error::other(e))).await;
                    return StreamCompletion { timed_out: false };
                }
            };

            if let Ok(data) = frame.into_data() {
                buffer.extend_from_slice(&data);
                let mut newline_idx = 0usize;
                let mut saw_effective_chunk = false;

                while newline_idx < buffer.len() {
                    let remaining = &buffer[newline_idx..];
                    match memchr::memchr(b'\n', remaining) {
                        Some(pos) => {
                            let line = &buffer[newline_idx..newline_idx + pos];
                            newline_idx += pos + 1;

                            if let Some(sse_data) = line.strip_prefix(b"data: ") {
                                if sse_data != b"[DONE]" {
                                    saw_effective_chunk = true;
                                    if let Ok(text) = std::str::from_utf8(sse_data) {
                                        if !in_thinking && text.contains(r#"\"type\":\"thinking\""#)
                                        {
                                            in_thinking = true;
                                            info!(
                                                "[{}] [THINKING] Started on {} via {}",
                                                rid, model, backend_name
                                            );
                                        }
                                        if in_thinking
                                            && (text.contains(r#"\"type\":\"content_block_stop\""#)
                                                || text.contains(r#"\"content_block_stop\""#))
                                            && !text.contains(r#"\"type\":\"thinking\""#)
                                        {
                                            in_thinking = false;
                                            info!("[{}] [THINKING] Ended on {} via {} (elapsed: {}ms)", rid, model, backend_name, t_start.elapsed().as_millis());
                                        }
                                        if first_text_token.is_none()
                                            && (text.contains("\"text_delta\"")
                                                || text.contains("\"content\":\"")
                                                || text.contains("\"reasoning_content\":\""))
                                        {
                                            first_text_token = Some(Instant::now());
                                        }
                                        append_content_text(text, &mut content_text);
                                        if let Some(n) = extract_completion_tokens(text) {
                                            backend_tokens = Some(n);
                                        }
                                        let check_elapsed = last_speed_check.elapsed();
                                        if check_elapsed >= Duration::from_secs(10) {
                                            let current_token_count = content_text.len();
                                            let recent_chars = current_token_count
                                                .saturating_sub(tokens_at_last_check);
                                            let recent_tps =
                                                recent_chars as f64 / check_elapsed.as_secs_f64();
                                            if recent_tps < 4.0
                                                && recent_tps > 0.0
                                                && !slow_warning_sent
                                            {
                                                warn!(
                                                    "[{}] [SLOW STREAM] {} via {}: ~{:.1} tok/s in last {:.0}s (total elapsed: {:.1}s)",
                                                    rid,
                                                    model, backend_name, recent_tps / 4.0, check_elapsed.as_secs_f64(),
                                                    t_start.elapsed().as_secs_f64()
                                                );
                                                slow_warning_sent = true;
                                            }
                                            tokens_at_last_check = current_token_count;
                                            last_speed_check = Instant::now();
                                        }
                                    }
                                }
                            }
                        }
                        None => break,
                    }
                }

                if newline_idx > 0 {
                    let remaining = buffer.len() - newline_idx;
                    if remaining > 0 {
                        buffer.copy_within(newline_idx.., 0);
                    }
                    buffer.truncate(remaining);
                }

                if saw_effective_chunk {
                    effective_chunk_seen = true;
                    last_effective_activity = tokio::time::Instant::now();
                }

                if tx.send(Ok(data)).await.is_err() {
                    info!("[{}] [STREAM] Client disconnected: {} via {} (tokens so far: {}, thinking={})", rid, model, backend_name, content_text.len(), in_thinking);
                    break;
                }
            }
        }

        let total_ms = t_start.elapsed().as_millis() as u64;
        let ttfb_ms = ttfb.as_millis() as u64;
        let ttft_ms = first_text_token
            .map(|t| t.duration_since(t_start).as_millis() as u64)
            .unwrap_or(ttfb_ms);
        let streaming_ms = total_ms.saturating_sub(ttft_ms);
        let output_tokens = backend_tokens.unwrap_or_else(|| count_tokens(&content_text));
        let denom_ms = if streaming_ms < 1000 {
            total_ms
        } else {
            streaming_ms
        };
        let tokens_per_sec = if output_tokens > 0 && denom_ms > 0 {
            output_tokens as f64 / (denom_ms as f64 / 1000.0)
        } else {
            0.0
        };

        metrics.record(&backend_name, tokens_per_sec);
        store.record(
            &backend_name,
            &resolved,
            tokens_per_sec,
            ttfb_ms,
            ttft_ms,
            total_ms,
            output_tokens,
        );

        info!("[{}] Done: {} -> {} via {} | total={}ms, ttfb={}ms, ttft={}ms, tokens={output_tokens}, {tokens_per_sec:.1}tok/s", rid, model, resolved, backend_name, total_ms, ttfb_ms, ttft_ms);
        StreamCompletion { timed_out: false }
    });

    let new_body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    (new_body, done)
}

/// Extract text from content, reasoning_content, and thinking fields.
/// Accumulates raw UTF-8 bytes (no serde_json, non-blocking).
fn append_content_text(json: &str, content_text: &mut String) {
    // 说明：这里是 stream 热路径，避免重复的 O(n*k) windows 扫描。
    // 我们采用单次线性扫描，识别三个字段：content / reasoning_content / thinking。
    // 保持原有语义：遇到字段后仍通过 append_json_string_value 进行 JSON escape 解码。
    let bytes = json.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        i += 1;
        if i >= bytes.len() {
            break;
        }
        // 读取 key（不做通用 JSON 解析，只覆盖我们关心的几个 key，且正确跳过转义）
        let key_start = i;
        let mut escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                break;
            }
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key_end = i;
        i += 1; // skip closing quote
                // 跳过空白，确认 ':'
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b':' {
            continue;
        }
        i += 1;
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'"' {
            continue;
        }
        // 现在 i 指向 value 的 opening quote
        let value_start = i + 1;
        let key = &bytes[key_start..key_end];
        if key == b"reasoning_content" || key == b"thinking" || key == b"content" {
            // 原逻辑：content 需排除 _content（如 reasoning_content）
            if key == b"content" && key_start >= 2 && bytes[key_start - 2] == b'_' {
                // ..."_content":"..."... 这种情况跳过
            } else {
                append_json_string_value(bytes, value_start, content_text);
            }
        }
        // 跳过 value 字符串内容（正确处理转义），避免对长文本重复扫描
        i = value_start;
        escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                i += 1;
                break;
            }
            i += 1;
        }
    }
}

fn append_json_string_value(bytes: &[u8], start: usize, out: &mut String) {
    let mut j = start;
    let mut escaped = false;
    while j < bytes.len() {
        let b = bytes[j];
        if escaped {
            escaped = false;
        } else if b == b'\\' {
            escaped = true;
        } else if b == b'"' {
            let json_start = start.saturating_sub(1);
            if let Ok(value) = serde_json::from_slice::<String>(&bytes[json_start..=j]) {
                out.push_str(&value);
            } else if let Ok(value) = std::str::from_utf8(&bytes[start..j]) {
                out.push_str(value);
            }
            return;
        }
        j += 1;
    }
}

/// Extract completion_tokens from backend's final usage chunk.
fn extract_completion_tokens(json: &str) -> Option<u32> {
    // 热路径：避免 windows().position() 额外迭代开销。
    // 这里不做完整 JSON 解析，只寻找 "completion_tokens": 后的连续数字。
    let bytes = json.as_bytes();
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        i += 1;
        let key_start = i;
        let mut escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                break;
            }
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key_end = i;
        i += 1; // closing quote
        if &bytes[key_start..key_end] != b"completion_tokens" {
            continue;
        }
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b':' {
            continue;
        }
        i += 1;
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if !bytes[i].is_ascii_digit() {
            return None;
        }
        let mut val: u32 = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b.is_ascii_digit() {
                val = val.saturating_mul(10).saturating_add((b - b'0') as u32);
                i += 1;
            } else {
                break;
            }
        }
        return Some(val);
    }
    None
}

/// Count tokens using tiktoken cl100k_base BPE encoding.
/// Accurate for mixed Chinese/English text across most modern LLMs.
/// BPE table is cached via OnceLock — init cost paid once, not per-request.
fn count_tokens(text: &str) -> u32 {
    static BPE: OnceLock<tiktoken_rs::CoreBPE> = OnceLock::new();
    let bpe = BPE.get_or_init(|| tiktoken_rs::cl100k_base().expect("tiktoken init failed"));
    bpe.encode_ordinary(text).len() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration as TokioDuration};

    #[test]
    fn append_content_text_decodes_standard_json_escapes() {
        let mut out = String::new();
        append_content_text(r#"{"content":"hello\n\u4e16\u754c\uD83D\uDE03"}"#, &mut out);
        assert_eq!(out, "hello\n世界😃");
    }

    #[test]
    fn append_content_text_preserves_multiple_supported_fields() {
        let mut out = String::new();
        append_content_text(
            r#"{"reasoning_content":"think\t","content":"answer","thinking":"done"}"#,
            &mut out,
        );
        assert_eq!(out, "think\tanswerdone");
    }

    #[test]
    fn extract_completion_tokens_parses_number() {
        let json = r#"{"usage":{"completion_tokens":123,"prompt_tokens":9}}"#;
        assert_eq!(extract_completion_tokens(json), Some(123));
    }

    #[test]
    fn append_content_text_ignores_nested_suffix_content_key() {
        let mut out = String::new();
        append_content_text(r#"{"foo_content":"skip","content":"keep"}"#, &mut out);
        assert_eq!(out, "keep");
    }

    #[test]
    fn extract_completion_tokens_rejects_missing_and_non_numeric_values() {
        assert_eq!(extract_completion_tokens(r#"{"usage":{"prompt_tokens":9}}"#), None);
        assert_eq!(extract_completion_tokens(r#"{"completion_tokens":"12"}"#), None);
        assert_eq!(extract_completion_tokens(r#"{"completion_tokens" 42}"#), None);
        assert_eq!(extract_completion_tokens(r#"{"completion_tokens": 42, "x": 1}"#), Some(42));
    }

    #[test]
    fn count_tokens_handles_mixed_language_text() {
        assert!(count_tokens("hello 世界").gt(&0));
    }

    #[test]
    fn append_content_text_handles_malformed_and_non_string_fields() {
        let mut out = String::new();
        append_content_text(r#"{"unterminated"#, &mut out);
        append_content_text(r#"{"escaped\"key":"ignored","content":"ok"}"#, &mut out);
        append_content_text(r#"{"content": 1, "thinking":"done"}"#, &mut out);
        append_content_text(r#"{"content":"raw\qfallback"}"#, &mut out);

        assert_eq!(out, "okdoneraw\\qfallback");
    }

    #[tokio::test]
    async fn instrument_stream_records_metrics_on_successful_completion() {
        let metrics = Arc::new(BackendMetrics::new(5));
        let dir = "target/test-metrics/streaming_success_records_metrics";
        let _ = std::fs::remove_dir_all(dir);
        let store = Arc::new(MetricsStore::new(dir));
        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
            ));
            sleep(TokioDuration::from_millis(10)).await;
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(
                b"data: {\"usage\":{\"completion_tokens\":7}}\n\n",
            ));
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"data: [DONE]\n\n"));
        };
        let body = Body::from_stream(stream);

        let (instrumented, done) = instrument_stream(
            body,
            "mock-model".to_string(),
            "resolved-model".to_string(),
            "backend-a".to_string(),
            Duration::from_millis(2),
            Instant::now(),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Arc::clone(&metrics),
            Arc::clone(&store),
            "rid-success".to_string(),
        );

        let bytes = instrumented
            .collect()
            .await
            .expect("instrumented body")
            .to_bytes();
        let completion = done.await.expect("stream task");

        assert!(!completion.timed_out);
        assert!(std::str::from_utf8(&bytes).expect("utf8 body").contains("hello"));
        assert!(metrics.avg("backend-a") > 0.0);
        assert!(store.avg_for_model("resolved-model") > 0.0);
    }

    #[tokio::test]
    async fn instrument_stream_first_chunk_timeout_returns_timed_out_completion() {
        let stream = async_stream::stream! {
            sleep(TokioDuration::from_millis(80)).await;
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"data: {\"content\":\"late\"}\n\n"));
        };
        let body = Body::from_stream(stream);
        let metrics = Arc::new(BackendMetrics::new(5));
        let store = Arc::new(MetricsStore::new(
            "target/test-metrics/streaming_first_chunk_timeout",
        ));

        let (instrumented, done) = instrument_stream(
            body,
            "mock-model".to_string(),
            "resolved-model".to_string(),
            "backend-a".to_string(),
            Duration::from_millis(1),
            Instant::now(),
            Duration::from_secs(1),
            Duration::from_millis(20),
            Duration::from_secs(1),
            metrics,
            store,
            "rid-timeout".to_string(),
        );

        let body_result = instrumented.collect().await;
        let completion = done.await.expect("stream task");

        assert!(completion.timed_out);
        assert!(body_result.is_err());
    }

    #[tokio::test]
    async fn instrument_stream_idle_timeout_ignores_role_only_chunks() {
        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n",
            ));
            sleep(TokioDuration::from_millis(15)).await;
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
            ));
            sleep(TokioDuration::from_millis(70)).await;
            yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"late\"}}]}\n\n",
            ));
        };
        let body = Body::from_stream(stream);
        let metrics = Arc::new(BackendMetrics::new(5));
        let store = Arc::new(MetricsStore::new(
            "target/test-metrics/streaming_idle_timeout_ignores_role_only",
        ));

        let (instrumented, done) = instrument_stream(
            body,
            "mock-model".to_string(),
            "resolved-model".to_string(),
            "backend-a".to_string(),
            Duration::from_millis(1),
            Instant::now(),
            Duration::from_millis(40),
            Duration::from_secs(1),
            Duration::from_secs(1),
            metrics,
            store,
            "rid-idle-timeout".to_string(),
        );

        let body_result = instrumented.collect().await;
        let completion = done.await.expect("stream task");

        assert!(completion.timed_out);
        assert!(body_result.is_err());
    }
}
