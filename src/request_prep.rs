use bytes::Bytes;

use crate::config::Backend;

/// 从 JSON bytes 中快速提取 "model" 字段值（手动扫描，避免完整反序列化）
/// 优化：只扫描前 2KB（model 字段始终在 JSON 开头附近）
#[inline]
pub(crate) fn extract_model_from_json(bytes: &[u8]) -> String {
    const PATTERN: &[u8] = b"\"model\"";
    // model 字段始终在 JSON 开头附近，只扫描前 2KB
    let scan_range = &bytes[..bytes.len().min(2048)];
    if let Some(pos) = scan_range.windows(PATTERN.len()).position(|w| w == PATTERN) {
        let after_key = &bytes[pos + PATTERN.len()..];
        let after_colon = skip_whitespace(after_key);
        if !after_colon.is_empty() && after_colon[0] == b':' {
            let after_colon = skip_whitespace(&after_colon[1..]);
            if !after_colon.is_empty() && after_colon[0] == b'\"' {
                if let Some(end) = after_colon[1..].iter().position(|&c| c == b'\"') {
                    let model_bytes = &after_colon[1..1 + end];
                    return std::str::from_utf8(model_bytes)
                        .map(|s| s.to_owned())
                        .unwrap_or_default();
                }
            }
        }
    }
    String::new()
}

/// 从 JSON bytes 中快速提取 max_tokens / max_completion_tokens（手动扫描，避免完整反序列化）
pub(crate) fn extract_max_tokens(bytes: &[u8]) -> Option<u32> {
    // 扫描前 8KB（max_tokens 通常在 model 附近）
    let scan_range = &bytes[..bytes.len().min(8192)];
    const MAX_TOK: &[u8] = b"\"max_tokens\"";
    const MAX_COMP_TOK: &[u8] = b"\"max_completion_tokens\"";
    for key in [MAX_TOK, MAX_COMP_TOK] {
        if let Some(pos) = scan_range.windows(key.len()).position(|w| w == key) {
            let after_key = &bytes[pos + key.len()..];
            let after_colon = skip_whitespace(after_key);
            if !after_colon.is_empty() && after_colon[0] == b':' {
                let after_colon = skip_whitespace(&after_colon[1..]);
                if !after_colon.is_empty()
                    && (after_colon[0].is_ascii_digit() || after_colon[0] == b'-')
                {
                    let mut val: u32 = 0;
                    for &b in after_colon.iter().take(10) {
                        if b.is_ascii_digit() {
                            val = val * 10 + (b - b'0') as u32;
                        } else {
                            break;
                        }
                    }
                    return Some(val);
                }
            }
        }
    }
    None
}

#[inline]
fn skip_whitespace(s: &[u8]) -> &[u8] {
    s.iter()
        .position(|&c| !matches!(c, b' ' | b'\t' | b'\n' | b'\r'))
        .map_or(&[], |i| &s[i..])
}

/// 字节级替换 JSON 中的 model 字段值，避免完整反序列化+序列化。
/// 优化：先找匹配位置，然后一次性构建结果，避免逐字节 push。
fn patch_json_model(bytes: &[u8], old_model: &str, new_model: &str) -> Vec<u8> {
    if old_model == new_model {
        return bytes.to_vec();
    }
    let pattern = "\"model\"".to_string();
    let p = pattern.as_bytes();
    let Some(mut pos) = bytes.windows(p.len()).position(|w| w == p) else {
        return bytes.to_vec();
    };
    pos += p.len();

    while pos < bytes.len() && matches!(bytes[pos], b' ' | b'\t' | b'\n' | b'\r') {
        pos += 1;
    }
    if pos >= bytes.len() || bytes[pos] != b':' {
        return bytes.to_vec();
    }
    pos += 1;
    while pos < bytes.len() && matches!(bytes[pos], b' ' | b'\t' | b'\n' | b'\r') {
        pos += 1;
    }
    if pos >= bytes.len() || bytes[pos] != b'\"' {
        return bytes.to_vec();
    }
    let value_start = pos + 1;
    let mut value_end = value_start;
    let mut escaped = false;
    while value_end < bytes.len() {
        let b = bytes[value_end];
        if escaped {
            escaped = false;
        } else if b == b'\\' {
            escaped = true;
        } else if b == b'\"' {
            break;
        }
        value_end += 1;
    }

    if value_end >= bytes.len() {
        return bytes.to_vec();
    }
    if std::str::from_utf8(&bytes[value_start..value_end]).ok() != Some(old_model) {
        return bytes.to_vec();
    }

    let mut result =
        Vec::with_capacity(bytes.len() + new_model.len().saturating_sub(old_model.len()));
    result.extend_from_slice(&bytes[..value_start]);
    result.extend_from_slice(new_model.as_bytes());
    result.extend_from_slice(&bytes[value_end..]);
    result
}

/// Prepare request body: apply model patch, then backend-specific transforms.
/// Preserves zero-copy path for unaffected backends (especially OpenAI).
pub(crate) fn prepare_request_body(
    bytes: &Bytes,
    original_model: &str,
    try_model: &str,
    resolved: &str,
    backend: &Backend,
) -> Bytes {
    let needs_model_patch = resolved != try_model || original_model != try_model;
    let needs_strip = !backend.strip_params.is_empty();
    let needs_sanitize = backend.name == "zhipu-anthropic";
    // MiniMax Anthropic needs max_completion_tokens → max_tokens rename
    let needs_max_tokens_rename =
        backend.protocol == "anthropic" && backend.name.starts_with("minimax-anthropic");

    if !needs_model_patch && !needs_strip && !needs_sanitize && !needs_max_tokens_rename {
        return bytes.clone();
    }

    let base: Vec<u8> = if needs_model_patch {
        patch_json_model(bytes, original_model, resolved)
    } else {
        bytes.to_vec()
    };

    // Step 2: sanitize for strict JSON parsers (zhipu-anthropic)
    let base = if needs_sanitize {
        sanitize_json_bytes(&base)
    } else {
        base
    };

    // Step 3: strip configured params + optional max_completion_tokens rename
    if !needs_strip && !needs_max_tokens_rename {
        return Bytes::from(base);
    }

    let mut val: serde_json::Value = match serde_json::from_slice(&base) {
        Ok(v) => v,
        Err(_) => return Bytes::from(base),
    };

    if let Some(obj) = val.as_object_mut() {
        for field in &backend.strip_params {
            obj.remove(field);
        }
        if needs_max_tokens_rename {
            if let Some(max_comp) = obj.remove("max_completion_tokens") {
                obj.entry("max_tokens").or_insert(max_comp);
            }
        }
    }

    Bytes::from(serde_json::to_vec(&val).unwrap_or(base))
}

/// Sanitize JSON bytes: strip trailing commas, BOM, and re-serialize cleanly.
/// Used for zhipu-anthropic which has a strict JSON parser that rejects trailing commas.
fn sanitize_json_bytes(bytes: &[u8]) -> Vec<u8> {
    // Fast path: already valid JSON, just re-serialize for clean output
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) {
        return serde_json::to_vec(&val).unwrap_or_else(|_| bytes.to_vec());
    }

    // Slow path: strip trailing commas then retry
    let mut cleaned = Vec::with_capacity(bytes.len());
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            cleaned.push(b);
            if b == b'\\' && i + 1 < bytes.len() {
                i += 1;
                cleaned.push(bytes[i]);
            } else if b == b'\"' {
                in_string = false;
            }
        } else {
            match b {
                b'\"' => {
                    in_string = true;
                    cleaned.push(b);
                }
                b',' => {
                    // Look ahead: skip comma if followed by } or ]
                    let mut j = i + 1;
                    while j < bytes.len() && matches!(bytes[j], b' ' | b'\t' | b'\n' | b'\r') {
                        j += 1;
                    }
                    if j < bytes.len() && (bytes[j] == b'}' || bytes[j] == b']') {
                        // Trailing comma: skip it
                    } else {
                        cleaned.push(b);
                    }
                }
                _ => cleaned.push(b),
            }
        }
        i += 1;
    }

    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&cleaned) {
        serde_json::to_vec(&val).unwrap_or(cleaned)
    } else {
        cleaned
    }
}
