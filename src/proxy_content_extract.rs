
/// Extract text from all content fields (content, reasoning_content, thinking) in an SSE JSON chunk.
/// Longer patterns are matched first to avoid substring collisions (e.g. "content" inside "reasoning_content").
fn extract_content_text(json: &str) -> Option<String> {
    const FIELDS: &[&[u8]] = &[
        b"\"reasoning_content\":\"",  // must precede "content":"
        b"\"content\":\"",
        b"\"thinking\":\"",
    ];
    let bytes = json.as_bytes();
    let mut result = String::new();
    let mut skip_until = 0usize;
    for pat in FIELDS {
        for i in skip_until..bytes.len().saturating_sub(pat.len()) {
            if &bytes[i..i + pat.len()] == *pat {
                let mut j = i + pat.len();
                while j < bytes.len() {
                    let b = bytes[j];
                    if b == b'\\' && j + 1 < bytes.len() {
                        let next = bytes[j + 1];
                        match next {
                            b'"' | b'\\' | b'/' => result.push(next as char),
                            b'n' => result.push('\n'),
                            b't' => result.push('\t'),
                            b'r' => result.push('\r'),
                            _ => { result.push(b as char); result.push(next as char); }
                        }
                        j += 2;
                    } else if b == b'"' {
                        skip_until = j + 1;
                        break;
                    } else {
                        result.push(b as char);
                        j += 1;
                    }
                }
                break; // only match first occurrence per pattern
            }
        }
    }
    if result.is_empty() { None } else { Some(result) }
}
