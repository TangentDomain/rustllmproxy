use axum::body::Body;
use axum::http::Response;

pub fn inject_stream_response_headers(
    resp: &mut Response<Body>,
    request_id: &str,
    backend_name: &str,
    ttfb_ms: u64,
) {
    let headers = resp.headers_mut();
    headers.insert("x-accel-buffering", "no".parse().unwrap());
    headers.insert("cache-control", "no-cache".parse().unwrap());
    headers.insert("x-request-id", request_id.parse().unwrap());
    headers.insert("x-backend", backend_name.parse().unwrap());
    headers.insert("x-ttfb-ms", ttfb_ms.to_string().parse().unwrap());
}

pub fn inject_non_stream_response_headers(
    resp: &mut Response<Body>,
    request_id: &str,
    backend_name: &str,
    ttfb_ms: u64,
    total_ms: u64,
) {
    let headers = resp.headers_mut();
    headers.insert("x-request-id", request_id.parse().unwrap());
    headers.insert("x-backend", backend_name.parse().unwrap());
    headers.insert("x-ttfb-ms", ttfb_ms.to_string().parse().unwrap());
    headers.insert("x-total-ms", total_ms.to_string().parse().unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Response;

    #[test]
    fn inject_stream_response_headers_sets_expected_fields() {
        let mut resp = Response::new(Body::empty());
        inject_stream_response_headers(&mut resp, "req-1", "backend-a", 123);

        let headers = resp.headers();
        assert_eq!(headers.get("x-accel-buffering").and_then(|v| v.to_str().ok()), Some("no"));
        assert_eq!(headers.get("cache-control").and_then(|v| v.to_str().ok()), Some("no-cache"));
        assert_eq!(headers.get("x-request-id").and_then(|v| v.to_str().ok()), Some("req-1"));
        assert_eq!(headers.get("x-backend").and_then(|v| v.to_str().ok()), Some("backend-a"));
        assert_eq!(headers.get("x-ttfb-ms").and_then(|v| v.to_str().ok()), Some("123"));
    }

    #[test]
    fn inject_non_stream_response_headers_sets_expected_fields() {
        let mut resp = Response::new(Body::empty());
        inject_non_stream_response_headers(&mut resp, "req-2", "backend-b", 45, 678);

        let headers = resp.headers();
        assert_eq!(headers.get("x-request-id").and_then(|v| v.to_str().ok()), Some("req-2"));
        assert_eq!(headers.get("x-backend").and_then(|v| v.to_str().ok()), Some("backend-b"));
        assert_eq!(headers.get("x-ttfb-ms").and_then(|v| v.to_str().ok()), Some("45"));
        assert_eq!(headers.get("x-total-ms").and_then(|v| v.to_str().ok()), Some("678"));
    }
}
