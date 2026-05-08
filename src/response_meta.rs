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
