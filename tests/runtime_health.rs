use llmproxy::runtime_health::{Decision, RuntimeHealth, RuntimeHealthSnapshot, decide};

#[test]
fn inflight_request_decrement_saturates_at_zero() {
    let health = RuntimeHealth::new();

    health.inflight_requests_dec();
    let snapshot = health.snapshot(1);
    assert_eq!(snapshot.inflight_requests, 0);
    assert_eq!(snapshot.oldest_inflight_ms, None);

    health.request_started(123);
    health.request_finished();
    health.request_finished();
    let snapshot = health.snapshot(2);
    assert_eq!(snapshot.inflight_requests, 0);
    assert_eq!(snapshot.oldest_inflight_ms, None);
}

#[test]
fn inflight_stream_and_body_read_decrement_saturate_at_zero() {
    let health = RuntimeHealth::new();

    health.inflight_streams_dec();
    health.body_read_inflight_dec();
    health.body_read_finished(10);
    let snapshot = health.snapshot(10);
    assert_eq!(snapshot.inflight_streams, 0);
    assert_eq!(snapshot.body_read_inflight, 0);
    assert_eq!(snapshot.last_body_read_done_ms, Some(10));

    health.inflight_streams_inc();
    health.inflight_streams_dec();
    health.inflight_streams_dec();
    health.body_read_started(20);
    health.body_read_finished(21);
    health.body_read_finished(22);
    let snapshot = health.snapshot(22);
    assert_eq!(snapshot.inflight_streams, 0);
    assert_eq!(snapshot.body_read_inflight, 0);
    assert_eq!(snapshot.last_body_read_start_ms, Some(20));
    assert_eq!(snapshot.last_body_read_done_ms, Some(22));
}

#[test]
fn decide_live_when_ticks_recent() {
    let now = 1_000_000u64;
    let snap = RuntimeHealthSnapshot {
        now_ms: now,
        last_runtime_tick_ms: now - 500,
        last_http_seen_ms: now - 500,
        inflight_requests: 0,
        inflight_streams: 0,
        body_read_inflight: 0,
        oldest_inflight_ms: None,
        last_body_read_start_ms: None,
        last_body_read_done_ms: None,
        last_error: None,
        last_effective_stream_chunk_ms: None,
        last_metrics_flush_ok_ms: None,
    };
    let out = decide(&snap);
    assert_eq!(out.decision, Decision::Live);
}

#[test]
fn decide_stalled_when_runtime_tick_old() {
    let now = 1_000_000u64;
    let snap = RuntimeHealthSnapshot {
        now_ms: now,
        last_runtime_tick_ms: now - 10_000,
        last_http_seen_ms: now - 500,
        inflight_requests: 0,
        inflight_streams: 0,
        body_read_inflight: 0,
        oldest_inflight_ms: None,
        last_body_read_start_ms: None,
        last_body_read_done_ms: None,
        last_error: None,
        last_effective_stream_chunk_ms: None,
        last_metrics_flush_ok_ms: None,
    };
    let out = decide(&snap);
    assert_eq!(out.decision, Decision::Stalled);
    assert!(out.reason.contains("runtime"));
}