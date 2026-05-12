use llmproxy::runtime_health::{RuntimeHealth, RuntimeHealthSnapshot};
use llmproxy::watchdog::{decide_watchdog_action, WatchdogAction};

fn snap(now_ms: u64, last_tick_ms: u64) -> RuntimeHealthSnapshot {
    RuntimeHealthSnapshot {
        now_ms,
        last_runtime_tick_ms: last_tick_ms,
        last_http_seen_ms: 0,
        inflight_requests: 0,
        inflight_streams: 0,
        body_read_inflight: 0,
        oldest_inflight_ms: None,
        last_body_read_start_ms: None,
        last_body_read_done_ms: None,
        last_error: None,
        last_effective_stream_chunk_ms: None,
        last_metrics_flush_ok_ms: None,
    }
}

#[test]
fn watchdog_decision_live_is_none() {
    let s = snap(10_000, 9_900);
    let action = decide_watchdog_action(&s, 5_000, None, 120_000);
    assert_eq!(action, WatchdogAction::None);
}

#[test]
fn watchdog_decision_stalled_is_reexec() {
    let s = snap(10_000, 1);
    let action = decide_watchdog_action(&s, 5_000, None, 120_000);
    assert_eq!(action, WatchdogAction::ReExec);
}

#[test]
fn watchdog_decision_cooldown_blocks() {
    let s = snap(200_000, 1);
    let action = decide_watchdog_action(&s, 5_000, Some(150_000), 120_000);
    assert_eq!(action, WatchdogAction::None);
}

#[test]
fn watchdog_decision_cooldown_elapsed_allows() {
    let s = snap(300_000, 1);
    let action = decide_watchdog_action(&s, 5_000, Some(150_000), 120_000);
    assert_eq!(action, WatchdogAction::ReExec);
}

#[test]
fn watchdog_spawn_failure_respects_cooldown_state_machine() {
    // 不跑真实 watchdog_loop（会阻塞/exit），只模拟状态机边界。
    let rh = RuntimeHealth::new();

    // 模拟：tick 不更新 => stalled
    rh.tick_runtime(0);

    let mut last_attempt: Option<u64> = None;
    let stall_ms = 5_000;
    let cooldown_ms = 120_000;

    // 第一次：应该尝试
    let s1 = rh.snapshot(200_000);
    assert_eq!(
        decide_watchdog_action(&s1, stall_ms, last_attempt, cooldown_ms),
        WatchdogAction::ReExec
    );
    // 记录 attempt（等价于 watchdog_loop 的“先记录再 spawn”）
    last_attempt = Some(s1.now_ms);

    // cooldown 内：不应再次尝试
    let s2 = rh.snapshot(250_000);
    assert_eq!(
        decide_watchdog_action(&s2, stall_ms, last_attempt, cooldown_ms),
        WatchdogAction::None
    );

    // cooldown 过后：允许再次尝试
    let s3 = rh.snapshot(400_001);
    assert_eq!(
        decide_watchdog_action(&s3, stall_ms, last_attempt, cooldown_ms),
        WatchdogAction::ReExec
    );
}
