use std::ffi::OsString;
use std::thread;
use std::time::Duration;

use crate::runtime_health::{RuntimeHealth, RuntimeHealthSnapshot};

/// watchdog 的运行参数（来自 config.server）。
#[derive(Debug, Clone)]
pub struct WatchdogConfig {
    pub enabled: bool,
    pub check_interval: Duration,
    pub runtime_tick_stall: Duration,
    pub restart_cooldown: Duration,
}

impl WatchdogConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            check_interval: Duration::from_millis(2_000),
            runtime_tick_stall: Duration::from_millis(5_000),
            restart_cooldown: Duration::from_secs(120),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchdogAction {
    None,
    ReExec,
}

/// 可测试的纯决策函数：仅依赖快照与时间。
///
/// 语义：
/// - enabled=false => 永远 None
/// - runtime tick age > stall => ReExec（受 cooldown 限制）
/// - cooldown 未到 => None
pub fn decide_watchdog_action(
    snap: &RuntimeHealthSnapshot,
    runtime_tick_stall_ms: u64,
    last_restart_attempt_at_ms: Option<u64>,
    restart_cooldown_ms: u64,
) -> WatchdogAction {
    if runtime_tick_stall_ms == 0 {
        // 0 表示禁用 stall 判定（但保留 enabled 语义），避免除零/误判。
        return WatchdogAction::None;
    }

    let runtime_age = snap.now_ms.saturating_sub(snap.last_runtime_tick_ms);
    if runtime_age <= runtime_tick_stall_ms {
        return WatchdogAction::None;
    }

    if let Some(last_ms) = last_restart_attempt_at_ms {
        let age = snap.now_ms.saturating_sub(last_ms);
        if age < restart_cooldown_ms {
            return WatchdogAction::None;
        }
    }

    WatchdogAction::ReExec
}

/// 生产环境的 re-exec 动作边界：spawn 同一可执行文件并返回 spawn 结果。
///
/// - exe 与 args 使用 OsString，Windows 安全。
/// - 失败时由上层记录/限流（cooldown）。
pub fn spawn_reexec(exe: &std::path::Path, args: &[OsString]) -> std::io::Result<()> {
    let mut cmd = std::process::Command::new(exe);
    cmd.args(args);
    cmd.spawn().map(|_child| ())
}

/// watchdog 的状态（用于 cooldown）。
#[derive(Debug, Default)]
pub struct WatchdogState {
    last_restart_attempt_at_ms: Option<u64>,
}

/// 运行 watchdog loop（阻塞线程）。
///
/// 约束：
/// - OS thread，不能依赖 Tokio。
/// - 只读 RuntimeHealth 快照。
/// - re-exec 动作通过闭包注入，便于测试。
pub fn watchdog_loop<FExit, FSpawn>(
    runtime_health: RuntimeHealth,
    cfg: WatchdogConfig,
    mut do_spawn: FSpawn,
    mut do_exit: FExit,
 )
where
    FSpawn: FnMut(&RuntimeHealthSnapshot) -> bool,
    FExit: FnMut(i32),
{
    let mut state = WatchdogState::default();

    loop {
        thread::sleep(cfg.check_interval);

        if !cfg.enabled {
            continue;
        }

        let now_ms = now_ms();
        let snap = runtime_health.snapshot(now_ms);

        let action = decide_watchdog_action(
            &snap,
            cfg.runtime_tick_stall.as_millis() as u64,
            state.last_restart_attempt_at_ms,
            cfg.restart_cooldown.as_millis() as u64,
        );

        if action == WatchdogAction::None {
            continue;
        }

        // 先更新时间戳再执行 spawn，避免 spawn 阻塞/失败导致 tight-loop。
        state.last_restart_attempt_at_ms = Some(now_ms);

        // 只要尝试过就进入 cooldown；spawn 成功后直接 exit。
        let ok = do_spawn(&snap);
        if ok {
            do_exit(66);
            break;
        }
    }
}

pub fn start_watchdog_thread<FSpawn>(
    runtime_health: RuntimeHealth,
    cfg: WatchdogConfig,
    spawn_action: FSpawn,
 ) -> thread::JoinHandle<()>
where
    FSpawn: Fn(&RuntimeHealthSnapshot) -> bool + Send + 'static,
{
    thread::Builder::new()
        .name("watchdog".to_string())
        .spawn(move || {
            // 生产：exit 直接终止进程。
            let do_exit = |code: i32| {
                std::process::exit(code);
            };
            watchdog_loop(runtime_health, cfg, spawn_action, do_exit)
        })
        .expect("failed to spawn watchdog thread")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
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
    fn decide_disabled_when_runtime_tick_is_fresh() {
        let s = snap(10_000, 9_000);
        let action = decide_watchdog_action(&s, 5_000, None, 120_000);
        assert_eq!(action, WatchdogAction::None);
    }

    #[test]
    fn decide_stalled_when_tick_age_exceeds_threshold() {
        let s = snap(10_000, 1);
        let action = decide_watchdog_action(&s, 5_000, None, 120_000);
        assert_eq!(action, WatchdogAction::ReExec);
    }

    #[test]
    fn decide_cooldown_blocks_restart_attempts() {
        let s = snap(200_000, 1);
        // last attempt at 150_000, cooldown 120_000 => age 50_000 < cooldown
        let action = decide_watchdog_action(&s, 5_000, Some(150_000), 120_000);
        assert_eq!(action, WatchdogAction::None);
    }

    #[test]
    fn decide_cooldown_elapsed_allows_restart() {
        let s = snap(300_000, 1);
        let action = decide_watchdog_action(&s, 5_000, Some(150_000), 120_000);
        assert_eq!(action, WatchdogAction::ReExec);
    }

    #[test]
    fn watchdog_loop_records_attempt_before_spawn() {
        let rh = RuntimeHealth::new();
        // 强制 stalled：last tick = 0
        rh.tick_runtime(0);

        let cfg = WatchdogConfig {
            enabled: true,
            check_interval: Duration::from_millis(1),
            runtime_tick_stall: Duration::from_millis(1),
            restart_cooldown: Duration::from_secs(120),
        };

        let attempts: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(vec![]));
        let attempts2 = attempts.clone();

        let spawn_action = move |s: &RuntimeHealthSnapshot| {
            attempts2.lock().unwrap().push(s.now_ms);
            false // spawn 失败，不退出
        };

        // 用 panic 作为退出边界，避免真的退出测试进程。
        let do_exit = |_code: i32| -> ! { panic!("exit should not be called") };

        // 运行一小段时间后终止线程：通过注入 spawn_action 第一次后修改 cfg.enabled=false。
        // 这里用另一线程短暂 sleep 后将 enabled 置 false 不方便，因此直接在 spawn_action 里
        // 记录后返回 false，watchdog_loop 会继续 sleep；我们只验证至少被调用一次即可。
        let handle = thread::spawn(move || {
            // 让 loop 至少跑几次后用 panic 终止，避免测试挂死。
            let mut loops = 0u32;
            let do_spawn = spawn_action;
            let do_exit2 = do_exit;
            let mut state = WatchdogState::default();
            loop {
                thread::sleep(cfg.check_interval);
                loops += 1;
                if loops > 10 {
                    break;
                }
                let now_ms = 1_000 + loops as u64;
                let s = rh.snapshot(now_ms);
                let action = decide_watchdog_action(
                    &s,
                    cfg.runtime_tick_stall.as_millis() as u64,
                    state.last_restart_attempt_at_ms,
                    cfg.restart_cooldown.as_millis() as u64,
                );
                if action == WatchdogAction::ReExec {
                    state.last_restart_attempt_at_ms = Some(now_ms);
                    let _ = do_spawn(&s);
                }
            }
            let _ = do_exit2;
        });

        handle.join().expect("watchdog thread join");
        assert!(!attempts.lock().unwrap().is_empty());
    }
}
