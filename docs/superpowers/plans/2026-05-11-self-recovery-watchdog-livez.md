# Self-Recovery (Watchdog + Livez) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `unified-proxy` 内实现“无需外部调用”的自诊断与自恢复：RuntimeHealth 观测 + `/livez` 活性快照 + body read timeout + 独立 OS 线程 watchdog 触发 self re-exec。

**Architecture:** 在热路径用 Atomic 维护 `RuntimeHealth`（低开销、无 blocking I/O）。Tokio 内部维护 tick 与请求生命周期打点；另起 `std::thread` watchdog 定期读取状态，判定 stall 后先 `spawn` 新进程（同 exe + 同 config 参数），再触发旧进程优雅退出（超时则 `std::process::exit`）。

**Tech Stack:** Rust, Tokio, Axum, tracing, serde_json。

---

## File / Module Plan

**Create**
- `src/runtime_health.rs`：RuntimeHealth 数据结构、原子写入接口、快照与判定逻辑（live/suspect/stalled）。

**Modify**
- `src/proxy.rs`：
  - Router 新增 `GET /livez`（不经 auth）
  - 在最外层加 ingress 打点 middleware（或 layer）
  - 在 `Proxy::handle` 中对 `to_bytes()` 增加 timeout + 打点
  - 记录 inflight / body_read_inflight
  - 在 metrics maintenance task 成功 flush 后打点
- `src/streaming.rs`：在“有效 chunk”与 timeout 分支更新 streaming 打点（last_effective_chunk、inflight_streams）。
- `src/bin/unified.rs`（或 `src/binlib/unified_routes.rs`）：
  - 解析并保留 config path / argv（用于 self re-exec）
  - 启动 watchdog 线程（仅生产 binary）
- `src/config.rs`：新增自恢复相关配置（带默认值），例如：
  - `server.body_read_timeout_secs`（默认 30s）
  - `server.watchdog_enabled`（默认 true）
  - `server.watchdog_check_interval_ms`（默认 2000）
  - `server.watchdog_runtime_tick_stall_ms`（默认 5000）
  - `server.watchdog_restart_cooldown_secs`（默认 120）

**Tests**
- `tests/runtime_health.rs`（新建）：纯 Rust 单元/集成测试（不需要网络）验证判定逻辑。
- `tests/protocol_routes.rs`（扩展）：验证 `/livez` 不需要 auth、且在未 ready 时仍可访问。
- `tests/request_edge_cases.rs`（扩展或新建 `tests/body_read_timeout.rs`）：用慢 body 触发 `to_bytes` timeout（验证返回码 + RuntimeHealth 状态变化）。

---

## Task 1: Implement RuntimeHealth core

**Files:**
- Create: `src/runtime_health.rs`
- Test: `tests/runtime_health.rs`

- [ ] **Step 1: Write failing tests for decision logic**

Create `tests/runtime_health.rs`:
```rust
use llmproxy::runtime_health::{Decision, RuntimeHealthSnapshot, decide};

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
        last_error: None,
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
        last_error: None,
    };
    let out = decide(&snap);
    assert_eq!(out.decision, Decision::Stalled);
    assert!(out.reason.contains("runtime"));
}
```

- [ ] **Step 2: Run test to ensure it fails**

Run: `cargo test --test runtime_health -- --nocapture`
Expected: compile error (module missing)

- [ ] **Step 3: Implement RuntimeHealth minimal API**

Create `src/runtime_health.rs` with:
- `RuntimeHealth` (Arc-shared) with AtomicU64/AtomicU32
- `RuntimeHealthSnapshot` (plain struct, serde Serialize)
- `Decision` enum + `DecisionOutput { decision, reason }`
- `snapshot()` + `decide(snapshot)`

- [ ] **Step 4: Run tests; ensure PASS**

Run: `cargo test --test runtime_health -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/runtime_health.rs tests/runtime_health.rs
git commit -m "feat: add runtime health snapshot and decision" -m "Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-openagent)"
```

---

## Task 2: Add /livez endpoint (unauth) + runtime tick

**Files:**
- Modify: `src/proxy.rs`
- Test: `tests/protocol_routes.rs`

- [ ] **Step 1: Add /livez route to public router**
- [ ] **Step 2: Add tokio runtime tick task**
  - `tokio::spawn` loop 每 1s 更新 `last_runtime_tick_ms`
- [ ] **Step 3: Add test ensuring /livez is public**

Example test snippet to add in `tests/protocol_routes.rs`:
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn livez_is_public() {
    let config = TestConfigBuilder::new().build();
    let (proxy_addr, proxy_handle) = spawn_proxy(config).await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("http://{proxy_addr}/livez")).send().await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);

    proxy_handle.abort();
}
```

- [ ] **Step 4: Verify**
Run: `cargo test --test protocol_routes -- --nocapture`

- [ ] **Step 5: Commit**
```bash
git add src/proxy.rs tests/protocol_routes.rs
git commit -m "feat: expose unauthenticated /livez endpoint" -m "Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-openagent)"
```

---

## Task 3: Ingress + body read tracking + to_bytes timeout

**Files:**
- Modify: `src/proxy.rs`
- Modify: `src/config.rs`
- Test: `tests/body_read_timeout.rs` (new) or extend `tests/request_edge_cases.rs`

- [x] **Step 1: Add config fields with defaults**
Add fields to `ServerConfig`:
- `body_read_timeout_secs: u64` default 30

- [x] **Step 2: Add ingress middleware**
- On request start: bump `inflight_requests`, update `last_http_seen_ms`.
- On drop/response: decrement `inflight_requests`.

- [x] **Step 3: Wrap `to_bytes()` with timeout**
- Before to_bytes: set `body_read_inflight += 1`, update `last_body_read_start_ms`
- After success: `body_read_inflight -= 1`, update `last_body_read_done_ms`
- On timeout: `body_read_inflight -= 1`, set `last_error=body_read_timeout`, return `408`.

- [x] **Step 4: Add test for slow body timeout**
Use an axum route and a custom request body stream that yields bytes slowly, then assert proxy returns 408 within expected window.

- [ ] **Step 5: Commit**
```bash
git add src/proxy.rs src/config.rs tests/body_read_timeout.rs
git commit -m "fix: add body read timeout and ingress health ticks" -m "Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-openagent)"
```

---

## Task 4: Stream + metrics ticks

**Files:**
- Modify: `src/streaming.rs`
- Modify: `src/proxy.rs`
- Test: extend `tests/stream_timeout.rs` (optional) and/or add `tests/livez_streaming.rs`

- [x] **Step 1: Track inflight_streams**
- stream start: `inflight_streams += 1`
- done (success/timeout/error): `inflight_streams -= 1` (saturating)

- [x] **Step 2: Track last_effective_stream_chunk_ms**
- On detecting effective chunk (content/token/thinking), update timestamp.
- `is_effective_sse_chunk()` 与 `append_content_text()` 语义对齐。

- [x] **Step 3: Metrics maintenance tick**
- After successful flush/evict, update `last_metrics_flush_ok_ms`.

- [x] **Step 4: Commit**

---

## Task 5: OS-thread watchdog + self re-exec

**Files:**
- Modify: `src/bin/unified.rs` (or `src/binlib/unified_routes.rs` if needed)
- Modify: `src/config.rs`
- Test: `tests/watchdog_reexec.rs` (new; minimal)

- [x] **Step 1: Add watchdog config defaults**
`ServerConfig` add:
- `watchdog_enabled: bool` default true
- `watchdog_check_interval_ms: u64` default 2000
- `watchdog_runtime_tick_stall_ms: u64` default 5000
- `watchdog_restart_cooldown_secs: u64` default 120

- [x] **Step 2: Capture argv / config path for re-exec**
In `src/bin/unified.rs`, record executable path + args via `current_exe()` + `args_os().skip(1)`.

- [x] **Step 3: Start watchdog thread**
New `src/watchdog.rs` module with injectable `RestartAction`:
- Pure decision function `decide_watchdog_action()`
- `start_watchdog_thread()` on OS thread (`std::thread`)
- Production re-exec: `Command::new(exe).args(args).spawn()` + `std::process::exit(66)`
- Spawn failure respects cooldown, no tight-loop.
- `on_proxy_ready` hook injects `Arc<RuntimeHealth>` into watchdog.

- [x] **Step 4: Minimal integration test**
`tests/watchdog_reexec.rs` (5 tests): disabled/live/stalled/cooldown/spawn-failure.
- Uses fake restart action, no real process duplication.
- `configs/unified-dev.toml` + `configs/mock-test.toml` set `watchdog_enabled = false`.

- [x] **Step 5: Commit**

---

## Task 6: Final verification

- [x] Run: `cargo test` → 168 passed, 0 failed
- [x] Run: `cargo clippy --all-targets --all-features -- -D warnings` → 0 warnings
- [x] Run: `cargo build --release` → success

---

## Notes / Safety
- `/livez` 必须 public（不经 auth），否则 hang 时无法探测。
- Watchdog 不得按镜像名 kill；本方案不 kill，只做 self re-exec + 旧进程 exit。
- 防双实例：可选用 lockfile（后续增强），第一版至少要有 cooldown。
