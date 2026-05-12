
## 2026-05-11 Task3（Ingress + body read tracking + to_bytes timeout）
- 新增配置 `server.body_read_timeout_secs`（默认 30s），用于对 `axum::body::to_bytes(...)` 包一层 `tokio::time::timeout`，避免慢速上传导致热路径卡死。
- RuntimeHealth 扩展：
  - request ingress：`request_started(now_ms)` / `request_finished()` 维护 `inflight_requests` 与 `oldest_inflight_ms`（0 表示 None）。
  - body read：`body_read_started(now_ms)` / `body_read_finished(now_ms)` 维护 `body_read_inflight`、`last_body_read_start_ms`、`last_body_read_done_ms`。
  - 错误记录：`record_error(RuntimeHealthError::BodyReadTimeout, now_ms)`；快照输出 `last_error: { code, at_ms }`。
- Proxy 热路径实现要点：
  - 在 `Proxy::handle` 内尽量靠前打 ingress 点，并使用 Drop guard 确保请求结束时必定 decrement（避免 early return 漏减）。
  - body read timeout 返回 `408 Request Timeout`，同时确保 body_read_inflight 状态清理与 last_error 写入。
- 测试技巧：用 `reqwest::Body::wrap_stream` + 慢速 `Stream<Item=Result<Bytes, Infallible>>` 构造“极慢上传”，可稳定触发 body read timeout；随后调用 `/livez` 断言 `last_error.code == body_read_timeout` 且 `body_read_inflight == 0`。

- ServerConfig 新增 body_read_timeout_secs(默认30s) 后，所有手写 struct literal 初始化点（含 tests）都必须补齐该字段，否则触发 E0063。
- RuntimeHealthSnapshot 新增 last_body_read_start_ms/last_body_read_done_ms 后，tests/runtime_health.rs 这类手工构造快照的测试也需补齐字段（用 None 即可）。

- 2026-05-11: Stream instrumentation should把 inflight_streams 的增减放在 spawn/Drop 保护里，避免 success/timeout/error 分支漏减。
- 2026-05-11: effective SSE chunk 的判定需与 idle timeout 口径一致；role-only/keepalive 不应刷新 last_effective_stream_chunk_ms。
- 2026-05-11: metrics maintenance 的 flush 成功点可以直接复用 spawn_blocking 结束后的成功分支，顺手 tick last_metrics_flush_ok_ms，/livez 只读 Atomic 快照即可。


## 2026-05-11 Task4 review blockers 修复笔记
- AtomicU32 计数器的 decrement 不能直接 `fetch_sub(1)`：一旦出现漏配对/重复 Drop/异常路径，多线程下会 u32 下溢到巨大值，/livez 会被污染且难以定位。
  - 做法：为 RuntimeHealth 统一提供 CAS 饱和减法（0 保持 0），所有 decrement 路径（request_finished / inflight_*_dec / body_read_*_dec/finished）全部改用该 helper。
- SSE "有效 chunk" 判定必须和内容抽取语义一致：如果 `append_content_text()` 会把 `thinking` 计入内容指标，那 `is_effective_sse_chunk()` 也必须把 `"thinking":"..."` 视为有效活动，否则 idle timeout/活性打点会出现“有内容但不刷新活性”的矛盾。
- stream 类集成测试不要断言 metrics maintenance tick 的“立刻发生”：后台任务时序不确定，应该用轮询等待（带上限）或让该字段由其它测试覆盖，避免 flaky。

## 2026-05-11 Task5（OS-thread watchdog + self re-exec）
- watchdog 必须跑在 `std::thread`，不能依赖 Tokio；否则 runtime 自己卡住时 watchdog 也会一起卡死。
- `/livez` 与 watchdog 必须读取同一个 `RuntimeHealth` 实例；如果在 binary 入口重新 `RuntimeHealth::new()`，会造成探针和自恢复看到的是两套世界。
- 自恢复动作边界要做成可注入：纯决策测试只断言 `ReExec/None`，不要在测试里真的 spawn 生产代理进程。
- spawn 失败也要先写入 restart attempt 时间，再进入 cooldown；否则 spawn 失败会 tight-loop 拉起/刷日志。
- Windows PDB 损坏（LNK1285）是偶发问题，`cargo clean` 后重编可恢复；不影响代码正确性。
- watchdog 模块内测试不应使用 `mut` 绑定只需读取的闭包，clippy `-D warnings` 会抓。
- `drop()` 对 Copy 类型闭包无效，应使用 `let _ = ...` 抑制 clippy `dropping_copy_types` 警告。