## 2026-05-11 Kickoff
目标：实现进程内自诊断/自恢复基础设施（RuntimeHealth + /livez + ingress/body read 追踪 + body read timeout），用于定位“客户端持续发请求但服务无日志/看似无请求”的卡住问题。

范围：
- Phase 1：RuntimeHealth 状态与 /livez（只读快照）
- Phase 2：请求入口与 body read 活性追踪、为 to_bytes 加超时
- 不做（本次）：进程外 watchdog（但保留建议与接口契约）

注意：
- 不能用日志作为唯一健康信号；/livez 输出必须可用且轻量。
- 不能误伤 streaming 长请求；需要区分 inflight_streams 与 last_effective_stream_chunk。
- Windows 生产安全：不引入按镜像名 kill 的逻辑。

- 合并重复慢请求体超时测试：删除 tests/body_read_timeout.rs，保留 tests/request_edge_cases.rs::slow_body_triggers_body_read_timeout_and_updates_livez_snapshot，避免同语义重复覆盖。

- 合并重复慢请求体超时测试：删除 tests/body_read_timeout.rs，保留 tests/request_edge_cases.rs::slow_body_triggers_body_read_timeout_and_updates_livez_snapshot，避免同语义重复覆盖。

## 2026-05-11 Task5 决策
- 保持 watchdog 在 `src/watchdog.rs` 做薄模块：纯决策 + 可注入 re-exec action，避免把自恢复逻辑塞进 proxy 热路径。
- server 启动流程新增 `run_server_with_listener_and_hook(...)`：仅在 Proxy 构建完成后给 binary 一个 hook，测试 helper 继续走旧入口，默认不启动 watchdog。
- production re-exec 使用 `current_exe()` + `args_os().skip(1)` + `Command::new(exe).args(args).spawn()`；参数以 `OsString` 传递，兼容 Windows。
- dev 配置 `configs/unified-dev.toml` 与 mock 配置 `configs/mock-test.toml` 显式 `watchdog_enabled = false`，避免本地/测试惊群重启。
