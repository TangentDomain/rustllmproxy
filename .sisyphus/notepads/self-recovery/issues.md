## 2026-05-11 Risks
- 需要确保新增 /livez 不经 auth（否则 hang 时无法探测）。
- to_bytes timeout 可能改变边界行为：客户端慢发 body 会更早 408，需要可配置。
- RuntimeHealth 写路径必须低开销：Atomic/parking_lot，避免跨 await 持锁。
- 需要保证不会在热路径引入 blocking fs I/O。

- Windows 下偶发 LINK LNK1285：PDB 文件损坏（target\\debug\\deps\\*.pdb）。处理方式：执行 cargo clean 后重建即可。

- 2026-05-11: Windows 下 cargo test 期间遇到多次 LNK1285 PDB 损坏；先 cargo clean 后重跑可恢复。
- 2026-05-11: tests/streaming.rs 原有测试块末尾出现括号错位，补新用例后需要重新检查文件末尾闭合。
- 2026-05-11 Task5: 当前实现需要重点复核 binary 入口是否真正使用 server.port 绑定监听，以及 watchdog hook 是否拿到与 `/livez` 相同的 RuntimeHealth；这两点一旦偏离，功能会“看起来有代码、实际上无效”。
- 2026-05-11 Task5: `bind_listener` 若保持私有，binary 入口无法安全复用现有 socket 配置；要么公开它，要么提供新的 public 启动 API。
- 2026-05-11 Task5: `clippy -D warnings` 会抓未使用 import/变量，watchdog/binary 改动后必须先清理编译噪音再跑全量验证。
