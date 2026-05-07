# AGENTS.md

本文件记录未来 OpenCode agent 容易踩坑、且已从本仓库验证过的高信号规则。泛化 Rust 建议不要写在这里。

## 生产安全

- **不要执行** `taskkill /F /IM unified-proxy.exe`；它会按镜像名杀掉生产实例，生产端口 `8090` 承载真实流量。
- 开发测试只操作 dev 实例：`unified-proxy-dev.exe` / 端口 `8091` / `configs/unified-dev.toml` / `logs-dev/`。
- 如果必须停止进程，优先停止 dev：`taskkill /F /IM unified-proxy-dev.exe`；如需处理其他 `unified-proxy.exe`，先按 PID 确认路径，避免误杀 `run\unified-proxy.exe`。
- 启动脚本会把 `target\release\unified-proxy.exe` 复制到 `run\` 或 `run-dev\` 后再运行，目的是避免运行中的二进制锁住 `target\release` 影响重编译。

## 常用命令

- 完整验证：`cargo test` → `cargo clippy --all-targets --all-features -- -D warnings` → `cargo build --release`。
- 单个 integration test：`cargo test --test <name> -- --nocapture`，例如 `cargo test --test stream_timeout -- --nocapture`。
- 正式构建：`cargo build --release`。
- 启动 dev：优先在 Windows 下非阻塞启动 `run-dev\unified-proxy-dev.exe configs\unified-dev.toml`；批处理入口是 `start-unified-proxy-dev.bat`，但某些 runner 中 `cmd /c start` 可能表现为阻塞。
- 健康检查：`curl.exe -s -S --max-time 5 http://127.0.0.1:8091/health`。
- 覆盖率（Windows 优先）：首次运行先 `rustup component add llvm-tools-preview` 并安装 `cargo install cargo-llvm-cov`；常用报告命令 `cargo llvm-cov --all-targets --all-features --html`，不要优先选依赖 Linux ptrace 的 tarpaulin。

## 入口与配置

- Cargo binaries：`unified-proxy` → `src/bin/unified.rs`，`mock_llm_server` → `src/bin/mock_llm_server.rs`，`log_analyzer` → `src/bin/log_analyzer.rs`。
- `unified-proxy` 的第一个参数是 TOML 配置路径；不传则默认 `configs/unified.toml`。
- 配置文件中 `${VAR}` 会由 `Config::load` 展开；`.env.example` 只列出 `ZHIPU_API_KEY`、`MINIMAX_API_KEY_1`、`MINIMAX_API_KEY_2`。
- 主要配置端口：`configs/unified.toml` / `configs/unified-home.toml` 使用 `8090`，`configs/unified-dev.toml` 使用 `8091`，`configs/mock-test.toml` 使用 `8092`。
- prod 和 dev 共享根目录 `.env`；dev 只通过端口、配置和日志目录隔离，不是独立密钥环境。

## 请求流与协议细节

- 真实入口在 `src/bin/unified.rs`：按前缀路由 `/openai/v1/...`、`/anthropic/v1/...`，分别剥掉 `/openai` / `/anthropic`，并把协议字符串写入 request extensions。
- `src/proxy.rs::run_server_with_listener` 只让 `/health` 公开；`/backends`、模型列表和协议路由都经过 `auth_layer`。
- `Proxy::handle` 先从 JSON bytes 快速扫描 `model`，空或缺失直接 `400`，不会转发。
- Fallback 链：`Config::get_fallback_chain` 先用具体模型配置，再用 `fallback.default`，且始终把请求模型放在链首。
- Backend 选择不是稳定顺序：按健康后端和 tok/s 权重随机；测试不要假设第一个 backend 必然被选中，需用不同模型 + fallback chain 控制路径。
- OpenAI backend 用 `Authorization: Bearer <key>`；Anthropic backend 用 `x-api-key: <key>` 并强制 `anthropic-version: 2023-06-01`。
- BigModel OpenAI backend 会把转发路径中的 `/v1/` 改写为 `/v4/`；Anthropic 不改这个路径。
- `prepare_request_body` 可能按 backend 修改 body：模型替换、`strip_params`、`zhipu-anthropic` JSON sanitize、`minimax-anthropic*` 的 `max_completion_tokens` → `max_tokens`。
- 错误策略：`401` 和 `422` 直接返回；`429`、`5xx`、以及除 `401/422` 外的 `4xx` 会尝试下一个 backend / fallback model；全部耗尽返回 `503` JSON。
- `Backend.connect_timeout_secs` 可在配置里写，但当前 `Proxy::new` 的 reqwest client connect timeout 是硬编码 `3s`；不要误以为该字段已驱动 client connect timeout。
- `WeightedRoundRobin::start_health_check` 当前只是记录“健康检查已禁用”，后端健康主要由请求失败标记和 60s 被动恢复控制。

## Streaming 与指标

- 流式响应由 `instrument_stream` 包装，返回头包括 `x-accel-buffering: no`、`cache-control: no-cache`、`x-request-id`、`x-backend`、`x-ttfb-ms`。
- 流式超时有三类：首个有效 SSE chunk 超时 `stream_first_chunk_timeout_secs`、有效 chunk 间 idle 超时 `stream_idle_timeout_secs`、总时长超时使用 `server.timeout_secs`。
- role-only / keepalive 这类非有效内容不刷新 idle timeout；相关回归在 `tests/stream_timeout.rs`。
- 请求日志和 metrics 关注 `ttfb`、`ttft`、`total`、`tokens`、`tok/s`；`log_analyzer` 解析已有 proxy 日志并输出同名 `.md` 报告。

## 测试约定

- 新测试优先写 Rust integration tests，不要新增 Bash/Python/curl 脚本依赖；已有高价值 harness 在 `tests/common/mod.rs`。
- 使用 `TestConfigBuilder`、`spawn_mock`、`spawn_proxy` / `spawn_proxy_with_routes`，它们绑定 `127.0.0.1:0` 随机端口，避免占用 `8090/8091` 和消耗真实 token。
- 测试 API key 常量是 `tests/common/mod.rs::TEST_API_KEY`，值为 `sk-proxy-default`。
- 本仓库 `reqwest` 未启用 `json` feature；测试里不要用 `.json(...)`，用 `serde_json::to_string(...)` + `.body(...)`，响应用 `.text()` 后 `serde_json::from_str(...)`。
- `src/bin/mock_llm_server.rs` 可作为手动 mock backend，默认端口 `8766`，支持 `MOCK_PORT`、`MOCK_DELAY_MS`、`MOCK_ERROR_RATE`、`MOCK_RATE_LIMIT_RATE`。
- `configs/mock-test.toml` 是手动 mock 配置：proxy 端口 `8092`、auth disabled、backend 指向 `127.0.0.1:8766`。

## 代码风格与变更边界

- 文档和解释性注释默认中文；API 名、变量名、协议名保留英文。
- Bugfix 保持最小改动，不要顺手重构 fallback、streaming、auth 或 balancer 热路径。
- 不要用 `as any`、`@ts-ignore` 等类型压制习惯迁移到本仓库；Rust 侧也不要用无依据的 `unwrap` 扩大 panic 面，除非现有测试/启动代码已明确这样做。
