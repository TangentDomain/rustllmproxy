# AGENTS.md

**Generated:** 2026-05-18 | **Commit:** 315fa0a | **Branch:** master

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
- `Backend.connect_timeout_secs` 会用于构建并选择按超时时间缓存的 `reqwest::Client`；只有没有任何 backend 时才会退回到默认 `3s` client。
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

## 闭环验证方法论

本项目采用闭环验证思想：不只验证最终响应，还要验证中间决策轨迹。对于同一请求与同一 mock 后端序列，期望产生一致的路由、fallback、health、metadata 决策。

### 核心公理
- 相同请求 + 相同 mock 后端响应序列，应产生相同的决策轨迹。
- 失败时优先定位 first divergent attempt，而不是只看最终 status/body。
- 核心验证优先使用 Rust integration tests + in-process mock backend，不依赖真实 API，不消耗真实 token。
- baseline 是代理语义公理，不是真实 provider 当前行为。

### 分层验证
- J1：纯函数 / 结构层。验证请求解析、配置、模型映射、token 校验等不依赖网络的逻辑。
- J2：代理行为层。验证 fallback、backend health、状态码语义、response metadata。
- J3：协议入口层。验证 OpenAI / Anthropic 路由边界和协议改写。
- J4：运行态层。验证 dev binary、mock 配置、健康检查和本地启动。

### 建议的诊断输出
- 记录 first divergent attempt、期望 backend、实际 backend、期望 status、实际 status。
- 记录是否触发 health penalty、是否 fallback、是否注入 proxy metadata headers。
- 对比应输出结构化差异，不要只给 `FAIL`。

### 反模式
- ❌ 只验证最终响应而忽略 fallback 中间步骤。
- ❌ 假设 backend 选择顺序稳定。
- ❌ 用真实 token 作为回归测试依赖。
- ❌ 用 Bash / Python / curl 脚本作为核心单元或集成测试依赖。
- ❌ 在没有确认语义变化是预期的情况下自动更新 baseline。

## ANTI-PATTERNS (THIS PROJECT)

### 生产安全红线
- ❌ **绝对禁止** `taskkill /F /IM unified-proxy.exe` — 按镜像名杀进程会误杀生产（8090）
- ❌ 只操作 dev 实例：`unified-proxy-dev.exe` / 8091 / `configs/unified-dev.toml`
- ❌ 不要假设 backend 选择顺序稳定（按健康后端和 tok/s 权重随机）

### 代码质量红线
- ❌ 不要用 `as any`、`@ts-ignore` 等类型压制
- ❌ 不要用无依据的 `unwrap` 扩大 panic 面（现有测试/启动代码除外）
- ❌ Bugfix 不要顺手重构 fallback、streaming、auth 或 balancer 热路径
- ❌ 避免 `unsafe` 块（当前 codebase 无 unsafe，应保持）
- ❌ 避免 `todo!` / `unimplemented!` / `FIXME` / `HACK` / `XXX`（当前 codebase 清洁）

### 测试反模式
- ❌ 只验证最终响应而忽略 fallback 中间步骤
- ❌ 用真实 token 作为回归测试依赖
- ❌ 用 Bash / Python / curl 脚本作为核心测试依赖
- ❌ 在没有确认语义变化是预期的情况下自动更新 baseline

### 热路径警告区
- `src/balancer.rs` 的 `balancer.select().unwrap()` — 选择器状态机，有 panic 风险
- `src/proxy.rs` 的 TCP socket 5 连 `expect()` — socket 创建/绑定链
- `src/streaming.rs` 的 `tiktoken_rs::cl100k_base().expect()` — tokenizer 初始化

## UNIQUE STYLES

- **binlib 分离模式**：二进制中的纯逻辑抽到 `src/binlib/` 供测试复用，入口只负责 CLI glue
- **北京时间日志**：自定义 `BeijingTime` formatter（`chrono::FixedOffset::east_opt(8 * 3600)`），不依赖系统时区
- **reqwest 无 json feature**：序列化用 `serde_json::to_string()` + `.body()`，反序列化用 `.text()` + `serde_json::from_str()`
- **随机端口测试**：所有测试绑定 `127.0.0.1:0`，通过 `listener.local_addr()` 获取分配端口
- **熔断被动恢复**：无主动健康检查探针，后端健康由请求失败标记 + 60s 冷却自动恢复
- **BigModel 路径改写**：智谱 OpenAI backend 转发时 `/v1/` → `/v4/`（coding API 要求）

## WHERE TO LOOK

| 任务 | 位置 | 备注 |
|------|------|------|
| 代理核心逻辑 | `src/proxy.rs` | fallback 链、请求转发、错误分类 |
| 负载均衡 + 熔断 | `src/balancer.rs` | WeightedRoundRobin + Circuit Breaker |
| 配置解析 | `src/config.rs` | TOML 解析、env 展开、fallback/模型映射 |
| 流式超时控制 | `src/streaming.rs` | instrument_stream、三层超时、token 计数 |
| 请求预处理 | `src/request_prep.rs` | 模型替换、strip_params、JSON sanitize |
| 认证限流 | `src/middleware.rs` | API Key + rate limit |
| 运行态健康 | `src/runtime_health.rs` | Atomic 指标，低开销 |
| 看门狗 | `src/watchdog.rs` | runtime stall 检测 + ReExec |
| 指标持久化 | `src/metrics.rs` | tok/s 滚动窗口、每日归档 |
| 协议路由构建 | `src/binlib/unified_routes.rs` | OpenAI/Anthropic 路由公共定义 |
| 测试基础设施 | `tests/common/mod.rs` | TestConfigBuilder、spawn_mock、spawn_proxy |
| Mock 后端 | `src/bin/mock_llm_server.rs` | 独立 binary，支持 MOCK_PORT 等环境变量 |
| 日志分析 | `src/bin/log_analyzer.rs` / `src/binlib/log_analyzer.rs` | 解析日志生成 Markdown 报告 |
