# Rust LLM Proxy

基于 Rust 的高性能 LLM 代理服务，支持多后端负载均衡和智能故障转移。

## ⚠️ CRITICAL: 生产环境保护

**切勿使用 `taskkill /F /IM unified-proxy.exe`** — 这会杀死所有实例（包括生产环境 8090 端口）。

- 生产端口 `8090`：承载真实流量
- 开发端口 `8091`：用于测试，独立环境
- 重启开发环境：`start-unified-proxy-dev.bat` 或 `taskkill /F /IM unified-proxy-dev.exe`

## 功能特性

- **统一代理**：单个进程同时支持 OpenAI 和 Anthropic API
- **多后端负载均衡**：加权轮询，支持多 key 负载均衡
- **智能故障转移**：模型级 fallback 链，429/5xx/4xx(除401) 自动降级
- **性能监控**：TTFB、TTFT、tokens/s 等实时指标
- **日志轮转**：按天自动轮转，异步写入不阻塞请求
- **认证限流**：API Key 认证 + 请求速率限制

## 快速开始

### 环境要求

- Rust 1.70+
- 复制 `.env.example` 为 `.env` 并配置 API keys

### 构建

```bash
cargo build --release
```

### 启动

```bash
# 生产环境（端口 8090）
start-unified-proxy.bat

# 开发环境（端口 8091，日志在 logs-dev/）
start-unified-proxy-dev.bat

# 停止开发环境
taskkill /F /IM unified-proxy-dev.exe
```

## API 路由

通过路径前缀区分协议：

| 协议 | 路径前缀 | 示例 |
|------|---------|------|
| OpenAI | `/openai/v1/...` | `/openai/v1/chat/completions` |
| Anthropic | `/anthropic/v1/...` | `/anthropic/v1/messages` |

### 管理端点

| 端点 | 认证 | 说明 |
|------|------|------|
| `GET /health` | 无 | 健康检查 |
| `GET /backends` | 需要认证 | 后端状态（名称、URL、健康状态、失败次数） |
| `GET /openai/v1/models` | 需要认证 | 可用模型列表（OpenAI 格式）|
| `GET /anthropic/v1/models` | 需要认证 | 可用模型列表（Anthropic 格式）|

## 配置说明

配置文件：`configs/unified.toml`

### 后端

```toml
[[backends]]
name = "zhipu-openai"
url = "https://open.bigmodel.cn/api/coding/paas"
api_key = "${ZHIPU_API_KEY}"   # 支持环境变量
weight = 10
models = ["glm-5.1", "glm-4.7"]
timeout_secs = 300
protocol = "openai"             # "openai" 或 "anthropic"
```

### Fallback 链

```toml
[fallback]
"glm-5.1" = ["glm-5v-turbo", "glm-4.7"]
default = ["glm-5.1", "glm-4.7"]
```

### 认证

```toml
[auth]
enabled = true

[[auth.keys]]
key = "sk-proxy-default"
name = "default"
rate_limit = 999
```

## 性能监控

每次请求自动记录指标到日志：

```
Request model=glm-5.1, path=/v1/messages, body=570996B
Responding: glm-5.1 -> glm-5.1 via zhipu-anthropic | ttfb=1109ms, stream=true
Done: glm-5.1 -> glm-5.1 via zhipu-anthropic | total=13789ms, ttfb=1109ms, ttft=13384ms, tokens=91, 33.6tok/s
```

| 指标 | 含义 |
|------|------|
| ttfb | Time To First Byte，后端首字节响应时间 |
| ttft | Time To First Token，第一个文本 token 输出时间 |
| total | 请求总耗时（含流式传输完成） |
| tokens | 输出 token 数量 |
| tok/s | 输出吞吐量 |

## 日志

- **位置**：`logs/` 目录
- **轮转**：按天自动轮转，文件名 `unified-proxy.log.YYYY-MM-DD`
- **异步写入**：不阻塞请求处理

## 故障转移策略

| 状态码 | 行为 |
|--------|------|
| 200 | 返回成功 |
| 401 | 直接返回（key 无效） |
| 429 | 切换下一个 key |
| 其他 4xx | fallback 到下一个后端 |
| 5xx | fallback 到下一个后端 |

## 使用示例

```bash
# OpenAI
curl http://localhost:8090/openai/v1/chat/completions \
  -H "Authorization: Bearer sk-proxy-default" \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-5.1","messages":[{"role":"user","content":"Hello"}]}'

# Anthropic
curl http://localhost:8090/anthropic/v1/messages \
  -H "Authorization: Bearer sk-proxy-default" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-5.1","max_tokens":1024,"messages":[{"role":"user","content":"Hello"}]}'
```

## 环境变量

在 `.env` 文件中配置 API keys，配置文件通过 `${VAR}` 引用：

```bash
ZHIPU_API_KEY=your_key
MINIMAX_API_KEY_1=your_key
```

## 架构设计

### 请求流

```
Request → unified.rs (按路径前缀路由: /openai/v1/* 或 /anthropic/v1/*)
  → auth_layer (API Key + 速率限制检查)
  → proxy::handle()
    → 从 JSON 快速提取 model 字段（限制 body 上限 10 MiB）
    → 获取该模型的 fallback 链
    → 遍历链中每个模型:
      → 筛选支持该模型的后端（按协议过滤）
      → 过滤掉熔断中的后端（Circuit Breaker）
      → WeightedRoundRobin::select() → 选出一个后端
      → prepare_request_body()（模型替换、参数裁剪、JSON sanitize）
      → do_forward() → 转发到后端 API
      → 遇到 429/5xx/4xx(除401)：继续尝试下一个后端
      → 遇到 200：instrument_stream() → 记录指标 → 返回
    → 所有后端耗尽 → 返回 503 JSON 错误
```

### 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| unified.rs | src/bin/unified.rs | 入口：路径前缀路由、协议标记 |
| Proxy | src/proxy.rs | 主代理逻辑：fallback 链、请求转发、错误处理 |
| Config | src/config.rs | TOML 配置解析、环境变量展开、fallback/模型映射 |
| WeightedRoundRobin | src/balancer.rs | 加权轮询负载均衡 + 熔断器 (Circuit Breaker) |
| Middleware | src/middleware.rs | API Key 认证 + 速率限制 |
| MetricsStore | src/metrics.rs | 请求指标持久化（tok/s、TTFB、TTFT 等） |
| BackendMetrics | src/backend_metrics.rs | Per-backend tok/s 滚动窗口，自适应负载均衡 |
| Streaming | src/streaming.rs | 流式响应包装：超时控制、token 计数、指标记录 |
| RequestPrep | src/request_prep.rs | 请求体预处理：模型替换、参数裁剪、JSON sanitize |
| RuntimeHealth | src/runtime_health.rs | 运行态健康指标（Atomic 操作，低开销） |
| Watchdog | src/watchdog.rs | OS 线程看门狗：检测 runtime stall 并自动重启 |
| ResponseMeta | src/response_meta.rs | 响应头注入（x-request-id, x-backend, x-ttfb-ms 等） |

## 流式超时控制

流式响应有三层超时保护：

| 超时类型 | 配置项 | 默认值 | 说明 |
|----------|--------|--------|------|
| 首个有效 chunk 超时 | stream_first_chunk_timeout_secs | 60s | 等待后端返回第一个有效 SSE chunk（role-only/keepalive 不算） |
| chunk 间空闲超时 | stream_idle_timeout_secs | 120s | 两个有效 chunk 之间的最大等待时间 |
| 请求总超时 | server.timeout_secs | 300s | 整个请求（含流式传输）的最大时长 |
| Fallback 链总超时 | fallback_timeout_secs | 300s | fallback 链尝试所有后端的最大总时长 |
| 请求体读取超时 | body_read_timeout_secs | 30s | 读取客户端请求体的超时，超时返回 408 |

## 熔断与恢复 (Circuit Breaker)

后端熔断器保护系统免受持续故障影响：

- **故障阈值**：连续 3 次失败 → 触发熔断（Open 状态）
- **恢复冷却**：60 秒后进入探测（HalfOpen 状态），可通过 `recovery_cooldown_secs` 配置
- **状态流转**：Closed（健康） → Open（熔断中） → HalfOpen（探测中）
- **探测逻辑**：HalfOpen 状态下放行一个请求，成功则回到 Closed，失败则重新 Open
- **选择跳过**：backend 选择时自动跳过 Open 状态的后端

## 模型映射 (Model Mapping)

`model_mapping` 是入口级的随机模型重写表，用于将一个请求模型随机分配到多个等效候选模型：

- 请求模型匹配到某个 key 后，proxy 从候选列表中随机选取一个
- 这不是 fallback 链，而是用于等效模型间的负载分散
- 候选模型应具有相近的能力和成本

```toml
[model_mapping]
"glm-5.1" = ["glm-5v-turbo", "glm-5.1"]   # 高端随机分流
"glm-4.7" = ["glm-4.7", "glm-4.7-plus"]   # 中端随机分流
"glm-4.0" = ["glm-4.0", "glm-4.0-plus"]   # 低端随机分流
```

此外，每个 backend 可单独配置 `model_mappings`，用于后端特定的模型名称翻译（比如后端 A 叫 `gpt-4o`，后端 B 叫 `claude-sonnet-4-20250514`）。

## 参数裁剪 (strip_params)

部分后端不支持某些请求参数（如 `thinking`、`metadata`、`service_tier`），转发前需要裁剪掉。

```toml
[[backends]]
name = "minimax-anthropic"
strip_params = ["metadata", "service_tier", "thinking"]
```

`strip_params` 列表中的字段会在 `prepare_request_body()` 阶段从请求 JSON 中移除，避免后端返回 400/422 错误。

## Watchdog 看门狗

Watchdog 是一个独立的 OS 线程，负责监控 tokio runtime 的健康状态：

- **检测机制**：监控 runtime tick，超过阈值未收到 tick 判定为 stall（默认 5s）
- **恢复动作**：ReExec（重新启动进程），通过 `execv` 自替换，保持监听端口不变
- **冷却时间**：两次重启间隔至少 120 秒，防止频繁重启
- **开发环境**：默认关闭，生产环境建议开启

相关配置项：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| watchdog_enabled | false | 看门狗开关 |
| watchdog_check_interval_ms | 2000 | 检查间隔 |
| watchdog_runtime_tick_stall_ms | 5000 | Runtime tick stall 阈值 |
| watchdog_restart_cooldown_secs | 120 | 重启冷却时间 |

## 辅助工具

### mock_llm_server

用于测试的 Mock LLM 后端，无需真实 API 即可验证代理行为。

- 默认端口 `8766`，通过 `MOCK_PORT` 环境变量配置
- 支持 `MOCK_DELAY_MS`（响应延迟）、`MOCK_ERROR_RATE`（错误率 0.0-1.0）、`MOCK_RATE_LIMIT_RATE`（限流率）
- Binary：`cargo run --bin mock_llm_server`

### log_analyzer

解析 proxy 日志，生成 Markdown 格式的统计报告。

- 输出每个模型的平均 TTFB、TTFT、tok/s、token 数量等统计
- Binary：`cargo run --bin log_analyzer`

## 测试

集成测试位于 `tests/` 目录，基于 in-process mock backend，不依赖真实 API。

### 测试基础设施

- `TestConfigBuilder`：快速构建测试配置
- `spawn_mock`：启动 mock LLM 后端
- `spawn_proxy_with_routes`：启动带自定义路由的代理
- 所有测试绑定 `127.0.0.1:0` 随机端口，避免端口冲突

### 主要测试文件

| 测试文件 | 覆盖范围 |
|----------|---------|
| stream_timeout.rs | 流式超时（首 chunk、idle、总时长） |
| fallback_status.rs | Fallback 链（状态码驱动降级） |
| protocol_routes.rs | 协议路由（OpenAI/Anthropic 路径前缀） |
| auth_rate_limit.rs | 认证与速率限制 |
| runtime_health.rs | 运行态健康指标 |
| watchdog_reexec.rs | Watchdog 重启行为 |

### 运行测试

```bash
# 全部测试
cargo test

# 单个测试文件（带输出）
cargo test --test stream_timeout -- --nocapture

# 性能基准测试（criterion）
cargo bench
```

## 完整配置参考

### Server 配置

```toml
[server]
port = 8090                         # 监听端口
timeout_secs = 300                  # 请求总超时（秒）
log_dir = "logs"                    # 日志目录
stream_idle_timeout_secs = 120      # 流式 chunk 间空闲超时
stream_first_chunk_timeout_secs = 60 # 首个有效 chunk 超时
fallback_timeout_secs = 300         # Fallback 链总超时
recovery_cooldown_secs = 60         # 熔断恢复冷却时间
body_read_timeout_secs = 30         # 请求体读取超时
watchdog_enabled = true             # 看门狗开关
watchdog_check_interval_ms = 2000   # 看门狗检查间隔
watchdog_runtime_tick_stall_ms = 5000 # Runtime tick stall 阈值
watchdog_restart_cooldown_secs = 120 # 重启冷却时间
```

### Backend 完整配置

```toml
[[backends]]
name = "backend-name"               # 后端名称（唯一标识）
url = "https://..."                 # 后端 API 地址
api_key = "${ENV_VAR}"              # API Key（支持环境变量）
weight = 10                         # 负载均衡权重
models = ["model-1", "model-2"]     # 支持的模型列表
timeout_secs = 300                  # 请求超时
connect_timeout_secs = 5            # 连接超时（默认 5s）
protocol = "openai"                 # 协议类型：openai / anthropic
strip_params = ["thinking"]         # 转发前裁剪的参数
# model_mappings: per-backend model name translation (高级用法)
```

## 环境变量

在 `.env` 文件中配置，配置文件通过 `${VAR}` 引用：

| 变量名 | 用途 | 示例 |
|--------|------|------|
| ZHIPU_API_KEY | 智谱 API Key | sk-xxx |
| MINIMAX_API_KEY_1 | MiniMax API Key 1 | sk-xxx |
| MINIMAX_API_KEY_2 | MiniMax API Key 2 | sk-xxx |
| GUIDOR_API_KEY | Guidor Claude 系列 Key | sk-xxx |
| GUIDOR_GPT_API_KEY | Guidor GPT 系列 Key | sk-xxx |
| MOCK_PORT | Mock 服务器端口（测试用） | 8766 |
| MOCK_DELAY_MS | Mock 响应延迟（测试用） | 1000 |
| MOCK_ERROR_RATE | Mock 错误率 0.0-1.0（测试用） | 0.1 |
| MOCK_RATE_LIMIT_RATE | Mock 限流率（测试用） | 0.5 |
