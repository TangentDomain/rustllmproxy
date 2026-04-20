# Rust LLM Proxy

基于 Rust 的高性能 LLM 代理服务，支持多后端负载均衡和智能故障转移。

## 功能特性

- **统一代理**：单个进程同时支持 OpenAI 和 Anthropic API
- **多后端负载均衡**：加权轮询，支持多 key 负载均衡
- **智能故障转移**：模型级 fallback 链，429/5xx/4xx(除401) 自动降级
- **性能监控**：TTFB、TTFT、tokens/s 等实时指标
- **日志轮转**：按天自动轮转，异步写入不阻塞请求
- **认证限流**：API Key 认证 + 请求速率限制

## 快速开始

```bash
# 构建
cargo build --release

# 启动（自动拷贝到 run/ 目录，不影响后续编译）
start-unified-proxy.bat

# 停止
taskkill /F /IM unified-proxy.exe
```

## API 路由

通过路径前缀区分协议：

| 协议 | 路径前缀 | 示例 |
|------|---------|------|
| OpenAI | `/openai/v1/...` | `/openai/v1/chat/completions` |
| Anthropic | `/anthropic/v1/...` | `/anthropic/v1/messages` |

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
