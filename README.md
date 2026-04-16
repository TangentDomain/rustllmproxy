# Rust LLM Proxy

基于 Rust 的高性能 LLM 代理服务，支持多后端负载均衡和故障转移。

## 功能特性

- **统一代理**：单个进程同时支持 OpenAI 和 Anthropic API
- **OpenAI/Anthropic 兼容 API**：完全兼容 OpenAI 和 Anthropic API 规范
- **多后端负载均衡**：支持配置多个后端服务，按权重分配请求
- **智能故障转移**：模型级别的 fallback 链，自动降级到可用模型
- **认证与限流**：支持 API Key 认证和请求速率限制
- **高性能**：基于 Tokio 异步运行时和 Axum 框架

## 快速开始

### 构建

```bash
cargo build --release
```

### 运行

```bash
start-unified-proxy.bat
# 或
cargo run --release --bin unified-proxy configs/unified.toml
```

### 停止

```bash
stop-unified-proxy.bat
```

## 统一代理说明

统一代理通过路径前缀区分 API 类型：

- **OpenAI API**：`/openai/v1/chat/completions`, `/openai/v1/completions`, `/openai/v1/models`
- **Anthropic API**：`/anthropic/v1/messages`, `/anthropic/v1/models`

配置文件中每个后端需指定 `protocol` 字段：
```toml
[[backends]]
name = "openai-backend"
protocol = "openai"
url = "https://api.example.com"
api_key = "YOUR_API_KEY"

[[backends]]
name = "anthropic-backend"
protocol = "anthropic"
url = "https://api.example.com/anthropic"
api_key = "YOUR_API_KEY"
```

## 配置说明

配置文件位于 `configs/` 目录，支持以下配置：

### 后端配置

```toml
[[backends]]
name = "backend-name"
url = "https://api.example.com"
api_key = "YOUR_API_KEY"
weight = 10              # 负载均衡权重
models = ["model-1", "model-2"]
timeout_secs = 300
connect_timeout_secs = 10
protocol = "openai"      # "openai" 或 "anthropic"
```

### Fallback 链

为每个模型配置降级链：

```toml
[fallback]
"gpt-4" = ["gpt-3.5-turbo", "claude-3-sonnet"]
default = ["gpt-4", "gpt-3.5-turbo"]
```

### 认证配置

```toml
[auth]
enabled = true

[[auth.keys]]
key = "sk-proxy-default"
name = "default"
rate_limit = 60  # 每分钟请求限制
```

## 目录结构

```
.
├── src/
│   ├── bin/
│   │   └── unified.rs      # 统一代理入口
│   ├── balancer.rs         # 负载均衡器
│   ├── config.rs           # 配置解析
│   ├── middleware.rs       # 中间件（认证、限流）
│   ├── proxy.rs            # 代理核心逻辑
│   └── lib.rs
├── configs/
│   └── unified.toml        # 统一代理配置
└── Cargo.toml
```

## 使用示例

## 使用示例

```bash
# OpenAI 兼容请求
curl http://localhost:8090/openai/v1/chat/completions \
  -H "Authorization: Bearer sk-proxy-default" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}'

# Anthropic 兼容请求
curl http://localhost:8090/anthropic/v1/messages \
  -H "x-api-key: sk-proxy-default" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-3-sonnet","max_tokens":1024,"messages":[{"role":"user","content":"Hello"}]}'
```

## 注意事项

- 首次运行前需配置 API Key：
  1. 复制 `.env.example` 为 `.env`
  2. 填入真实的 API keys
  3. Windows 下启动脚本会自动加载 `.env` 文件
- 确保后端服务可访问
- Windows 下建议使用 `.bat` 脚本启动

## 环境变量配置

支持通过 `.env` 文件配置 API keys：

```bash
# .env 文件示例
ZHIPU_API_KEY=your_zhipu_api_key_here
MINIMAX_API_KEY_1=your_minimax_api_key_1_here
MINIMAX_API_KEY_2=your_minimax_api_key_2_here
```

配置文件中引用环境变量：
```toml
[[backends]]
name = "zhipu-openai"
api_key = "${ZHIPU_API_KEY}"
```
