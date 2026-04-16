# Rust LLM Proxy

基于 Rust 的高性能 LLM 代理服务，支持多后端负载均衡和故障转移。

## 功能特性

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

**OpenAI 代理（端口 8091）：**
```bash
start-proxy.bat
# 或
cargo run --release --bin openai-proxy configs/openai.toml
```

**Anthropic 代理（端口 8092）：**
```bash
start-anthropic-proxy.bat
# 或
cargo run --release --bin anthropic-proxy configs/anthropic.toml
```

### 停止

```bash
stop-proxy.bat          # 停止 OpenAI 代理
stop-anthropic-proxy.bat # 停止 Anthropic 代理
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
│   │   ├── openai.rs       # OpenAI 代理入口
│   │   └── anthropic.rs    # Anthropic 代理入口
│   ├── balancer.rs         # 负载均衡器
│   ├── config.rs           # 配置解析
│   ├── middleware.rs       # 中间件（认证、限流）
│   ├── proxy.rs            # 代理核心逻辑
│   └── lib.rs
├── configs/
│   ├── openai.toml         # OpenAI 代理配置
│   └── anthropic.toml      # Anthropic 代理配置
└── Cargo.toml
```

## 使用示例

```bash
# OpenAI 兼容请求
curl http://localhost:8091/v1/chat/completions \
  -H "Authorization: Bearer sk-proxy-default" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}'

# Anthropic 兼容请求
curl http://localhost:8092/v1/messages \
  -H "x-api-key: sk-proxy-default" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-3-sonnet","max_tokens":1024,"messages":[{"role":"user","content":"Hello"}]}'
```

## 注意事项

- 首次运行前需修改 `configs/*.toml` 中的 API Key
- 确保后端服务可访问
- Windows 下建议使用 `.bat` 脚本启动
