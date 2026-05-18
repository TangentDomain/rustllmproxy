# tests/ — 集成测试

## OVERVIEW
基于 in-process mock backend 的 Rust 集成测试，不依赖真实 API。

## STRUCTURE
```
tests/
├── common/
│   └── mod.rs             # 核心测试基础设施
├── stream_timeout.rs      # 流式超时（首 chunk、idle、总时长）
├── fallback_status.rs     # Fallback 链（状态码驱动降级）
├── protocol_routes.rs     # 协议路由（OpenAI/Anthropic 路径前缀）
├── auth_rate_limit.rs     # 认证与速率限制
├── request_edge_cases.rs  # 边界：缺失 model、慢速 body、嵌套 JSON
├── streaming_variants.rs  # 流式完成、首 chunk 超时、总超时
├── runtime_health.rs      # RuntimeHealth 纯单元测试
├── watchdog_reexec.rs     # Watchdog 状态机纯单元测试
└── perf_test.rs           # 独立性能基准（不使用 common/mod.rs）
```

## WHERE TO LOOK
| 任务 | 位置 | 备注 |
|------|------|------|
| 添加测试 helper | tests/common/mod.rs | TestConfigBuilder、spawn_mock 等 |
| 添加流式相关测试 | tests/stream_timeout.rs | SSE mock + 超时断言 |
| 添加 fallback 测试 | tests/fallback_status.rs | 多 backend mock + /backends API 验证 |
| 添加协议路由测试 | tests/protocol_routes.rs | 前缀路由 + 模型列表 |

## CONVENTIONS

### 端口与绑定
- 所有测试绑定 `127.0.0.1:0`（随机端口），通过 `listener.local_addr()` 获取
- **禁止**硬编码 8090/8091/8092 端口

### Mock 模式
- 每个测试文件定义自己的 `async fn spawn_xxx_mock()` 局部函数
- Mock 使用 `axum::Router` + `spawn_mock()` 启动 in-process
- 不依赖外部 mock server 进程

### 断言模式
- 用 `x-backend` 响应头验证 backend 选择（不假设选择顺序）
- 熔断状态通过轮询 `/backends` API 验证（带超时）
- 指标刷新通过轮询 `/livez` 验证（background task 异步）

### 请求序列化
- `reqwest` 无 `json` feature：用 `serde_json::to_string()` + `.body()`
- 响应反序列化：`.text()` + `serde_json::from_str()`

### 测试标记
- 集成测试：`#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`
- 纯单元测试：`#[test]`
- 测试 API Key：`TEST_API_KEY = "sk-proxy-default"`

## ANTI-PATTERNS
- ❌ 用硬编码端口替代 `bind_random_listener()`
- ❌ 用真实 token / 真实 API 做回归测试
- ❌ 只断言最终 status code，不验证 fallback 中间步骤
- ❌ 假设 backend 选择顺序稳定
- ❌ 新增 Bash/Python/curl 测试脚本
