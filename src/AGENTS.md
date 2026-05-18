# src/ — 核心库

## OVERVIEW
代理核心逻辑、配置、负载均衡、流式处理、认证、指标、看门狗。

## STRUCTURE
```
src/
├── lib.rs              # 模块导出
├── proxy.rs            # 主代理：fallback 链、请求转发、错误分类（1205 行）
├── config.rs           # TOML 配置解析、env 展开（866 行）
├── balancer.rs         # WeightedRoundRobin + Circuit Breaker（465 行）
├── streaming.rs        # 流式响应包装、三层超时、token 计数（681 行）
├── request_prep.rs     # 请求体预处理：模型替换、strip_params（340 行）
├── middleware.rs       # API Key + rate limit
├── metrics.rs          # 指标持久化、tok/s 滚动窗口、每日归档
├── backend_metrics.rs  # Per-backend tok/s 滚动窗口，自适应权重
├── model_resolution.rs # fallback_chain / backends_for_model 的纯函数层
├── selection.rs        # 加权随机选择（choose_weighted_index）
├── response_meta.rs    # 响应头注入（x-request-id, x-backend, x-ttfb-ms）
├── runtime_health.rs   # Atomic 运行态健康指标（低开销快照）
├── watchdog.rs         # OS 线程看门狗：stall 检测 + ReExec
├── binlib.rs           # 给 bin/ 复用的公共逻辑入口
├── bin/                # 二进制入口（CLI glue）
│   ├── unified.rs      # 主服务入口
│   ├── mock_llm_server.rs
│   └── log_analyzer.rs
└── binlib/             # bin 可测试逻辑
    ├── unified_routes.rs   # 协议路由公共定义
    ├── mock_llm.rs         # mock server 可测试逻辑
    └── log_analyzer.rs     # 日志分析纯逻辑
```

## WHERE TO LOOK
| 任务 | 文件 | 备注 |
|------|------|------|
| 添加新配置项 | config.rs | Struct 字段 + serde default 函数 |
| 修改请求转发逻辑 | proxy.rs::handle | 最核心的热路径 |
| 修改流式超时行为 | streaming.rs::instrument_stream | 三层超时 + token 计数 |
| 修改 fallback 策略 | config.rs::get_fallback_chain | 链构建逻辑 |
| 修改负载均衡 | balancer.rs | 选择器 + 熔断状态机 |
| 添加新管理端点 | binlib/unified_routes.rs + proxy.rs | 路由定义 + handler |

## CONVENTIONS
- 所有模块包含 `#[cfg(test)]` 内联单元测试
- 文档和注释默认中文，API 名/变量名保留英文
- `reqwest` 无 `json` feature：用 `serde_json::to_string()` + `.body()`
- 无 `unsafe` 块，无 `todo!/FIXME/HACK/XXX`
- 新增公开 API 必须补充 `///` 文档注释（当前覆盖率不均）

## ANTI-PATTERNS
- ❌ 在 `Proxy::handle` 热路径中添加不必要的序列化/反序列化
- ❌ 在 `balancer.rs` 的选择器状态机中引入新的 `unwrap()`（已有 3 个热区）
- ❌ 修改 `streaming.rs` 的 SSE 解析逻辑时破坏 role-only/keepalive 语义
