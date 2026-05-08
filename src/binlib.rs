//! 给 `src/bin/*` 复用的库代码。
//!
//! 目的：
//! - 把二进制入口中的“纯逻辑”抽出来，方便单元测试提升覆盖率。
//! - 保持二进制入口仅负责 CLI glue（参数解析/配置加载/启动服务）。

pub mod log_analyzer;
pub mod mock_llm;
pub mod unified_routes;
