pub mod backend_metrics;
pub mod balancer;
pub mod config;
pub mod metrics;
pub mod middleware;
pub mod proxy;
pub mod streaming;

/// 与二进制入口（src/bin/*）复用的、可测试的通用逻辑。
pub mod binlib;
