pub mod backend_metrics;
pub mod balancer;
pub mod config;
pub mod metrics;
pub mod middleware;
pub mod proxy;
pub mod request_prep;
pub mod response_meta;
pub mod selection;
pub mod streaming;
pub mod runtime_health;
pub mod watchdog;

/// 与二进制入口（src/bin/*）复用的、可测试的通用逻辑。
pub mod binlib;
pub mod model_resolution;
