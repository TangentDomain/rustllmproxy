use llmproxy::binlib::unified_routes::protocol_routes;
use llmproxy::config::Config;
use llmproxy::proxy;
use llmproxy::watchdog::{spawn_reexec, start_watchdog_thread, WatchdogConfig};

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let config_path = config_path_from_args(std::env::args());
    let config = Config::load(&config_path).expect("Failed to load config");

    let log_dir = config.server.log_dir.clone();
    std::fs::create_dir_all(&log_dir).ok();
    let (log_dir, log_file_prefix) = log_file_name(&log_dir, config.server.port);
    let file_appender = tracing_appender::rolling::daily(&log_dir, log_file_prefix);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    std::mem::forget(guard);

    use tracing_subscriber::{prelude::*, Layer};

    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        // Console: INFO+, colored
        .with(
            tracing_subscriber::fmt::Layer::new()
                .with_writer(std::io::stdout)
                .with_ansi(atty::is(atty::Stream::Stdout))
                .with_target(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        )
        // File: all levels, no colors
        .with(
            tracing_subscriber::fmt::Layer::new()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(false),
        )
        .init();

    tracing::info!(
        "Starting unified-proxy on :{}, logging to {}/",
        config.server.port,
        log_dir
    );
    tracing::info!("Loaded config: {}", config_path);

    let watchdog_cfg = WatchdogConfig {
        enabled: config.server.watchdog_enabled,
        check_interval: std::time::Duration::from_millis(config.server.watchdog_check_interval_ms),
        runtime_tick_stall: std::time::Duration::from_millis(
            config.server.watchdog_runtime_tick_stall_ms,
        ),
        restart_cooldown: std::time::Duration::from_secs(
            config.server.watchdog_restart_cooldown_secs,
        ),
    };

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.server.port));
    let listener = proxy::bind_listener(addr);

    proxy::run_server_with_listener_and_hook(config, protocol_routes(), listener, move |proxy| {
        if !watchdog_cfg.enabled {
            return;
        }
        let exe = match std::env::current_exe() {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("watchdog current_exe failed: {e}");
                return;
            }
        };
        let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
        let runtime_health = proxy.runtime_health().clone();
        let spawn_action = move |_snap: &llmproxy::runtime_health::RuntimeHealthSnapshot| {
            match spawn_reexec(&exe, &args) {
                Ok(_) => true,
                Err(e) => {
                    tracing::error!("watchdog spawn re-exec failed: {e}");
                    false
                }
            }
        };
        start_watchdog_thread(runtime_health, watchdog_cfg.clone(), spawn_action);
        tracing::info!("watchdog started");
    })
    .await;
}

fn config_path_from_args<I, S>(args: I) -> String
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    args.into_iter()
        .nth(1)
        .map(Into::into)
        .unwrap_or_else(|| "configs/unified.toml".to_string())
}

fn log_file_name(log_dir: &str, port: u16) -> (String, String) {
    (log_dir.to_string(), format!("proxy-{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_path_defaults_to_unified_toml() {
        let args = ["unified-proxy"];
        assert_eq!(config_path_from_args(args), "configs/unified.toml");
    }

    #[test]
    fn config_path_uses_first_cli_argument() {
        let args = ["unified-proxy", "configs/unified-dev.toml"];
        assert_eq!(config_path_from_args(args), "configs/unified-dev.toml");
    }

    #[test]
    fn log_file_name_matches_runtime_convention() {
        assert_eq!(
            log_file_name("logs-dev", 8091),
            ("logs-dev".to_string(), "proxy-8091".to_string())
        );
    }
}
