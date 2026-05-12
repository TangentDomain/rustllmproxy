use std::collections::HashMap;

use crate::config::{Backend, Config};

pub fn fallback_chain(config: &Config, model: &str) -> Vec<String> {
    config.get_fallback_chain(model)
}

pub fn backends_for_model<'a>(
    config: &'a Config,
    model: &str,
    protocol: Option<&str>,
) -> Vec<&'a Backend> {
    config.find_backends_for_model(model, protocol)
}

pub fn resolved_model(backend: &Backend, model: &str) -> String {
    backend.resolve_model(model)
}

pub fn api_key_index<'a>(config: &'a Config, key: &str) -> Option<&'a usize> {
    config.auth.key_index.get(key)
}

pub fn build_auth_key_index(keys: &[crate::config::ApiKey]) -> HashMap<String, usize> {
    let mut key_index = HashMap::with_capacity(keys.len());
    for (idx, k) in keys.iter().enumerate() {
        key_index.entry(k.key.clone()).or_insert(idx);
    }
    key_index
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiKey, AuthConfig, Backend, Config, ServerConfig};

    fn sample_backend() -> Backend {
        Backend {
            name: "backend-a".to_string(),
            url: "http://localhost".to_string(),
            api_key: "sk-test".to_string(),
            weight: 10,
            models: vec!["model-a".to_string(), "model-b".to_string()],
            timeout_secs: 30,
            connect_timeout_secs: 5,
            model_mappings: HashMap::from([
                ("alias-a".to_string(), "real-a".to_string()),
            ]),
            protocol: "openai".to_string(),
            auth_header: "Bearer sk-test".to_string(),
            strip_params: vec![],
        }
    }

    fn sample_config() -> Config {
        Config {
            server: ServerConfig {
                port: 8090,
                timeout_secs: 300,
                log_dir: "logs".to_string(),
                stream_idle_timeout_secs: 120,
                stream_first_chunk_timeout_secs: 60,
                fallback_timeout_secs: 300,
                recovery_cooldown_secs: 60,
                body_read_timeout_secs: 30,
                watchdog_enabled: false,
                watchdog_check_interval_ms: 2_000,
                watchdog_runtime_tick_stall_ms: 5_000,
                watchdog_restart_cooldown_secs: 120,
            },
            r#type: "unified".to_string(),
            auth: AuthConfig {
                enabled: true,
                keys: vec![
                    ApiKey {
                        key: "key-1".to_string(),
                        name: "primary".to_string(),
                        rate_limit: 100,
                    },
                    ApiKey {
                        key: "key-2".to_string(),
                        name: "secondary".to_string(),
                        rate_limit: 50,
                    },
                ],
                key_index: HashMap::new(),
            },
            backends: vec![sample_backend()],
            retry: 3,
            retry_delay_ms: 100,
            fallback: HashMap::from([
                ("model-a".to_string(), vec!["model-b".to_string()]),
                ("default".to_string(), vec!["fallback-x".to_string()]),
            ]),
            model_mapping: HashMap::from([
                ("client-model".to_string(), vec!["model-a".to_string()]),
            ]),
        }
    }

    #[test]
    fn fallback_chain_prefers_specific_rule() {
        let config = sample_config();
        assert_eq!(fallback_chain(&config, "model-a"), vec!["model-a", "model-b"]);
    }

    #[test]
    fn fallback_chain_uses_default_when_missing_specific_rule() {
        let config = sample_config();
        assert_eq!(fallback_chain(&config, "unknown"), vec!["unknown", "fallback-x"]);
    }

    #[test]
    fn backends_for_model_filters_by_protocol_and_supports_mapping() {
        let mut config = sample_config();
        config.backends.push(Backend {
            name: "backend-b".to_string(),
            url: "http://localhost-2".to_string(),
            api_key: "sk-test-2".to_string(),
            weight: 5,
            models: vec!["other".to_string()],
            timeout_secs: 30,
            connect_timeout_secs: 5,
            model_mappings: HashMap::from([
                ("client-model".to_string(), "mapped-model".to_string()),
            ]),
            protocol: "anthropic".to_string(),
            auth_header: "sk-test-2".to_string(),
            strip_params: vec![],
        });

        let openai_backends = backends_for_model(&config, "model-a", Some("openai"));
        assert_eq!(openai_backends.len(), 1);
        assert_eq!(openai_backends[0].name, "backend-a");

        let anthropic_backends = backends_for_model(&config, "client-model", Some("anthropic"));
        assert_eq!(anthropic_backends.len(), 1);
        assert_eq!(anthropic_backends[0].name, "backend-b");

        let all_backends = backends_for_model(&config, "model-a", None);
        assert_eq!(all_backends.len(), 1);
    }

    #[test]
    fn resolved_model_and_auth_key_index_work() {
        let backend = sample_backend();
        assert_eq!(resolved_model(&backend, "client-model"), "client-model");
        assert_eq!(resolved_model(&backend, "alias-a"), "real-a");

        let index = build_auth_key_index(&sample_config().auth.keys);
        assert_eq!(index.get("key-1"), Some(&0));
        assert_eq!(index.get("key-2"), Some(&1));

    }
}
