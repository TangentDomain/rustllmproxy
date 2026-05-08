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
