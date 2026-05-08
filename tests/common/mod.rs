#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use llmproxy::config::{ApiKey, AuthConfig, Backend, Config, ServerConfig};
use llmproxy::proxy::{run_server_with_listener, Proxy};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub const TEST_API_KEY: &str = "sk-proxy-default";

#[derive(Clone, Debug)]
pub struct TestBackend {
    pub name: String,
    pub addr: SocketAddr,
    pub protocol: String,
    pub models: Vec<String>,
    pub api_key: String,
}

impl TestBackend {
    pub fn openai(name: impl Into<String>, addr: SocketAddr) -> Self {
        Self {
            name: name.into(),
            addr,
            protocol: "openai".to_string(),
            models: vec!["mock-model".to_string()],
            api_key: "mock-key".to_string(),
        }
    }

    pub fn anthropic(name: impl Into<String>, addr: SocketAddr) -> Self {
        Self {
            name: name.into(),
            addr,
            protocol: "anthropic".to_string(),
            models: vec!["mock-model".to_string()],
            api_key: "mock-key".to_string(),
        }
    }

    pub fn with_models(mut self, models: &[&str]) -> Self {
        self.models = models.iter().map(|model| (*model).to_string()).collect();
        self
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = api_key.into();
        self
    }
}

pub struct TestConfigBuilder {
    auth_enabled: bool,
    api_key: String,
    rate_limit: u32,
    timeout_secs: u64,
    stream_idle_timeout_secs: u64,
    stream_first_chunk_timeout_secs: u64,
    fallback_timeout_secs: u64,
    backends: Vec<TestBackend>,
    fallback: HashMap<String, Vec<String>>,
    model_mapping: HashMap<String, Vec<String>>,
}

impl TestConfigBuilder {
    pub fn new() -> Self {
        Self {
            auth_enabled: true,
            api_key: TEST_API_KEY.to_string(),
            rate_limit: 1_000,
            timeout_secs: 10,
            stream_idle_timeout_secs: 10,
            stream_first_chunk_timeout_secs: 10,
            fallback_timeout_secs: 10,
            backends: Vec::new(),
            fallback: HashMap::new(),
            model_mapping: HashMap::new(),
        }
    }

    pub fn auth_enabled(mut self, enabled: bool) -> Self {
        self.auth_enabled = enabled;
        self
    }

    pub fn rate_limit(mut self, limit: u32) -> Self {
        self.rate_limit = limit;
        self
    }

    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    pub fn stream_idle_timeout_secs(mut self, secs: u64) -> Self {
        self.stream_idle_timeout_secs = secs;
        self
    }

    pub fn stream_first_chunk_timeout_secs(mut self, secs: u64) -> Self {
        self.stream_first_chunk_timeout_secs = secs;
        self
    }

    pub fn fallback_timeout_secs(mut self, secs: u64) -> Self {
        self.fallback_timeout_secs = secs;
        self
    }

    pub fn backend(mut self, backend: TestBackend) -> Self {
        self.backends.push(backend);
        self
    }

    pub fn fallback_chain(mut self, model: &str, chain: &[&str]) -> Self {
        self.fallback.insert(
            model.to_string(),
            chain.iter().map(|entry| (*entry).to_string()).collect(),
        );
        self
    }

    pub fn model_mapping(mut self, source: &str, targets: &[&str]) -> Self {
        self.model_mapping.insert(
            source.to_string(),
            targets.iter().map(|target| (*target).to_string()).collect(),
        );
        self
    }

    pub fn build(self) -> Config {
        let backends = self
            .backends
            .into_iter()
            .map(|backend| Backend {
                name: backend.name,
                url: format!("http://{}", backend.addr),
                api_key: backend.api_key.clone(),
                weight: 1,
                models: backend.models,
                timeout_secs: self.timeout_secs,
                connect_timeout_secs: 1,
                model_mappings: HashMap::new(),
                protocol: backend.protocol,
                auth_header: format!("Bearer {}", backend.api_key),
                strip_params: vec![],
            })
            .collect();

        Config {
            server: ServerConfig {
                port: 0,
                timeout_secs: self.timeout_secs,
                log_dir: "logs-test".to_string(),
                stream_idle_timeout_secs: self.stream_idle_timeout_secs,
                stream_first_chunk_timeout_secs: self.stream_first_chunk_timeout_secs,
                fallback_timeout_secs: self.fallback_timeout_secs,
            },
            r#type: "openai".to_string(),
            auth: AuthConfig {
                enabled: self.auth_enabled,
                keys: vec![ApiKey {
                    key: self.api_key.clone(),
                    name: "default".to_string(),
                    rate_limit: self.rate_limit,
                }],
                key_index: HashMap::from([(self.api_key, 0usize)]),
            },
            backends,
            retry: 0,
            retry_delay_ms: 0,
            fallback: self.fallback,
            model_mapping: self.model_mapping,
        }
    }
}

impl Default for TestConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn bind_random_listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener")
}

pub async fn spawn_mock(app: Router) -> (SocketAddr, JoinHandle<()>) {
    let listener = bind_random_listener().await;
    let addr = listener.local_addr().expect("mock addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock server error");
    });
    (addr, handle)
}

pub async fn spawn_proxy(config: Config) -> (SocketAddr, JoinHandle<()>) {
    spawn_proxy_with_routes(config, protocol_routes()).await
}

pub async fn spawn_proxy_with_routes(
    config: Config,
    extra_routes: Router<Arc<Proxy>>,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = bind_random_listener().await;
    let addr = listener.local_addr().expect("proxy addr");
    let handle = tokio::spawn(async move {
        run_server_with_listener(config, extra_routes, listener).await;
    });
    (addr, handle)
}

pub fn protocol_routes() -> Router<Arc<Proxy>> {
    llmproxy::binlib::unified_routes::protocol_routes()
}

pub fn openai_chat_body(model: &str, content: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": content}]
    }))
    .expect("serialize openai body")
}

pub fn anthropic_messages_body(model: &str, content: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "model": model,
        "max_tokens": 32,
        "messages": [{"role": "user", "content": content}]
    }))
    .expect("serialize anthropic body")
}
