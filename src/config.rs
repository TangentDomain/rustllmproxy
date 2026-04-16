use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub r#type: String,
    pub auth: AuthConfig,
    pub backends: Vec<Backend>,
    pub retry: u32,
    pub retry_delay_ms: u64,
    pub fallback: HashMap<String, Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    pub enabled: bool,
    pub keys: Vec<ApiKey>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiKey {
    pub key: String,
    pub name: String,
    pub rate_limit: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Backend {
    pub name: String,
    pub url: String,
    pub api_key: String,
    pub weight: u32,
    pub models: Vec<String>,
    /// 请求总超时（秒）
    pub timeout_secs: u64,
    /// 连接超时（秒），用于快速失败判断，默认5
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    #[serde(default)]
    pub model_mappings: HashMap<String, String>,
    /// Protocol type: "openai" or "anthropic"
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

fn default_connect_timeout() -> u64 { 5 }
fn default_protocol() -> String { "openai".to_string() }

impl Backend {
    /// 判断该后端是否支持某个模型（直接支持或通过映射）
    pub fn supports_model(&self, model: &str) -> bool {
        self.models.contains(&model.to_string()) || self.model_mappings.contains_key(model)
    }

    /// 获取该后端的实际模型名（有映射则转换，否则原样返回）
    pub fn resolve_model(&self, model: &str) -> String {
        self.model_mappings.get(model).cloned().unwrap_or_else(|| model.to_string())
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(Path::new(path))
            .with_context(|| format!("读取配置文件失败: {path}"))?;
        let mut cfg: Config = toml::from_str(&content)
            .with_context(|| format!("解析配置文件失败: {path}"))?;
        for b in &mut cfg.backends {
            b.api_key = expand_env(&b.api_key);
        }
        tracing::info!("配置加载成功: {path} (类型={}, 后端数={})", cfg.r#type, cfg.backends.len());
        Ok(cfg)
    }

    pub fn all_models(&self) -> Vec<String> {
        self.backends.iter().flat_map(|b| b.models.clone()).collect()
    }

    /// 获取模型的fallback链：先查具体模型，再查default（以请求模型为首）
    pub fn get_fallback_chain(&self, model: &str) -> Vec<String> {
        // 有专门配置的fallback链
        if let Some(chain) = self.fallback.get(model) {
            let mut v = vec![model.to_string()];
            for m in chain {
                if !v.contains(m) { v.push(m.clone()); }
            }
            return v;
        }
        // 使用default链，但以请求的模型为第一优先级
        if let Some(chain) = self.fallback.get("default") {
            let mut v = vec![model.to_string()];
            for m in chain {
                if !v.contains(m) { v.push(m.clone()); }
            }
            return v;
        }
        vec![model.to_string()]
    }

    /// 找到所有能服务某个模型的后端
    /// protocol: 可选的协议过滤 ("openai" 或 "anthropic")
    pub fn find_backends_for_model(&self, model: &str, protocol: Option<&str>) -> Vec<&Backend> {
        let mut backends: Vec<&Backend> = self.backends.iter()
            .filter(|b| b.supports_model(model))
            .collect();

        // 如果指定了协议，过滤出匹配的后端
        if let Some(proto) = protocol {
            backends = backends.into_iter()
                .filter(|b| b.protocol == proto)
                .collect();
        }

        backends
    }

    pub fn find_api_key(&self, key: &str) -> Option<&ApiKey> {
        self.auth.keys.iter().find(|k| k.key == key)
    }
}

/// 替换 ${VAR} 为环境变量值
fn expand_env(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // skip '{'
            let mut var = String::new();
            while let Some(vc) = chars.next() {
                if vc == '}' { break; }
                var.push(vc);
            }
            result.push_str(&std::env::var(&var).unwrap_or_else(|_| format!("${{{var}}}")));
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backend(name: &str, models: &[&str]) -> Backend {
        Backend {
            name: name.to_string(),
            url: format!("https://{name}.com"),
            api_key: format!("key-{name}"),
            weight: 10,
            models: models.iter().map(|s| s.to_string()).collect(),
            timeout_secs: 30,
            connect_timeout_secs: 5,
            model_mappings: HashMap::new(),
        }
    }

    fn make_backend_with_map(name: &str, models: &[&str], mappings: HashMap<&str, &str>) -> Backend {
        let mut m = HashMap::new();
        for (k, v) in &mappings {
            m.insert(k.to_string(), v.to_string());
        }
        Backend {
            name: name.to_string(),
            url: format!("https://{name}.com"),
            api_key: format!("key-{name}"),
            weight: 10,
            models: models.iter().map(|s| s.to_string()).collect(),
            timeout_secs: 30,
            connect_timeout_secs: 5,
            model_mappings: m,
        }
    }

    fn make_config(fallback: HashMap<String, Vec<String>>, backends: Vec<Backend>) -> Config {
        Config {
            server: ServerConfig { port: 8091, timeout_secs: 30 },
            r#type: "openai".to_string(),
            auth: AuthConfig { enabled: false, keys: vec![] },
            backends,
            retry: 2,
            retry_delay_ms: 500,
            fallback,
        }
    }

    // ─── Fallback Chain Tests ──────────────────────────────

    #[test]
    fn test_fallback_chain_specific_model() {
        let mut fb = HashMap::new();
        fb.insert("glm-5.1".to_string(), vec!["glm-5v-turbo".to_string(), "glm-4.7".to_string()]);
        fb.insert("default".to_string(), vec!["a".to_string(), "b".to_string()]);
        let cfg = make_config(fb, vec![]);

        let chain = cfg.get_fallback_chain("glm-5.1");
        assert_eq!(chain, vec!["glm-5.1", "glm-5v-turbo", "glm-4.7"]);
    }

    #[test]
    fn test_fallback_chain_default_with_unknown_model() {
        let mut fb = HashMap::new();
        fb.insert("default".to_string(), vec!["x".to_string(), "y".to_string(), "z".to_string()]);
        let cfg = make_config(fb, vec![]);

        let chain = cfg.get_fallback_chain("unknown-model");
        assert_eq!(chain, vec!["unknown-model", "x", "y", "z"]);
    }

    #[test]
    fn test_fallback_chain_no_config() {
        let cfg = make_config(HashMap::new(), vec![]);
        let chain = cfg.get_fallback_chain("anything");
        assert_eq!(chain, vec!["anything"]);
    }

    #[test]
    fn test_fallback_chain_dedup() {
        let mut fb = HashMap::new();
        fb.insert("glm-5.1".to_string(), vec!["glm-5v-turbo".to_string(), "glm-5.1".to_string(), "glm-4.7".to_string()]);
        let cfg = make_config(fb, vec![]);

        let chain = cfg.get_fallback_chain("glm-5.1");
        assert_eq!(chain.len(), 3);
        assert_eq!(chain, vec!["glm-5.1", "glm-5v-turbo", "glm-4.7"]);
    }

    #[test]
    fn test_fallback_chain_request_model_first_in_default() {
        let mut fb = HashMap::new();
        fb.insert("default".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let cfg = make_config(fb, vec![]);

        let chain = cfg.get_fallback_chain("my-model");
        assert_eq!(chain[0], "my-model");
        assert_eq!(chain, vec!["my-model", "a", "b", "c"]);
    }

    // ─── Backend Model Matching Tests ──────────────────

    #[test]
    fn test_supports_model_direct() {
        let b = make_backend("zhipu", &["glm-5.1", "glm-4.6"]);
        assert!(b.supports_model("glm-5.1"));
        assert!(b.supports_model("glm-4.6"));
        assert!(!b.supports_model("MiniMax-M2.7"));
    }

    #[test]
    fn test_supports_model_via_mapping() {
        let mut m = HashMap::new();
        m.insert("glm-5.1", "MiniMax-M2.1");
        m.insert("glm-4.6", "MiniMax-M2.1");
        let b = make_backend_with_map("minimax", &["MiniMax-M2.1"], m);

        assert!(b.supports_model("glm-5.1"));
        assert!(b.supports_model("glm-4.6"));
        assert!(b.supports_model("MiniMax-M2.1"));
        assert!(!b.supports_model("glm-4.7"));
    }

    #[test]
    fn test_resolve_model_no_mapping() {
        let b = make_backend("zhipu", &["glm-5.1"]);
        assert_eq!(b.resolve_model("glm-5.1"), "glm-5.1");
    }

    #[test]
    fn test_resolve_model_with_mapping() {
        let mut m = HashMap::new();
        m.insert("glm-5.1", "MiniMax-M2.1");
        let b = make_backend_with_map("minimax", &["MiniMax-M2.1"], m);
        assert_eq!(b.resolve_model("glm-5.1"), "MiniMax-M2.1");
        assert_eq!(b.resolve_model("MiniMax-M2.1"), "MiniMax-M2.1");
    }

    // ─── Routing Tests ───────────────────────────────────

    #[test]
    fn test_find_backends_glm_routes_to_zhipu() {
        let cfg = make_config(HashMap::new(), vec![
            make_backend("zhipu", &["glm-5.1", "glm-4.6", "glm-4.7"]),
            make_backend("minimax", &["MiniMax-M2.7"]),
        ]);

        assert_eq!(cfg.find_backends_for_model("glm-5.1").len(), 1);
        assert_eq!(cfg.find_backends_for_model("glm-5.1")[0].name, "zhipu");

        assert_eq!(cfg.find_backends_for_model("MiniMax-M2.7").len(), 1);
        assert_eq!(cfg.find_backends_for_model("MiniMax-M2.7")[0].name, "minimax");

        assert!(cfg.find_backends_for_model("nonexistent").is_empty());
    }

    #[test]
    fn test_find_backends_multiple_match() {
        let cfg = make_config(HashMap::new(), vec![
            make_backend("zhipu", &["glm-5.1", "MiniMax-M2.7"]),
            make_backend("minimax", &["MiniMax-M2.7"]),
        ]);

        let backends = cfg.find_backends_for_model("MiniMax-M2.7");
        assert_eq!(backends.len(), 2);
        let names: Vec<&str> = backends.iter().map(|b| b.name.as_str()).collect();
        assert!(names.contains(&"zhipu"));
        assert!(names.contains(&"minimax"));
    }

    // ─── Full Integration Test (模拟真实配置) ───────────

    #[test]
    fn test_full_routing_scenario() {
        let mut fb = HashMap::new();
        fb.insert("glm-5.1".into(), vec!["glm-5v-turbo".into(), "MiniMax-M2.7".into(), "glm-4.7".into()]);
        fb.insert("glm-4.7".into(), vec!["MiniMax-M2.7".into(), "glm-4.6".into()]);
        fb.insert("MiniMax-M2.7".into(), vec!["glm-5.1".into(), "glm-4.7".into()]);
        fb.insert("default".into(), vec!["glm-5.1".into(), "glm-4.7".into(), "MiniMax-M2.7".into()]);

        let cfg = make_config(fb, vec![
            make_backend("zhipu", &["glm-5.1", "glm-5v-turbo", "glm-4.7", "glm-4.6"]),
            make_backend("minimax-1", &["MiniMax-M2.7"]),
            make_backend("minimax-2", &["MiniMax-M2.7"]),
        ]);

        // glm-5.1 链
        let chain = cfg.get_fallback_chain("glm-5.1");
        assert_eq!(chain, vec!["glm-5.1", "glm-5v-turbo", "MiniMax-M2.7", "glm-4.7"]);

        // glm-5.1 → zhipu
        assert_eq!(cfg.find_backends_for_model("glm-5.1").len(), 1);
        assert_eq!(cfg.find_backends_for_model("glm-5.1")[0].name, "zhipu");

        // glm-5v-turbo → default链，自身打头
        let chain2 = cfg.get_fallback_chain("glm-5v-turbo");
        assert_eq!(chain2[0], "glm-5v-turbo");

        // M2.7 → minimax组 (2个)
        let mm_backends = cfg.find_backends_for_model("MiniMax-M2.7");
        assert_eq!(mm_backends.len(), 2);
        assert!(mm_backends.iter().all(|b| b.name.starts_with("minimax")));

        // M2.7 专属链
        let mm_chain = cfg.get_fallback_chain("MiniMax-M2.7");
        assert_eq!(mm_chain, vec!["MiniMax-M2.7", "glm-5.1", "glm-4.7"]);

        // 验证完整路由过程
        for (i, m) in chain.iter().enumerate() {
            let backends = cfg.find_backends_for_model(m);
            match i {
                0 => { assert_eq!(backends.len(), 1); assert_eq!(backends[0].name, "zhipu"); }
                1 => { assert_eq!(backends.len(), 1); assert_eq!(backends[0].name, "zhipu"); }
                2 => { assert_eq!(backends.len(), 2); } // minimax 组
                3 => { assert_eq!(backends.len(), 1); assert_eq!(backends[0].name, "zhipu"); }
                _ => panic!("too many steps"),
            }
        }
    }

    // ─── Priority Order Test (M2.7 > glm-4.7 > M2.5 > glm-4.6) ─────

    #[test]
    fn test_priority_order_m27_gt_47_gt_m25_gt_46() {
        let mut fb = HashMap::new();
        let chain_47: Vec<String> = vec!["MiniMax-M2.7".to_string(), "glm-4.6".to_string(), "MiniMax-M2.5".to_string()];
        let chain_def: Vec<String> = vec!["glm-5.1".to_string(), "glm-4.7".to_string(), "MiniMax-M2.7".to_string(), "MiniMax-M2.5".to_string(), "glm-4.6".to_string()];
        fb.insert("glm-4.7".to_string(), chain_47);
        fb.insert("default".to_string(), chain_def);

        let cfg = make_config(fb, vec![
            make_backend("zhipu", &["glm-5.1", "glm-4.7", "glm-4.6"]),
            make_backend("minimax", &["MiniMax-M2.7", "MiniMax-M2.5"]),
        ]);

        let chain = cfg.get_fallback_chain("glm-4.7");
        assert_eq!(chain, vec!["glm-4.7", "MiniMax-M2.7", "glm-4.6", "MiniMax-M2.5"]);

        let expected = ["zhipu", "minimax", "zhipu", "minimax"];
        for (i, m) in chain.iter().enumerate() {
            let backends = cfg.find_backends_for_model(m);
            assert_eq!(
                backends[0].name,
                expected[i],
                "step {}: model={} should route to {}",
                i, m, expected[i]
            );
        }
    }

    // ─── Efficiency Tests ───────────────────────────────────

    #[test]
    fn test_efficiency_timeout_scenario() {
        // 场景: 第一个后端超时，应该快速fallback而不是等待3次超时
        let mut fb = HashMap::new();
        fb.insert("glm-5.1".to_string(), vec!["glm-4.7".to_string()]);

        let cfg = make_config(fb, vec![
            make_backend("slow-backend", &["glm-5.1"]),
            make_backend("fast-backend", &["glm-4.7"]),
        ]);

        // 当前实现: 如果 slow-backend 每次都超时 (180秒)
        // 等待时间 = 180秒 × 3次重试 = 540秒 ❌
        // 期望: 应该在第一次超时后就快速fallback ✅

        let chain = cfg.get_fallback_chain("glm-5.1");
        assert_eq!(chain, vec!["glm-5.1", "glm-4.7"]);

        // glm-5.1 只有 1 个后端，如果它超时应该立即切换
        let backends = cfg.find_backends_for_model("glm-5.1");
        assert_eq!(backends.len(), 1); // 单点故障
    }

    #[test]
    fn test_efficiency_429_rate_limit() {
        // 场景: 429 rate limit 应该立即切换到另一个key
        let cfg = make_config(HashMap::new(), vec![
            make_backend("minimax-1", &["MiniMax-M2.7"]),
            make_backend("minimax-2", &["MiniMax-M2.7"]),
        ]);

        // MiniMax-M2.7 有 2 个后端
        let backends = cfg.find_backends_for_model("MiniMax-M2.7");
        assert_eq!(backends.len(), 2);

        // 当前实现: minimax-1 返回429后会重试2次
        // 问题: 如果是rate limit，重试同一个key没用 ❌
        // 期望: 429应该立即切换到minimax-2 ✅
    }

    #[test]
    fn test_efficiency_unhealthy_backend_skip() {
        // 场景: 后端已被健康检查标记为不健康，应该跳过
        let mut fb = HashMap::new();
        fb.insert("glm-5.1".to_string(), vec!["glm-4.7".to_string()]);

        let cfg = make_config(fb, vec![
            make_backend("dead-backend", &["glm-5.1"]),
            make_backend("alive-backend", &["glm-4.7"]),
        ]);

        let chain = cfg.get_fallback_chain("glm-5.1");
        // 如果 dead-backend 不健康，应该立即跳过
        // 不应该尝试连接直到超时
        assert_eq!(chain, vec!["glm-5.1", "glm-4.7"]);
    }

    #[test]
    fn test_efficiency_client_error_no_retry() {
        // 场景: 400/401 等客户端错误不应该触发重试
        // 当前实现: proxy.rs 正确处理了这一点
        // 4xx 错误直接返回，不重试 ✅

        let cfg = make_config(HashMap::new(), vec![
            make_backend("backend", &["glm-5.1"]),
        ]);

        let backends = cfg.find_backends_for_model("glm-5.1");
        assert_eq!(backends.len(), 1);
        // 400 错误应该直接返回给客户端，不应该fallback
    }

    #[test]
    fn test_efficiency_multiple_backends_same_model() {
        // 场景: 同一模型有多个后端时的效率
        let cfg = make_config(HashMap::new(), vec![
            make_backend("backend-1", &["MiniMax-M2.7"]),
            make_backend("backend-2", &["MiniMax-M2.7"]),
            make_backend("backend-3", &["MiniMax-M2.7"]),
        ]);

        let backends = cfg.find_backends_for_model("MiniMax-M2.7");
        assert_eq!(backends.len(), 3);

        // 当前实现: 3个后端加权随机
        // 如果都返回 429，会重试第一个 3次 ❌
        // 期望: 应该轮询所有3个后端，每个只试1次 ✅
    }
}
