use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Serialize, Deserialize)]
pub struct KafkaConfig {
    kafka: HashMap<String, ListenerConfig>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ListenerConfig {
    #[serde(default = "default_bootstrap_servers")]
    #[serde(rename = "bootstrap.servers")]
    bootstrap_servers: String,

    #[serde(default = "default_group_id")]
    #[serde(rename = "group.id")]
    group_id: String,

    #[serde(default = "default_auto_offset_reset")]
    #[serde(rename = "auto.offset.reset")]
    auto_offset_reset: String,

    #[serde(default = "default_enable_auto_commit")]
    #[serde(rename = "enable.auto.commit")]
    enable_auto_commit: String,

    #[serde(default = "default_session_timeout_ms")]
    #[serde(rename = "session.timeout.ms")]
    session_timeout_ms: String,

    #[serde(default = "default_heartbeat_interval_ms")]
    #[serde(rename = "heartbeat.interval.ms")]
    heartbeat_interval_ms: String,

    #[serde(default = "default_max_poll_interval_ms")]
    #[serde(rename = "max.poll.interval.ms")]
    max_poll_interval_ms: String,

    #[serde(default = "default_fetch_max_bytes")]
    #[serde(rename = "fetch.max.bytes")]
    fetch_max_bytes: String,

    #[serde(default = "default_security_protocol")]
    #[serde(rename = "security.protocol")]
    security_protocol: String,

    #[serde(default)]
    #[serde(rename = "sasl.mechanism")]
    sasl_mechanism: Option<String>,

    #[serde(default)]
    #[serde(rename = "sasl.username")]
    sasl_username: Option<String>,

    #[serde(default)]
    #[serde(rename = "sasl.password")]
    sasl_password: Option<String>,
}

// Default functions
fn default_bootstrap_servers() -> String { "localhost:9092".to_string() }
fn default_group_id() -> String { "default-group".to_string() }
fn default_auto_offset_reset() -> String { "earliest".to_string() }
fn default_enable_auto_commit() -> String { "false".to_string() }
fn default_session_timeout_ms() -> String { "6000".to_string() }
fn default_heartbeat_interval_ms() -> String { "3000".to_string() }
fn default_max_poll_interval_ms() -> String { "300000".to_string() }
fn default_fetch_max_bytes() -> String { "52428800".to_string() }
fn default_security_protocol() -> String { "plaintext".to_string() }

impl ListenerConfig {
    /// Applies environment variable overrides for a specific listener.
    fn apply_env_overrides(&mut self, listener_id: &str) {
        let apply = |field: &mut String, key: &str| {
            let upper_id = listener_id.to_uppercase();
            // Try with underscores (e.g., KAFKA_LISTENER_1_BOOTSTRAP_SERVERS)
            let listener_key_underscore = format!("KAFKA_{}_{}", upper_id.replace('-', "_"), key);
            // Try with hyphens (e.g., KAFKA_LISTENER-1_BOOTSTRAP_SERVERS)
            let listener_key_hyphen = format!("KAFKA_{}_{}", upper_id, key);
            let global_key = format!("KAFKA_GLOBAL_{}", key);

            if let Ok(value) = env::var(&listener_key_underscore) {
                *field = value;
            } else if let Ok(value) = env::var(&listener_key_hyphen) {
                *field = value;
            } else if let Ok(value) = env::var(&global_key) {
                *field = value;
            }
        };

        let apply_option = |field: &mut Option<String>, key: &str| {
            let upper_id = listener_id.to_uppercase();
            let listener_key_underscore = format!("KAFKA_{}_{}", upper_id.replace('-', "_"), key);
            let listener_key_hyphen = format!("KAFKA_{}_{}", upper_id, key);
            let global_key = format!("KAFKA_GLOBAL_{}", key);

            if let Ok(value) = env::var(&listener_key_underscore) {
                *field = Some(value);
            } else if let Ok(value) = env::var(&listener_key_hyphen) {
                *field = Some(value);
            } else if let Ok(value) = env::var(&global_key) {
                *field = Some(value);
            }
        };

        apply(&mut self.bootstrap_servers, "BOOTSTRAP_SERVERS");
        apply(&mut self.group_id, "GROUP_ID");
        apply(&mut self.auto_offset_reset, "AUTO_OFFSET_RESET");
        apply(&mut self.enable_auto_commit, "ENABLE_AUTO_COMMIT");
        apply(&mut self.session_timeout_ms, "SESSION_TIMEOUT_MS");
        apply(&mut self.heartbeat_interval_ms, "HEARTBEAT_INTERVAL_MS");
        apply(&mut self.max_poll_interval_ms, "MAX_POLL_INTERVAL_MS");
        apply(&mut self.fetch_max_bytes, "FETCH_MAX_BYTES");
        apply(&mut self.security_protocol, "SECURITY_PROTOCOL");
        apply_option(&mut self.sasl_mechanism, "SASL_MECHANISM");
        apply_option(&mut self.sasl_username, "SASL_USERNAME");
        apply_option(&mut self.sasl_password, "SASL_PASSWORD");
    }

    /// Converts the listener config into a ClientConfig for rdkafka.
    pub fn to_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &self.group_id)
            .set("auto.offset.reset", &self.auto_offset_reset)
            .set("enable.auto.commit", &self.enable_auto_commit)
            .set("session.timeout.ms", &self.session_timeout_ms)
            .set("heartbeat.interval.ms", &self.heartbeat_interval_ms)
            .set("max.poll.interval.ms", &self.max_poll_interval_ms)
            .set("fetch.max.bytes", &self.fetch_max_bytes)
            .set("security.protocol", &self.security_protocol);

        if let Some(mechanism) = &self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism);
        }
        if let Some(username) = &self.sasl_username {
            config.set("sasl.username", username);
        }
        if let Some(password) = &self.sasl_password {
            config.set("sasl.password", password);
        }

        config
    }
}

/// Loads Kafka configuration from a YAML file and applies environment overrides.
pub fn load_config(yaml_path: &str, listener_id: &str) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let mut file = File::open(yaml_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let mut kafka_config: KafkaConfig = serde_yaml::from_str(&contents)?;

    let mut listener_config = kafka_config.kafka.remove(listener_id)
        .ok_or_else(|| format!("Listener ID '{}' not found in YAML config", listener_id))?;

    // Apply environment variable overrides
    listener_config.apply_env_overrides(listener_id);

    Ok(listener_config.to_client_config())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[cfg(test)]
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Force single-threaded execution for tests in this module
    #[cfg(test)]
    static TEST_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        let thread_count = TEST_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
        if thread_count == 0 {
            std::env::set_var("RUST_TEST_THREADS", "1");
        }
    }

    #[test]
    fn test_yaml_and_env_overrides() {
        // Create a temporary YAML file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "kafka:\n  listener-1:\n    bootstrap.servers: \"original:9092\"\n    group.id: \"original-group\"").unwrap();

        // Set environment variables
        env::set_var("KAFKA_LISTENER_1_BOOTSTRAP_SERVERS", "listener1:9092");
        env::set_var("KAFKA_GLOBAL_GROUP_ID", "global-group");

        let config = load_config(temp_file.path().to_str().unwrap(), "listener-1").unwrap();

        assert_eq!(config.get("bootstrap.servers").unwrap(), "listener1:9092"); // Listener-specific override
        assert_eq!(config.get("group.id").unwrap(), "global-group"); // Global override
        assert_eq!(config.get("auto.offset.reset").unwrap(), "earliest"); // Default

        // Clean up
        env::remove_var("KAFKA_LISTENER_1_BOOTSTRAP_SERVERS");
        env::remove_var("KAFKA_GLOBAL_GROUP_ID");
    }

    #[test]
    fn test_yaml_without_overrides() {
        // Create a temporary YAML file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "kafka:\n  listener-1:\n    bootstrap.servers: \"original:9092\"\n    group.id: \"original-group\"").unwrap();

        let config = load_config(temp_file.path().to_str().unwrap(), "listener-1").unwrap();

        assert_eq!(config.get("bootstrap.servers").unwrap(), "original:9092");
        assert_eq!(config.get("group.id").unwrap(), "original-group");
        assert_eq!(config.get("auto.offset.reset").unwrap(), "earliest");
    }

    #[test]
    fn test_yaml_with_global_override_only() {
        // Create a temporary YAML file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "kafka:\n  listener-1:\n    bootstrap.servers: \"original:9092\"\n    group.id: \"original-group\"").unwrap();

        env::set_var("KAFKA_GLOBAL_GROUP_ID", "global-group");

        let config = load_config(temp_file.path().to_str().unwrap(), "listener-1").unwrap();

        assert_eq!(config.get("bootstrap.servers").unwrap(), "original:9092");
        assert_eq!(config.get("group.id").unwrap(), "global-group");
        assert_eq!(config.get("auto.offset.reset").unwrap(), "earliest");

        // Clean up
        env::remove_var("KAFKA_GLOBAL_GROUP_ID");
    }
}
