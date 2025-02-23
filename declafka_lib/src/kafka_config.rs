use log::info;
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
    #[serde(flatten)]
    properties: HashMap<String, String>
}

impl ListenerConfig {
    /// Applies environment variable overrides for a specific listener.
    fn apply_env_overrides(&mut self, listener_id: &str) {
        // Process all environment variables that start with KAFKA_
        for (key, value) in env::vars() {
            let upper_id = listener_id.to_uppercase();
            let listener_prefix_underscore = format!("KAFKA_{}_{}", upper_id.replace('-', "_"), "");
            let listener_prefix_hyphen = format!("KAFKA_{}_{}", upper_id, "");
            let global_prefix = "KAFKA_GLOBAL_";

            // Check if this env var is for our listener or global
            if let Some(kafka_key) = if key.starts_with(&listener_prefix_underscore) {
                Some(key[listener_prefix_underscore.len()..].to_string())
            } else if key.starts_with(&listener_prefix_hyphen) {
                Some(key[listener_prefix_hyphen.len()..].to_string())
            } else if key.starts_with(global_prefix) {
                Some(key[global_prefix.len()..].to_string())
            } else {
                None
            } {
                // Convert back to Kafka property format
                let kafka_key = kafka_key.to_lowercase().replace('_', ".");
                self.properties.insert(kafka_key, value); 
            }
        }
    }

    /// Converts the listener config into a ClientConfig for rdkafka.
    pub fn to_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        
        // Apply all properties
        for (key, value) in &self.properties {
            config.set(key, value);
        }

        config
    }

    /// Merges default config into this config, keeping existing values
    fn merge_defaults(&mut self, defaults: &ListenerConfig) {
        // Merge all properties from defaults that don't exist in self
        for (key, value) in &defaults.properties {
            self.properties.entry(key.clone()).or_insert_with(|| value.clone());
        }
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

    // Apply defaults if they exist
    if let Some(default_config) = kafka_config.kafka.get("default") {
        listener_config.merge_defaults(default_config);
    }

    // Apply environment variable overrides (after defaults)
    listener_config.apply_env_overrides(listener_id);
    let actual_client_config = listener_config.to_client_config();
    info!("Config for listener: {}: {:?}", listener_id, actual_client_config);

    Ok(actual_client_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[cfg(test)]
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[cfg(test)]
    static TEST_THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

    static TEMP_FILE: std::sync::LazyLock<NamedTempFile> = std::sync::LazyLock::new(|| {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config_yaml_raw = r#"
        kafka:
            default:
                auto.offset.reset: earliest
            listener-1:
                bootstrap.servers: original:9092
                group.id: original-group
        "#;
        writeln!(temp_file, "{}", config_yaml_raw).unwrap();
        temp_file
    });

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
        env::set_var("KAFKA_LISTENER_1_BOOTSTRAP_SERVERS", "listener1:9092");
        env::set_var("KAFKA_LISTENER_1_AUTO_OFFSET_RESET", "latest");
        env::set_var("KAFKA_GLOBAL_GROUP_ID", "global-group");

        let config = load_config(TEMP_FILE.path().to_str().unwrap(), "listener-1").unwrap();

        assert_eq!(config.get("bootstrap.servers").unwrap(), "listener1:9092");
        assert_eq!(config.get("group.id").unwrap(), "global-group");
        assert_eq!(config.get("auto.offset.reset").unwrap(), "latest");

        env::remove_var("KAFKA_LISTENER_1_BOOTSTRAP_SERVERS");
        env::remove_var("KAFKA_LISTENER_1_AUTO_OFFSET_RESET");
        env::remove_var("KAFKA_GLOBAL_GROUP_ID");
    }

    #[test]
    fn test_yaml_without_overrides() {

        let config = load_config(TEMP_FILE.path().to_str().unwrap(), "listener-1").unwrap();

        assert_eq!(config.get("bootstrap.servers").unwrap(), "original:9092");
        assert_eq!(config.get("group.id").unwrap(), "original-group");
        assert_eq!(config.get("auto.offset.reset").unwrap(), "earliest");
    }

    #[test]
    fn test_yaml_with_global_override_only() {
        env::set_var("KAFKA_GLOBAL_GROUP_ID", "global-group");

        let config = load_config(TEMP_FILE.path().to_str().unwrap(), "listener-1").unwrap();

        assert_eq!(config.get("bootstrap.servers").unwrap(), "original:9092");
        assert_eq!(config.get("group.id").unwrap(), "global-group");
        assert_eq!(config.get("auto.offset.reset").unwrap(), "earliest");

        env::remove_var("KAFKA_GLOBAL_GROUP_ID");
    }

    #[test]
    fn test_yaml_with_default_inheritance() {
        let default_inheritance_yaml = r#"
        kafka:
            default:
                bootstrap.servers: "localhost:19092"
                auto.offset.reset: "earliest"
            test-listener:
                group.id: "test-listener-group"
        "#;

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "{}", default_inheritance_yaml).unwrap();

        let config = load_config(temp_file.path().to_str().unwrap(), "test-listener").unwrap();

        assert_eq!(config.get("bootstrap.servers").unwrap(), "localhost:19092");
        assert_eq!(config.get("group.id").unwrap(), "test-listener-group");
        assert_eq!(config.get("auto.offset.reset").unwrap(), "earliest");
    }
}
