use log::{info, error};
use declafka_lib::{get_configuration, KafkaConfig, RetryConfig, Error, KafkaListener};
use declafka_lib::OffsetReset::LATEST;
use declafka_macro::kafka_listener;
use utils::deserializers::{string_deserializer, my_struct_deserializer};
use crate::models::models::MyStruct;
use crate::utils;


pub fn struct_config() -> KafkaConfig {
    let mut cfg = get_configuration();
    cfg.auto_offset_reset = LATEST;
    cfg.consumer_group = "test-json-consumer-group".to_owned();
    cfg
}

#[kafka_listener(
    topic = "topic-text",
    config = "get_configuration",
    deserializer = "string_deserializer",
    dlq_topic = "topic-text-dlq",
    retry_max_attempts = 2,
    retry_initial_backoff = 50,
    retry_max_backoff = 5000,
    retry_multiplier = 2.0
)]
pub fn handle_normal_string(message: String) -> Result<(), Error> {
    info!("Received text message: {}", message);
    if message.contains("error") {
        return Err(Error::ProcessingFailed("Message contains error".into()));
    }
    Ok(())
}

#[kafka_listener(
    topic = "topic-json",
    config = "struct_config",
    deserializer = "my_struct_deserializer",
    dlq_topic = "topic-json-dlq",
    retry_max_attempts = 3,
    retry_initial_backoff = 100,
    retry_max_backoff = 10000,
    retry_multiplier = 2.0
)]
pub fn handle_my_struct(message: MyStruct) -> Result<(), Error> {
    if message.field2 < 0 {
        error!("Negative value in field2: {}", message.field2);
        return Err(Error::ValidationFailed("field2 must be positive".into()));
    }
    
    info!("Received JSON message with field 1 as: {} and field 2 as: {}", 
          message.field1, message.field2);
    Ok(())
}

fn create_retry_config(attempts: u32, initial_ms: u64, max_ms: u64) -> RetryConfig {
    RetryConfig {
        max_attempts: attempts,
        initial_backoff_ms: initial_ms,
        max_backoff_ms: max_ms,
        backoff_multiplier: 2.0,
    }
}

pub fn get_configured_listeners() -> (KafkaListener<String>, KafkaListener<MyStruct>) {
    let string_listener = handle_normal_string_listener()
        .with_retry_config(create_retry_config(2, 50, 5000))
        .with_dead_letter_queue("topic-text-dlq");

    let struct_listener = handle_my_struct_listener()
        .with_retry_config(create_retry_config(3, 100, 10000))
        .with_dead_letter_queue("topic-json-dlq");

    (string_listener, struct_listener)
}
