use log::{info, error};
use declafka_lib::Error;
use declafka_macro::kafka_listener;
use utils::deserializers::{string_deserializer, my_struct_deserializer};
use crate::models::models::MyStruct;
use crate::utils;

#[kafka_listener(
    topic = "topic-text",
    listener_id = "string-listener",
    yaml_path = "example-app-kafka.yaml",
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
    listener_id = "json-listener",
    yaml_path = "example-app-kafka.yaml",
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
