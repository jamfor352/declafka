use log::info;
use my_kafka_lib::{get_configuration, KafkaConfig};
use my_kafka_lib::OffsetReset::LATEST;
use my_kafka_macros::kafka_listener;
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
    deserializer = "string_deserializer"
)]
pub fn handle_normal_string(message: String) {
    info!("Received text message: {}", message);
}

#[kafka_listener(
    topic = "topic-json",
    config = "struct_config",
    deserializer = "my_struct_deserializer"
)]
pub fn handle_my_struct(message: MyStruct) {
    info!("Received JSON message with field 1 as: {} and field 2 as: {}", message.field1, message.field2);
}
