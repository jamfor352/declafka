
use crate::utils::deserializers::{my_struct_deserializer, string_deserializer};
use crate::routes::routes::app;
use actix_web::HttpServer;
use log::info;
use my_kafka_lib::{get_configuration, KafkaConfig};
use my_kafka_lib::OffsetReset::LATEST;
use my_kafka_macros::kafka_listener;
use crate::models::models::MyStruct;

mod listeners;
mod models;
mod utils;
mod routes;


#[kafka_listener(
    topic = "topic-text",
    config = "get_configuration",
    deserializer = "string_deserializer"
)]
fn handle_text_message(msg: String) {
    info!("Received TEXT: {}", msg);
}

pub fn json_config() -> KafkaConfig {
    let mut cfg = get_configuration();
    cfg.auto_offset_reset = LATEST;
    cfg.consumer_group = "test-json-consumer-group".to_owned();
    cfg
}

#[kafka_listener(
    topic = "topic-json",
    config = "get_configuration",
    deserializer = "my_struct_deserializer"
)]
fn handle_json_message(msg: MyStruct) {
    info!("Received JSON: field1='{}', field2={}", msg.field1, msg.field2);
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // init logs
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // We can now build them:
    let text_listener = handle_text_message_listener();
    let json_listener = handle_json_message_listener();

    // Start them
    text_listener.start();
    json_listener.start();


    HttpServer::new(|| {app() })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
