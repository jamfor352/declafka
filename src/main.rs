use crate::kafka::kafka::OffsetReset::LATEST;
use crate::kafka::kafka::{get_configuration, KafkaConfig, KafkaListener};
use crate::listeners::listeners::{handle_my_struct, handle_normal_string};
use crate::utils::deserializers::{my_struct_deserializer, string_deserializer};
use crate::routes::routes::app;
use actix_web::HttpServer;

mod kafka;
mod listeners;
mod models;
mod utils;
mod routes;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let default_config = get_configuration();

    let json_config = KafkaConfig {
        bootstrap_servers: default_config.bootstrap_servers.clone(),
        auto_offset_reset: LATEST,
        consumer_group: "test-json-consumer-group".to_string()
    };

    // ✅ JSON Deserializer (for structured messages)
    let listener_json = KafkaListener::new(
        "topic-json",
        json_config,
        my_struct_deserializer(),  // ✅ JSON deserializer
        handle_my_struct,
    );

    // ✅ Plain Text Deserializer (for raw string messages)
    let listener_text = KafkaListener::new(
        "topic-test",
        default_config,
        string_deserializer(),  // ✅ Text deserializer
        handle_normal_string,
    );

    listener_json.start();
    listener_text.start();

    HttpServer::new(|| {app() })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
