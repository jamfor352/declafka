use crate::handler::handler::wait_for_shutdown;
use crate::kafka::kafka::{get_configuration, KafkaConfig, KafkaListener};
use crate::utils::deserializers::{my_struct_deserializer, string_deserializer};
use actix_web::HttpServer;
use customersvc::app;
use log::info;
use tokio::sync::watch;
use crate::kafka::kafka::OffsetReset::{EARLIEST, LATEST};
use crate::listeners::listeners::{handle_my_struct, handle_normal_string};

mod handler;
mod kafka;
mod listeners;
mod models;
mod utils;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

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
        shutdown_rx.clone(),
        my_struct_deserializer(),  // ✅ JSON deserializer
        handle_my_struct,
    );

    // ✅ Plain Text Deserializer (for raw string messages)
    let listener_text = KafkaListener::new(
        "topic-test",
        default_config,
        shutdown_rx.clone(),
        string_deserializer(),  // ✅ Text deserializer
        handle_normal_string,
    );

    listener_json.start();
    listener_text.start();

    let server = HttpServer::new(|| app())
        .bind("127.0.0.1:8080")?
        .run();
    let server_handle = server.handle();

    tokio::spawn(async move {
        wait_for_shutdown().await;
        info!("Shutdown signal received, notifying Kafka consumers...");
        shutdown_tx.send(true).unwrap();
        server_handle.stop(true).await;
    });

    server.await
}
