use crate::handler::handler::wait_for_shutdown;
use crate::kafka::kafka::{get_configuration, KafkaListener};
use crate::utils::deserializers::{my_struct_deserializer, string_deserializer};
use actix_web::HttpServer;
use customersvc::app;
use log::info;
use std::env;
use rdkafka::ClientConfig;
use tokio::sync::watch;
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

    let config = get_configuration();

    // ✅ JSON Deserializer (for structured messages)
    let listener_json = KafkaListener::new(
        "topic-json",
        config.clone(),
        shutdown_rx.clone(),
        my_struct_deserializer(),  // ✅ JSON deserializer
        handle_my_struct,
    );

    // ✅ Plain Text Deserializer (for raw string messages)
    let listener_text = KafkaListener::new(
        "blah",
        config,
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
