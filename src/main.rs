use crate::handler::handler::wait_for_shutdown;
use crate::listeners::listeners::KafkaListener;
use actix_web::HttpServer;
use customersvc::app;
use log::info;
use std::env;
use tokio::sync::watch;

mod listeners;
mod handler;

#[derive(Debug, serde::Deserialize)]
struct MyStruct {
    field1: String,
    field2: i32,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let group_id = env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "my-test-group".to_string());

    let (shutdown_tx, shutdown_rx) = watch::channel(false); // ✅ Create a watch channel for shutdown
    let shutdown_rx_json = shutdown_rx.clone();
    let shutdown_rx_text = shutdown_rx.clone();

    // ✅ JSON Deserializer (for structured messages)
    let listener_json = KafkaListener::new(
        "topic-json",
        brokers.clone(),
        group_id.clone(),
        shutdown_rx_json,
        |payload| serde_json::from_slice::<MyStruct>(payload).ok(),  // ✅ JSON deserializer
        |message: MyStruct| {
            info!("Received JSON message with field 1 as: {} and field 2 as: {}", message.field1, message.field2);
        },
    );

    // ✅ Plain Text Deserializer (for raw string messages)
    let listener_text = KafkaListener::new(
        "topic-text",
        brokers.clone(),
        group_id.clone(),
        shutdown_rx_text,
        |payload| Some(String::from_utf8_lossy(payload).to_string()),  // ✅ Text deserializer
        |message: String| {
            info!("Received text message: {}", message);
        },
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
