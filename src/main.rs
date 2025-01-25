use crate::handler::handler::wait_for_shutdown;
use crate::listeners::listeners::consume_and_print;
use actix_web::HttpServer;
use customersvc::app;
use log::info;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

mod listeners;
mod handler;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    // Read Kafka settings from environment variables
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| {
        println!("No value for KAFKA_BROKERS set in, defaulting to localhost:9092");
        String::from("localhost:9092")
    });
    let group_id = env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| {
        println!("No value for KAFKA_GROUP_ID set in, defaulting to example_consumer_group_id");
        String::from("example_consumer_group_id")
    });

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let kafka_shutdown_rx = shutdown_rx.clone();

    // Create offset tracker
    let offset_tracker = Arc::new(Mutex::new(HashMap::new()));
    // Spawn Kafka consumer in the background
    let kafka_offset_tracker = offset_tracker.clone();
    tokio::spawn(async move {
        consume_and_print(&brokers, &group_id, kafka_shutdown_rx, kafka_offset_tracker).await;
    });

    let server = HttpServer::new(|| app())
        .bind("127.0.0.1:8080")?
        .run();
    let server_handle = server.handle();

    tokio::spawn(async move {
        wait_for_shutdown().await;  // ✅ Wait for any shutdown event
        info!("Shutdown signal received, notifying tasks...");
        shutdown_tx.send(true).unwrap();
        server_handle.stop(true).await;
    });

    server.await
}
