use crate::listeners::listeners::consume_and_print;
use actix_web::HttpServer;
use customersvc::app;
use std::env;
use std::sync::Arc;
use tokio::sync::Notify;

mod listeners;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    // Read Kafka settings from environment variables
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let group_id = env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "example_consumer_group_id".to_string());

    let shutdown_notify = Arc::new(Notify::new());

    // Spawn Kafka consumer in the background
    let kafka_shutdown_notify = shutdown_notify.clone();

    // Run Kafka consumer in the background
    tokio::spawn(async move {
        consume_and_print(&brokers, &group_id, kafka_shutdown_notify).await;
    });

    HttpServer::new(|| app())
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
