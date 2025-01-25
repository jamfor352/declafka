use log::{debug, info, warn};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::task;

pub struct KafkaListener<T> {
    topic: String,
    brokers: String,
    group_id: String,
    shutdown_rx: watch::Receiver<bool>,
    handler: Arc<Box<dyn Fn(T) + Send + Sync>>,
    deserializer: Arc<Box<dyn Fn(&[u8]) -> Option<T> + Send + Sync>>, // ✅ Custom deserializer
}

impl<T> KafkaListener<T>
where
    T: DeserializeOwned + Send + 'static,
{
    pub fn new<F, D>(
        topic: &str,
        brokers: String,
        group_id: String,
        shutdown_rx: watch::Receiver<bool>,
        deserializer: D,
        handler: F,
    ) -> Self
    where
        F: Fn(T) + Send + Sync + 'static,
        D: Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    {
        KafkaListener {
            topic: topic.to_string(),
            brokers,
            group_id,
            shutdown_rx,
            handler: Arc::new(Box::new(handler)),
            deserializer: Arc::new(Box::new(deserializer)), // ✅ Store deserializer
        }
    }

    pub fn start(self) {
        task::spawn(async move {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", &self.group_id)
                .set("bootstrap.servers", &self.brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.offset.store", "false")
                .set("enable.auto.commit", "false")
                .create()
                .expect("Failed to create Kafka consumer");

            consumer
                .subscribe(&[&self.topic])
                .expect("Failed to subscribe to Kafka topic");

            info!("Kafka listener started for topic: {}", self.topic);

            let mut shutdown_rx = self.shutdown_rx.clone();

            loop {
                tokio::select! {
                    result = consumer.recv() => {
                        match result {
                            Err(e) => warn!("Kafka error: {}", e),
                            Ok(m) => {
                                if let Some(payload) = m.payload() {
                                    if let Some(parsed_msg) = (self.deserializer)(payload) {
                                        (self.handler)(parsed_msg);
                                    } else {
                                        warn!("Deserializer failed, skipping message. Raw payload: '{}'", String::from_utf8_lossy(payload));
                                    }
                                }

                                consumer.store_offset_from_message(&m).expect("Failed to store offset");
                                consumer.commit_message(&m, CommitMode::Async).expect("Failed to commit offset");
                                debug!("Committed offset {} for partition {}", m.offset(), m.partition());
                            }
                        }
                    },
                    _ = shutdown_rx.changed() => {
                        warn!("Shutting down Kafka listener...");
                        consumer.commit_consumer_state(CommitMode::Sync).expect("Failed to commit consumer state");
                        break;
                    }
                }
            }

            info!("Kafka listener for topic '{}' has stopped.", self.topic);
        });
    }
}