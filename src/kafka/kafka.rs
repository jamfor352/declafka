use log::{debug, info, warn};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::task;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    EARLIEST,
    LATEST
}

impl OffsetReset {
    pub fn as_str(&self) -> &'static str {
        match self {
            OffsetReset::EARLIEST => "earliest",
            OffsetReset::LATEST => "latest",
        }
    }
}

#[derive(Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub auto_offset_reset: OffsetReset,
    pub consumer_group: String,
}

pub struct KafkaListener<T> {
    topic: String,
    client_config: KafkaConfig,
    shutdown_rx: watch::Receiver<bool>,  // ✅ Store `Receiver<bool>` directly, NOT inside Arc
    offset_tracker: Arc<Mutex<HashMap<i32, i64>>>,
    handler: Arc<Box<dyn Fn(T) + Send + Sync>>,  // ✅ Use Arc to allow multiple uses
    deserializer: Arc<Box<dyn Fn(&[u8]) -> Option<T> + Send + Sync>>, // ✅ Custom deserializer
}

impl<T> KafkaListener<T>
where
    T: DeserializeOwned + Send + 'static,
{
    /// Creates a new KafkaListener for a given topic
    pub fn new<F, D>(
        topic: &str,
        client_config: KafkaConfig,
        shutdown_rx: watch::Receiver<bool>,
        deserializer: D,  // ✅ Accept custom deserializer
        handler: F,
    ) -> Self
    where
        F: Fn(T) + Send + Sync + 'static,
        D: Fn(&[u8]) -> Option<T> + Send + Sync + 'static,  // ✅ Generic deserializer
    {
        KafkaListener {
            topic: topic.to_string(),
            client_config,
            shutdown_rx,
            offset_tracker: Arc::new(Mutex::new(HashMap::new())),
            handler: Arc::new(Box::new(handler)),
            deserializer: Arc::new(Box::new(deserializer)), // ✅ Store deserializer
        }
    }

    /// Starts the Kafka consumer loop
    pub fn start(self) {
        task::spawn(async move {
            let consumer: StreamConsumer =
                make(self.client_config.clone())
                    .create()
                    .expect("Failed to create Kafka consumer");

            consumer
                .subscribe(&[&self.topic])
                .expect("Failed to subscribe to Kafka topic");

            info!("Kafka listener started for topic: {}", self.topic);

            let mut shutdown_rx = self.shutdown_rx.clone();  // ✅ Clone it before `tokio::select!`

            loop {
                tokio::select! {
                    result = consumer.recv() => {
                        match result {
                            Err(e) => warn!("Kafka error: {}", e),
                            Ok(m) => {
                                let current_offset = m.offset();
                                let partition = m.partition();
                                info!("Received a message on partition {}, offset {}", partition, current_offset);
                                let next_offset = m.offset() + 1;

                                let mut tracker = self.offset_tracker.lock().unwrap();
                                tracker.insert(partition, next_offset);

                                if let Some(payload) = m.payload() {
                                    if let Some(parsed_msg) = (self.deserializer)(payload) {  // ✅ Call the custom deserializer
                                        (self.handler)(parsed_msg);
                                    } else {
                                        warn!("Deserializer failed, skipping message. Raw payload: '{}'", String::from_utf8_lossy(payload));
                                    }
                                }

                                consumer.store_offset_from_message(&m).expect("Failed to store offset");  // ✅ Ensure Kafka knows what we're committing
                                consumer.commit_message(&m, CommitMode::Async).expect("Failed to commit offset");
                                debug!("Kafka listener async committed offset {} for partition {}", next_offset, m.partition());
                            }
                        }
                    },
                    _ = shutdown_rx.changed() => {
                        warn!("Kafka listener shutting down gracefully...");

                        let tracker = self.offset_tracker.lock().unwrap();
                        for (&partition, &offset) in tracker.iter() {
                            let mut topic_partition_list = rdkafka::TopicPartitionList::new();
                            topic_partition_list.add_partition_offset(&self.topic, partition, Offset::Offset(offset)).unwrap();
                            consumer.commit(&topic_partition_list, CommitMode::Sync).unwrap();
                            info!("Kafka listener committed: partition {}, offset: {} for consumer group: {}", partition, offset, self.client_config.consumer_group);
                        }

                        break;
                    }
                }
            }

            info!("Kafka listener for topic '{}' has stopped.", self.topic);
        });
    }
}

fn make(kafka_config: KafkaConfig) -> ClientConfig {
    let debug = ClientConfig::new()
        .set("bootstrap.servers", kafka_config.bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", kafka_config.auto_offset_reset.as_str())  // ✅ Prevents unexpected in-memory overwrites
        .set("enable.auto.commit", "false")
        .set("group.id", kafka_config.consumer_group)
        .clone();
    debug
}

pub fn get_configuration() -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string()),
        auto_offset_reset: OffsetReset::EARLIEST,
        consumer_group: env::var("DEFAULT_CONSUMER_GROUP").unwrap_or_else(|_| "test-service".to_string()),
    }
}
