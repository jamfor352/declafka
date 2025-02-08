//! Core Kafka consumer implementation with offset tracking and graceful shutdown.
//! 
//! This crate provides the runtime components for Kafka message consumption,
//! including consumer management, offset tracking, and message deserialization.

use log::{debug, info, warn};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::client::ClientContext;
use rdkafka::{ClientConfig, Message, Offset};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tokio::task;

// Your existing OffsetReset enum and impl remain unchanged
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

struct CustomContext {
    topic: String,
    offset_tracker: Arc<Mutex<HashMap<i32, i64>>>,
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _base_consumer: &rdkafka::consumer::BaseConsumer<CustomContext>, rebalance: &rdkafka::consumer::Rebalance) {
        info!("Pre-rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, base_consumer: &rdkafka::consumer::BaseConsumer<CustomContext>, rebalance: &rdkafka::consumer::Rebalance) {
        info!("Post-rebalance: {:?}", rebalance);
        if let rdkafka::consumer::Rebalance::Revoke(partitions) = rebalance {
            let tracker = self.offset_tracker.lock().unwrap();
            let mut tpl = rdkafka::TopicPartitionList::new();

            for tp in partitions.elements() {
                if let Some(&offset) = tracker.get(&tp.partition()) {
                    tpl.add_partition_offset(&self.topic, tp.partition(), Offset::Offset(offset))
                        .unwrap_or_else(|e| warn!("Failed to add partition to TPL: {}", e));
                }
            }

            if tpl.count() > 0 {
                if let Err(e) = base_consumer.commit(&tpl, CommitMode::Sync) {
                    warn!("Failed to commit offsets during rebalance: {}", e);
                }
            }
        }
    }
}

pub struct KafkaListener<T> {
    topic: String,
    client_config: KafkaConfig,
    offset_tracker: Arc<Mutex<HashMap<i32, i64>>>,
    handler: Arc<Box<dyn Fn(T) + Send + Sync>>,
    deserializer: Arc<Box<dyn Fn(&[u8]) -> Option<T> + Send + Sync>>,
    is_running: Arc<Mutex<bool>>,
}

impl<T> KafkaListener<T>
where
    T: DeserializeOwned + Send + 'static,
{
    // Tracks message offsets per partition to ensure at-least-once delivery
    // even during ungraceful shutdowns
    pub fn new<F, D>(
        topic: &str,
        client_config: KafkaConfig,
        deserializer: D,
        handler: F,
    ) -> Self
    where
        F: Fn(T) + Send + Sync + 'static,
        D: Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    {
        KafkaListener {
            topic: topic.to_string(),
            client_config,
            offset_tracker: Arc::new(Mutex::new(HashMap::new())),
            handler: Arc::new(Box::new(handler)),
            deserializer: Arc::new(Box::new(deserializer)),
            is_running: Arc::new(Mutex::new(false)),
        }
    }

    // Spawns the consumer task and sets up shutdown signal handling
    pub fn start(self) {
        tokio::spawn(async move {
            wait_for_shutdown().await;
            info!("Shutdown signal received, notifying Kafka consumer...");
        });

        task::spawn(async move {
            let context = CustomContext {
                topic: self.topic.clone(),
                offset_tracker: self.offset_tracker.clone(),
            };

            let consumer: StreamConsumer<CustomContext> = make(self.client_config.clone(), context)
                .create_with_context(CustomContext {
                    topic: self.topic.clone(),
                    offset_tracker: self.offset_tracker.clone(),
                })
                .expect("Failed to create Kafka consumer");

            consumer
                .subscribe(&[&self.topic])
                .expect("Failed to subscribe to Kafka topic");

            {
                let mut is_running_lock = self.is_running.lock().unwrap();
                *is_running_lock = true;
                info!("Kafka listener started for topic: {}", self.topic);
            }

            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
            tokio::spawn(async move {
                wait_for_shutdown().await;
                info!("Shutdown signal received, notifying Kafka consumers...");
                shutdown_tx.send(true).unwrap();
            });

            while *self.is_running.lock().unwrap() {
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
                                    if let Some(parsed_msg) = (self.deserializer)(payload) {
                                        (self.handler)(parsed_msg);
                                    } else {
                                        warn!("Deserializer failed, skipping message. Raw payload: '{}'", String::from_utf8_lossy(payload));
                                    }
                                }

                                consumer.store_offset_from_message(&m).expect("Failed to store offset");
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
                            debug!("Kafka listener committed: partition {}, offset: {} for consumer group: {}",
                                  partition, offset, self.client_config.consumer_group);
                        }

                        {
                            let mut is_running_lock = self.is_running.lock().unwrap();
                            *is_running_lock = false;
                            debug!("Shutting down Kafka listener...");
                        };
                    }
                }
            }

            info!("Kafka listener for topic '{}' has stopped.", self.topic);
        });
    }
}

fn make(kafka_config: KafkaConfig, _context: CustomContext) -> ClientConfig {
    ClientConfig::new()
        .set("bootstrap.servers", kafka_config.bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", kafka_config.auto_offset_reset.as_str())
        .set("enable.auto.commit", "false")
        .set("group.id", kafka_config.consumer_group)
        .clone()
}

// Your existing get_configuration function remains unchanged
pub fn get_configuration() -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string()),
        auto_offset_reset: OffsetReset::EARLIEST,
        consumer_group: env::var("DEFAULT_CONSUMER_GROUP").unwrap_or_else(|_| "test-service".to_string()),
    }
}

// Your existing wait_for_shutdown function remains unchanged
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigquit = signal(SignalKind::quit()).unwrap();

        tokio::select! {
            _ = sigint.recv() => {
                info!("Received SIGINT (Ctrl+C), shutting down...");
            },
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
            },
            _ = sigquit.recv() => {
                info!("Received SIGQUIT (Ctrl+\\), shutting down...");
            },
        }
    }

    #[cfg(windows)]
    {
        let _ = signal::ctrl_c().await;
        info!("Received Ctrl+C or termination signal, shutting down...");
    }
}