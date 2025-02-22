//! Robust Kafka consumer implementation designed for reliability and fault tolerance.
//! 
//! This module provides a Kafka consumer that ensures:
//! - **Offset Tracking**: Manually manages offsets to prevent data loss or duplication during failures or rebalances.
//! - **Graceful Shutdown**: Responds to shutdown signals to commit offsets and exit cleanly, maintaining data integrity.
//! - **Retry Mechanisms**: Implements exponential backoff retries to handle transient failures without overloading the system.
//! - **Dead-Letter Queue (DLQ) Support**: Routes unprocessable messages to a DLQ for later analysis, preventing message loss.
//! 
//! These features are crucial for building resilient Kafka consumers in distributed systems where failures are inevitable.

use log::{debug, info, warn, error};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::client::ClientContext;
use rdkafka::{ClientConfig, Message, Offset};
use rdkafka::message::BorrowedMessage;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tokio::task;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use std::time::Duration;
use serde_json::json;
use std::time::Instant;
use chrono;

pub mod kafka_config;
use kafka_config::load_config;

// -----------------------------------------------------------------------------
// CustomContext & Offset Tracker
// -----------------------------------------------------------------------------

#[derive(Clone)]
struct CustomContext {
    offset_tracker: Arc<Mutex<HashMap<(String, i32), i64>>>,
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
                if let Some(&offset) = tracker.get(&(tp.topic().to_string(), tp.partition())) {
                    tpl.add_partition_offset(tp.topic(), tp.partition(), Offset::Offset(offset))
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

// -----------------------------------------------------------------------------
// RetryConfig, Error, Metrics, HealthCheck
// -----------------------------------------------------------------------------

pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10000,
            backoff_multiplier: 2.0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Error {
    NoPayload,
    DeserializationFailed,
    ProcessingFailed(String),
    ValidationFailed(String),
}

struct Metrics {
    messages_processed: AtomicU64,
    messages_failed: AtomicU64,
    retry_attempts: AtomicU64,
    dead_letter_messages: AtomicU64,
    total_processing_time_ms: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            messages_failed: AtomicU64::new(0),
            retry_attempts: AtomicU64::new(0),
            dead_letter_messages: AtomicU64::new(0),
            total_processing_time_ms: AtomicU64::new(0),
        }
    }
}

pub struct HealthCheck {
    pub is_running: bool,
    pub messages_processed: u64,
    pub messages_failed: u64,
    pub retry_attempts: u64,
    pub dead_letter_messages: u64,
    pub avg_processing_time_ms: f64,
}

// -----------------------------------------------------------------------------
// KafkaListener
// -----------------------------------------------------------------------------

pub struct KafkaListener<T>
where
    T: DeserializeOwned + Send + Clone + 'static,
{
    topic: String,
    listener_id: String,
    yaml_path: String,
    offset_tracker: Arc<Mutex<HashMap<(String, i32), i64>>>,
    handler: Box<dyn Fn(T) -> Result<(), Error> + Send + Sync>,
    deserializer: Box<dyn Fn(&[u8]) -> Option<T> + Send + Sync>,
    is_running: Arc<AtomicBool>,
    retry_config: RetryConfig,
    metrics: Arc<Metrics>,
    dead_letter_producer: Option<FutureProducer>,
    dlq_topic: Option<String>,
}

impl<T> KafkaListener<T>
where
    T: DeserializeOwned + Send + Clone + 'static,
{
    pub fn new<F, D>(
        topic: &str,
        listener_id: &str,
        yaml_path: &str,
        deserializer: D,
        handler: F,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: Fn(T) -> Result<(), Error> + Send + Sync + 'static,
        D: Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    {
        // Validate the config exists during initialization
        let _ = load_config(yaml_path, listener_id)?;

        Ok(Self {
            topic: topic.to_string(),
            listener_id: listener_id.to_string(),
            yaml_path: yaml_path.to_string(),
            offset_tracker: Arc::new(Mutex::new(HashMap::new())),
            handler: Box::new(handler),
            deserializer: Box::new(deserializer),
            is_running: Arc::new(AtomicBool::new(false)),
            retry_config: RetryConfig::default(),
            metrics: Arc::new(Metrics::new()),
            dead_letter_producer: None,
            dlq_topic: None,
        })
    }

    pub fn start(self) {
        tokio::spawn(async move {
            wait_for_shutdown().await;
            info!("Shutdown signal received, notifying Kafka consumer...");
        });

        task::spawn(async move {
            let context = CustomContext {
                offset_tracker: self.offset_tracker.clone(),
            };

            let consumer: StreamConsumer<CustomContext> = {
                let config = load_config(&self.yaml_path, &self.listener_id)
                    .expect("Failed to load Kafka config from YAML");
                config.create_with_context(context)
                    .expect("Failed to create Kafka consumer")
            };

            consumer
                .subscribe(&[&self.topic])
                .expect("Failed to subscribe to Kafka topic");

            self.is_running.store(true, Ordering::SeqCst);
            info!("Kafka listener started for topic: {}", self.topic);

            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

            tokio::spawn(async move {
                wait_for_shutdown().await;
                info!("Shutdown signal received, notifying Kafka consumers...");
                let _ = shutdown_tx.send(true);
            });

            'main: while self.is_running.load(Ordering::SeqCst) {
                tokio::select! {
                    result = consumer.recv() => {
                        match result {
                            Err(e) => warn!("Kafka error: {}", e),
                            Ok(m) => {
                                if m.topic() != self.topic {
                                    debug!("Received message for topic {} (expected {}), skipping", m.topic(), self.topic);
                                    continue;
                                }
                                let current_offset = m.offset();
                                let partition = m.partition();
                                debug!("Received message on partition {}, offset {}", partition, current_offset);

                                {
                                    let mut tracker = self.offset_tracker.lock().unwrap();
                                    tracker.insert((m.topic().to_string(), partition), current_offset + 1);
                                }

                                if let Err(e) = self.process_message(&m).await {
                                    if self.dead_letter_producer.is_some() {
                                        warn!("Message processing failed (DLQ enabled): {:?}", e);
                                    } else {
                                        warn!("Message processing failed (no DLQ configured): {:?}", e);
                                    }
                                }
                            }
                        }
                    },
                    _ = shutdown_rx.changed() => {
                        warn!("Kafka listener shutting down gracefully...");
                        {
                            let tracker = self.offset_tracker.lock().unwrap();
                            for ((topic, partition), &offset) in tracker.iter() {
                                let mut tpl = rdkafka::TopicPartitionList::new();
                                tpl.add_partition_offset(topic, *partition, Offset::Offset(offset))
                                    .unwrap_or_else(|e| warn!("Failed to add partition to TPL: {}", e));
                                if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
                                    warn!("Failed to commit offset during shutdown: {}", e);
                                }
                            }
                        }
                        self.is_running.store(false, Ordering::SeqCst);
                        break 'main;
                    }
                }
            }

            info!("Kafka listener for topic '{}' has stopped.", self.topic);
            Ok::<(), Error>(())
        });
    }

    pub fn get_health_check(&self) -> HealthCheck {
        HealthCheck {
            is_running: self.is_running.load(Ordering::SeqCst),
            messages_processed: self.metrics.messages_processed.load(Ordering::Relaxed),
            messages_failed: self.metrics.messages_failed.load(Ordering::Relaxed),
            retry_attempts: self.metrics.retry_attempts.load(Ordering::Relaxed),
            dead_letter_messages: self.metrics.dead_letter_messages.load(Ordering::Relaxed),
            avg_processing_time_ms: self.calculate_avg_processing_time(),
        }
    }

    pub fn export_prometheus_metrics(&self) -> String {
        format!(
            "kafka_consumer_messages_processed {}\n\
             kafka_consumer_messages_failed {}\n\
             kafka_consumer_retry_attempts {}\n\
             kafka_consumer_dead_letter_messages {}\n\
             kafka_consumer_avg_processing_time_ms {}\n",
            self.metrics.messages_processed.load(Ordering::Relaxed),
            self.metrics.messages_failed.load(Ordering::Relaxed),
            self.metrics.retry_attempts.load(Ordering::Relaxed),
            self.metrics.dead_letter_messages.load(Ordering::Relaxed),
            self.calculate_avg_processing_time(),
        )
    }

    fn calculate_avg_processing_time(&self) -> f64 {
        let total_time = self.metrics.total_processing_time_ms.load(Ordering::Relaxed);
        let total_messages = self.metrics.messages_processed.load(Ordering::Relaxed);
        if total_messages == 0 {
            0.0
        } else {
            total_time as f64 / total_messages as f64
        }
    }

    async fn send_to_dlq(
        &self,
        producer: &FutureProducer,
        original_msg: &BorrowedMessage<'_>,
        error: Error,
    ) -> Result<(), rdkafka::error::KafkaError> {
        let msg_topic = original_msg.topic();
        warn!("Starting DLQ process for message from topic: {}", msg_topic);

        let dlq_topic = self.dlq_topic.as_ref()
            .expect("DLQ topic must be set when dead_letter_producer is Some");

        warn!("Preparing to send message to DLQ topic: {}", dlq_topic);
        warn!("Original message details - Partition: {}, Offset: {}, Error: {:?}", 
              original_msg.partition(), original_msg.offset(), error);

        let dlq_payload = json!({
            "original_topic": msg_topic,
            "original_partition": original_msg.partition(),
            "original_offset": original_msg.offset(),
            "error": format!("{:?}", error),
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": String::from_utf8_lossy(original_msg.payload().unwrap_or_default()),
        });

        let payload_str = serde_json::to_string(&dlq_payload).unwrap();
        warn!("DLQ payload prepared: {}", payload_str);

        let record = FutureRecord::to(dlq_topic)
            .payload(&payload_str)
            .key(original_msg.key().unwrap_or_default());

        warn!("Attempting to send message to DLQ...");
        match producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                self.metrics.dead_letter_messages.fetch_add(1, Ordering::Relaxed);
                warn!("Successfully sent to DLQ topic {} (partition: {}, offset: {})", 
                      dlq_topic, partition, offset);
                Ok(())
            },
            Err((e, _)) => {
                error!("Failed to send to DLQ topic {}: {}", dlq_topic, e);
                Err(e)
            }
        }
    }

    async fn process_message(&self, msg: &BorrowedMessage<'_>) -> Result<(), Error> {
        debug!("Processing message from topic: {}, partition: {}, offset: {}", 
               msg.topic(), msg.partition(), msg.offset());

        if msg.topic() != self.topic {
            debug!("Skipping message from topic {} (expected {})", msg.topic(), self.topic);
            return Ok(());
        }

        let start = Instant::now();
        let payload = match msg.payload() {
            Some(p) => {
                debug!("Payload received, size: {} bytes", p.len());
                p
            },
            None => {
                warn!("No payload found in message");
                let error = Error::NoPayload;
                if let Some(producer) = &self.dead_letter_producer {
                    warn!("Sending message to DLQ due to no payload");
                    if let Err(e) = self.send_to_dlq(producer, msg, error.clone()).await {
                        error!("Failed to send message to DLQ: {}", e);
                    }
                }
                return Err(error);
            }
        };

        debug!("Deserializing message");
        let parsed_msg = match (self.deserializer)(payload) {
            Some(msg) => {
                debug!("Message successfully deserialized");
                msg
            },
            None => {
                warn!("Deserialization failed");
                let error = Error::DeserializationFailed;
                if let Some(producer) = &self.dead_letter_producer {
                    warn!("Sending message to DLQ due to deserialization failure");
                    if let Err(e) = self.send_to_dlq(producer, msg, error.clone()).await {
                        error!("Failed to send message to DLQ: {}", e);
                    }
                }
                return Err(error);
            }
        };

        let mut attempt = 0;
        let mut last_error = None;
        let mut backoff = self.retry_config.initial_backoff_ms;

        while attempt < self.retry_config.max_attempts {
            debug!("Processing attempt {}/{}", attempt + 1, self.retry_config.max_attempts);
            match (self.handler)(parsed_msg.clone()) {
                Ok(_) => {
                    debug!("Message successfully processed on attempt {}", attempt + 1);
                    self.metrics.messages_processed.fetch_add(1, Ordering::Relaxed);
                    self.metrics.total_processing_time_ms.fetch_add(
                        start.elapsed().as_millis() as u64,
                        Ordering::Relaxed,
                    );
                    return Ok(());
                },
                Err(e) => {
                    attempt += 1;
                    debug!("Attempt {} failed with error: {:?}", attempt, e);
                    self.metrics.retry_attempts.fetch_add(1, Ordering::Relaxed);
                    last_error = Some(e);

                    if attempt < self.retry_config.max_attempts {
                        debug!("Retrying in {}ms", backoff);
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        backoff = (backoff as f64 * self.retry_config.backoff_multiplier) as u64;
                        backoff = backoff.min(self.retry_config.max_backoff_ms);
                    }
                }
            }
        }

        debug!("All processing attempts failed");
        self.metrics.messages_failed.fetch_add(1, Ordering::Relaxed);

        let error = last_error.clone().unwrap();
        debug!("Checking for DLQ configuration");
        if let Some(producer) = &self.dead_letter_producer {
            debug!("DLQ producer found, attempting to send to DLQ");
            if let Err(e) = self.send_to_dlq(producer, msg, error.clone()).await {
                error!("Failed to send message to DLQ: {}", e);
            } else {
                debug!("Successfully sent message to DLQ");
            }
        } else {
            warn!("No DLQ producer configured");
        }

        Err(last_error.unwrap())
    }
}

impl<T> Default for KafkaListener<T>
where
    T: DeserializeOwned + Send + Clone + 'static,
{
    fn default() -> Self {
        KafkaListener {
            topic: String::new(),
            listener_id: String::new(),
            yaml_path: String::new(),
            offset_tracker: Arc::new(Mutex::new(HashMap::new())),
            handler: Box::new(|_: T| Ok(())),
            deserializer: Box::new(|_| None),
            is_running: Arc::new(AtomicBool::new(false)),
            retry_config: RetryConfig::default(),
            metrics: Arc::new(Metrics::new()),
            dead_letter_producer: None,
            dlq_topic: None,
        }
    }
}

impl<T> KafkaListener<T>
where
    T: DeserializeOwned + Send + Clone + 'static,
{
    pub fn with_dead_letter_queue(mut self, dlq_topic: &str) -> Self {
        let config = load_config(&self.yaml_path, &self.listener_id)
            .expect("Failed to load config for DLQ producer");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", config.get("bootstrap.servers").unwrap_or("localhost:9092"))
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create DLQ producer");

        self.dlq_topic = Some(dlq_topic.to_string());
        self.dead_letter_producer = Some(producer);
        self
    }

    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }
}

// -----------------------------------------------------------------------------
// Helper functions
// -----------------------------------------------------------------------------

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigquit = signal(SignalKind::quit()).unwrap();

        tokio::select! {
            _ = sigint.recv() => info!("Received SIGINT (Ctrl+C), shutting down..."),
            _ = sigterm.recv() => info!("Received SIGTERM, shutting down..."),
            _ = sigquit.recv() => info!("Received SIGQUIT (Ctrl+\\), shutting down..."),
        }
    }

    #[cfg(windows)]
    {
        let _ = signal::ctrl_c().await;
        info!("Received Ctrl+C or termination signal, shutting down...");
    }
}
