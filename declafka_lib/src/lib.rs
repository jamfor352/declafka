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
use std::env;
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

// -----------------------------------------------------------------------------
// OffsetReset and KafkaConfig
// -----------------------------------------------------------------------------

/// Specifies where to start consuming messages when there are no committed offsets.
/// This is crucial for determining the initial position in the topic, affecting whether historical data is processed or only new messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    /// Start from the earliest available message, ensuring all data is processed.
    EARLIEST,
    /// Start from the latest message, skipping historical data and processing only new messages.
    LATEST,
}

impl OffsetReset {
    /// Converts the offset reset strategy to a string for Kafka configuration.
    /// Kafka expects specific string values for this setting, so this method ensures compatibility.
    pub fn as_str(&self) -> &'static str {
        match self {
            OffsetReset::EARLIEST => "earliest",
            OffsetReset::LATEST => "latest",
        }
    }
}

/// Configuration for the Kafka consumer, encapsulating essential settings for connecting to Kafka brokers and defining consumer behavior.
#[derive(Clone)]
pub struct KafkaConfig {
    /// Comma-separated list of Kafka broker addresses (e.g., "localhost:9092").
    /// This is necessary to establish connections to the Kafka cluster.
    pub bootstrap_servers: String,
    /// Consumer group ID for coordinating consumption with other consumers.
    /// The group ID is critical for Kafka's consumer group management, enabling load balancing and fault tolerance.
    pub consumer_group: String,
    /// Offset reset strategy when no offsets are committed.
    /// This setting determines whether to start from the beginning or end of the topic if no offsets are found, impacting data completeness.
    pub auto_offset_reset: OffsetReset,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            consumer_group: "default-group".to_string(),
            auto_offset_reset: OffsetReset::EARLIEST,
        }
    }
}

// -----------------------------------------------------------------------------
// CustomContext & Offset Tracker
// -----------------------------------------------------------------------------

/// Custom context for the Kafka consumer to handle rebalances and track offsets.
/// This context is necessary to intercept rebalance events and manage offsets manually, ensuring consistency during partition assignments.
#[derive(Clone)]
struct CustomContext {
    /// Shared offset tracker to store the latest processed offsets for each topic-partition.
    /// Using `Arc<Mutex<>>` ensures thread-safe access and updates, as rebalance callbacks and the main consumer loop may run concurrently.
    offset_tracker: Arc<Mutex<HashMap<(String, i32), i64>>>,
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    /// Logs the rebalance event before it occurs.
    /// This is useful for debugging and monitoring consumer group dynamics.
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<CustomContext>,
        rebalance: &rdkafka::consumer::Rebalance,
    ) {
        info!("Pre-rebalance: {:?}", rebalance);
    }

    /// Commits tracked offsets for revoked partitions during a rebalance.
    /// This ensures that the latest processed offsets are saved before partitions are reassigned, preventing duplicate processing.
    fn post_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<CustomContext>,
        rebalance: &rdkafka::consumer::Rebalance,
    ) {
        info!("Post-rebalance: {:?}", rebalance);
        if let rdkafka::consumer::Rebalance::Revoke(partitions) = rebalance {
            // Lock the offset tracker to safely access stored offsets.
            // The mutex ensures that only one thread modifies the tracker at a time, preventing data corruption.
            let tracker = self.offset_tracker.lock().unwrap();
            let mut tpl = rdkafka::TopicPartitionList::new();

            // Add tracked offsets for each revoked partition to the topic-partition list (TPL).
            for tp in partitions.elements() {
                if let Some(&offset) = tracker.get(&(tp.topic().to_string(), tp.partition())) {
                    tpl.add_partition_offset(tp.topic(), tp.partition(), Offset::Offset(offset))
                        .unwrap_or_else(|e| warn!("Failed to add partition to TPL: {}", e));
                }
            }

            // Synchronously commit offsets if any partitions were added.
            // Synchronous committing ensures that offsets are persisted before the rebalance completes, maintaining consistency across the consumer group.
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

/// Configuration for retrying message processing.
/// Retries are essential for handling transient failures, such as network issues or temporary service unavailability.
pub struct RetryConfig {
    /// Maximum number of retry attempts for failed messages.
    pub max_attempts: u32,
    /// Initial backoff duration in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds.
    pub max_backoff_ms: u64,
    /// Multiplier for exponential backoff between retries.
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

/// Custom error types for Kafka listener operations.
#[derive(Debug, Clone)]
pub enum Error {
    /// Message has no payload.
    NoPayload,
    /// Failed to deserialize the message payload.
    DeserializationFailed,
    /// Failed to process the message.
    ProcessingFailed(String),
    /// Message validation failed.
    ValidationFailed(String),
}

/// Collects runtime metrics to enable real-time monitoring and debugging of the consumer’s behavior.
/// Atomic types are used instead of locks to minimize contention and latency in a multi-threaded context,
/// ensuring that metrics updates don’t become a bottleneck in high-throughput scenarios.
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

/// Health check report for the Kafka listener.
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

/// Main Kafka listener implementation for consuming and processing messages.
pub struct KafkaListener<T>
where
    T: DeserializeOwned + Send + Clone + 'static,
{
    topic: String,
    client_config: KafkaConfig,
    /// Shared offset tracker for committing offsets. Uses `Arc<Mutex<>>` for thread-safe access.
    offset_tracker: Arc<Mutex<HashMap<(String, i32), i64>>>,
    handler: Box<dyn Fn(T) -> Result<(), Error> + Send + Sync>,
    deserializer: Box<dyn Fn(&[u8]) -> Option<T> + Send + Sync>,
    /// Indicates whether the listener is running. Uses `Arc<AtomicBool>` for thread-safe updates.
    is_running: Arc<AtomicBool>,
    retry_config: RetryConfig,
    /// Shared metrics for tracking listener activity. Uses `Arc` for sharing across threads.
    metrics: Arc<Metrics>,
    dead_letter_producer: Option<FutureProducer>,
    dlq_topic: Option<String>,
}

impl<T> KafkaListener<T>
where
    T: DeserializeOwned + Send + Clone + 'static,
{
    /// Creates a new `KafkaListener` instance.
    /// The consumer group is namespaced with the topic to avoid conflicts with other listeners.
    pub fn new<F, D>(
        topic: &str,
        mut client_config: KafkaConfig,
        deserializer: D,
        handler: F,
    ) -> Self
    where
        F: Fn(T) -> Result<(), Error> + Send + Sync + 'static,
        D: Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    {
        client_config.consumer_group = format!("{}-{}", client_config.consumer_group, topic);

        KafkaListener {
            topic: topic.to_string(),
            client_config,
            offset_tracker: Arc::new(Mutex::new(HashMap::new())),
            handler: Box::new(handler),
            deserializer: Box::new(deserializer),
            is_running: Arc::new(AtomicBool::new(false)),
            retry_config: RetryConfig::default(),
            metrics: Arc::new(Metrics::new()),
            dead_letter_producer: None,
            dlq_topic: None,
        }
    }

    /// Starts the Kafka consumer in a separate task and processes messages until shutdown.
    /// This method sets up the consumer, subscribes to the topic, and enters the main processing loop.
    pub fn start(self) {
        // Spawn a task to handle shutdown signals.
        // This allows the consumer to respond to external shutdown requests, ensuring a clean exit.
        tokio::spawn(async move {
            wait_for_shutdown().await;
            info!("Shutdown signal received, notifying Kafka consumer...");
        });

        task::spawn(async move {
            let context = CustomContext {
                offset_tracker: self.offset_tracker.clone(),
            };

            // Create the stream consumer with the custom context for rebalance handling.
            // The custom context is necessary to manage offsets during partition reassignments.
            let consumer: StreamConsumer<CustomContext> = {
                let config = make(self.client_config.clone());
                config.create_with_context(context)
                    .expect("Failed to create Kafka consumer")
            };

            consumer
                .subscribe(&[&self.topic])
                .expect("Failed to subscribe to Kafka topic");

            self.is_running.store(true, Ordering::SeqCst);
            info!("Kafka listener started for topic: {}", self.topic);

            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

            // Spawn a task to wait for shutdown and notify via the watch channel.
            // This decouples the shutdown signal handling from the main consumer loop, allowing for asynchronous notification.
            tokio::spawn(async move {
                wait_for_shutdown().await;
                info!("Shutdown signal received, notifying Kafka consumers...");
                let _ = shutdown_tx.send(true);
            });

            // Main loop: process messages until shutdown signal is received.
            // This loop ensures continuous message consumption while allowing for graceful shutdown.
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

                                // Update the offset tracker with the next offset to commit.
                                // Storing the next offset (+1) ensures that upon restart or rebalance, consumption resumes exactly after the last processed message.
                                {
                                    let mut tracker = self.offset_tracker.lock().unwrap();
                                    tracker.insert((m.topic().to_string(), partition), current_offset + 1);
                                }

                                // Process the message, handling retries and DLQ if enabled.
                                // This step is crucial for ensuring that messages are processed reliably, with mechanisms to handle failures gracefully.
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

                        // Commit tracked offsets during shutdown.
                        // This ensures that all processed messages are accounted for, preventing data loss or duplication on restart.
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

    /// Exports metrics in Prometheus format.
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

    /// Sends a failed message to the dead-letter queue (DLQ).
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
              original_msg.partition(),
              original_msg.offset(),
              error);

        // Create a JSON payload for the DLQ message, including original message details and error.
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
                warn!("Successfully sent message to DLQ topic {} (partition: {}, offset: {})",
                      dlq_topic, partition, offset);
                Ok(())
            },
            Err((e, _)) => {
                error!("Failed to send message to DLQ topic {}: {}", dlq_topic, e);
                Err(e)
            }
        }
    }

    /// Processes an incoming Kafka message with retry and DLQ support.
    /// This method attempts to deserialize and process the message, retrying on failure and routing to DLQ if all retries are exhausted.
    async fn process_message(&self, msg: &BorrowedMessage<'_>) -> Result<(), Error> {
        debug!("Processing message from topic: {}, partition: {}, offset: {}",
               msg.topic(), msg.partition(), msg.offset());

        if msg.topic() != self.topic {
            debug!("Skipping message from topic {} (listener configured for {})", msg.topic(), self.topic);
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
                    debug!("Sending message to DLQ");
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
                    debug!("Sending message to DLQ");
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

        // Retry loop: attempt to process the message multiple times with exponential backoff.
        // This strategy balances reliability and system stability, allowing recovery from transient failures without overwhelming resources.
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
            client_config: KafkaConfig::default(),
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

/// Additional methods which allow for chaining of configuration options.
impl<T> KafkaListener<T>
where
    T: DeserializeOwned + Send + Clone + 'static,
{
    pub fn with_dead_letter_queue(mut self, dlq_topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.client_config.bootstrap_servers)
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

/// Creates a `ClientConfig` from a `KafkaConfig`.
fn make(kafka_config: KafkaConfig) -> ClientConfig {
    ClientConfig::new()
        .set("bootstrap.servers", kafka_config.bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", kafka_config.auto_offset_reset.as_str())
        .set("enable.auto.commit", "false")
        .set("group.id", kafka_config.consumer_group)
        .clone()
}

// -----------------------------------------------------------------------------
// Helper functions
// -----------------------------------------------------------------------------

/// Retrieves default Kafka configuration from environment variables or defaults.
pub fn get_configuration() -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string()),
        auto_offset_reset: OffsetReset::EARLIEST,
        consumer_group: env::var("DEFAULT_CONSUMER_GROUP").unwrap_or_else(|_| "test-service".to_string()),
    }
}

/// Private method that waits for a shutdown signal (Unix signals or Ctrl+C on Windows). 
/// Used for graceful shutdown of the Kafka listener.
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
