//! Robust Kafka consumer implementation designed for reliability and fault tolerance.
//!
//! This module supports dependency injection for the consumer backend. It defines a generic
//! `KafkaListener` that works with either a real (rdkafka-based) backend or an in-memory mock
//! implementation. It also abstracts dead-letter queue (DLQ) support via the `DLQProducer` trait.

/// Declare the kafka_config module (provide your implementation separately)
pub mod kafka_config;

use async_trait::async_trait;
use chrono;
use log::{debug, info, warn};
use rdkafka::client::ClientContext;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Offset};
use rdkafka::Message; // For methods like topic(), offset(), etc.
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

// -----------------------------------------------------------------------------
// Kafka Consumer Abstraction
// -----------------------------------------------------------------------------

/// A simplified error type for consumer operations.
#[derive(Debug)]
pub enum KafkaConsumerError {
    RecvError(String),
    CommitError(String),
}

/// A simplified message type that our listener will use.
#[derive(Clone, Debug)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub payload: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
}

/// Trait for Kafka consumer backends.
#[async_trait]
pub trait KafkaConsumer: Send + Sync {
    async fn subscribe(&self, topics: &[String]) -> Result<(), KafkaConsumerError>;
    async fn recv(&self) -> Result<KafkaMessage, KafkaConsumerError>;
    async fn commit(&self, topic: &str, partition: i32, offset: i64) -> Result<(), KafkaConsumerError>;
}

// -----------------------------------------------------------------------------
// DLQ Producer Abstraction
// -----------------------------------------------------------------------------

/// An error type for DLQ operations.
#[derive(Debug)]
pub enum DLQError {
    ProducerError(String),
}

/// Trait to abstract DLQ producers.
#[async_trait]
pub trait DLQProducer: Send + Sync {
    async fn send(&self, payload: &str, dlq_topic: &str) -> Result<(i32, i64), DLQError>;
}

/// Trait for consumer backends to supply a default DLQ producer.
pub trait DLQProducerFactory {
    fn default_dlq_producer(yaml_path: &str, listener_id: &str) -> Box<dyn DLQProducer>;
}

/// Real DLQ producer implementation (only used in the real consumer).
pub struct RDKafkaDLQProducer {
    pub inner: FutureProducer,
}

#[async_trait]
impl DLQProducer for RDKafkaDLQProducer {
    async fn send(&self, payload: &str, dlq_topic: &str) -> Result<(i32, i64), DLQError> {
        let record = FutureRecord::to(dlq_topic)
            .payload(payload)
            .key("");
        self.inner.send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| DLQError::ProducerError(e.to_string()))
    }
}

/// Mock DLQ producer for testing.
pub struct MockDLQProducer;

#[async_trait]
impl DLQProducer for MockDLQProducer {
    async fn send(&self, payload: &str, dlq_topic: &str) -> Result<(i32, i64), DLQError> {
        info!("(Mock) DLQ send to {}: {}", dlq_topic, payload);
        Ok((0, 0))
    }
}

// -----------------------------------------------------------------------------
// RDKafka-based Consumer Implementation (Real Backend)
// -----------------------------------------------------------------------------

#[derive(Clone)]
struct CustomContext {
    offset_tracker: Arc<Mutex<HashMap<(String, i32), i64>>>,
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<CustomContext>,
        rebalance: &rdkafka::consumer::Rebalance,
    ) {
        info!("Pre-rebalance: {:?}", rebalance);
    }

    fn post_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<CustomContext>,
        rebalance: &rdkafka::consumer::Rebalance,
    ) {
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

/// Real consumer backend using rdkafka.
pub struct RDKafkaConsumer {
    consumer: StreamConsumer<CustomContext>,
}

impl RDKafkaConsumer {
    pub fn new(yaml_path: &str, listener_id: &str, topic: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config = kafka_config::load_config(yaml_path, listener_id)?;
        let context = CustomContext {
            offset_tracker: Arc::new(Mutex::new(HashMap::new())),
        };
        let consumer: StreamConsumer<CustomContext> = config.create_with_context(context)?;
        consumer.subscribe(&[topic])
            .map_err(|e| format!("Failed to subscribe: {}", e))?;
        Ok(Self { consumer })
    }
}

#[async_trait]
impl KafkaConsumer for RDKafkaConsumer {
    async fn subscribe(&self, topics: &[String]) -> Result<(), KafkaConsumerError> {
        let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        self.consumer.subscribe(&topic_refs)
            .map_err(|e| KafkaConsumerError::RecvError(format!("Subscribe error: {}", e)))?;
        Ok(())
    }

    async fn recv(&self) -> Result<KafkaMessage, KafkaConsumerError> {
        match self.consumer.recv().await {
            Ok(m) => Ok(KafkaMessage {
                topic: m.topic().to_string(),
                partition: m.partition(),
                offset: m.offset(),
                payload: m.payload().map(|p| p.to_vec()),
                key: m.key().map(|k| k.to_vec()),
            }),
            Err(e) => Err(KafkaConsumerError::RecvError(e.to_string())),
        }
    }

    async fn commit(&self, topic: &str, partition: i32, offset: i64) -> Result<(), KafkaConsumerError> {
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(|e| KafkaConsumerError::CommitError(e.to_string()))?;
        self.consumer.commit(&tpl, CommitMode::Sync)
            .map_err(|e| KafkaConsumerError::CommitError(e.to_string()))
    }
}

impl DLQProducerFactory for RDKafkaConsumer {
    fn default_dlq_producer(yaml_path: &str, listener_id: &str) -> Box<dyn DLQProducer> {
        let config = kafka_config::load_config(yaml_path, listener_id)
            .expect("Failed to load config for DLQ producer");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", config.get("bootstrap.servers").unwrap_or("localhost:9092"))
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create DLQ producer");
        Box::new(RDKafkaDLQProducer { inner: producer })
    }
}

// -----------------------------------------------------------------------------
// In-memory Consumer Implementation (Mock Backend)
// -----------------------------------------------------------------------------

use tokio::sync::mpsc::{self, Sender, Receiver};

/// In-memory Kafka consumer for testing.
pub struct MockKafkaConsumer {
    topics: Arc<Mutex<Vec<String>>>,
    sender: Sender<KafkaMessage>,
    receiver: Arc<tokio::sync::Mutex<Receiver<KafkaMessage>>>,
}

impl MockKafkaConsumer {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self {
            topics: Arc::new(Mutex::new(vec![])),
            sender,
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
        }
    }

    /// Helper: send a message into the in-memory consumer.
    pub async fn send(&self, msg: KafkaMessage) {
        let _ = self.sender.send(msg).await;
    }
}

#[async_trait]
impl KafkaConsumer for MockKafkaConsumer {
    async fn subscribe(&self, topics: &[String]) -> Result<(), KafkaConsumerError> {
        let mut t = self.topics.lock().unwrap();
        *t = topics.to_vec();
        Ok(())
    }

    async fn recv(&self) -> Result<KafkaMessage, KafkaConsumerError> {
        let mut rx = self.receiver.lock().await;
        rx.recv().await.ok_or(KafkaConsumerError::RecvError("Channel closed".into()))
    }

    async fn commit(&self, _topic: &str, _partition: i32, _offset: i64) -> Result<(), KafkaConsumerError> {
        Ok(())
    }
}

impl DLQProducerFactory for MockKafkaConsumer {
    fn default_dlq_producer(_yaml_path: &str, _listener_id: &str) -> Box<dyn DLQProducer> {
        Box::new(MockDLQProducer)
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
// KafkaListener (generic over the consumer backend)
// -----------------------------------------------------------------------------

pub struct KafkaListener<T, C>
where
    T: DeserializeOwned + Send + Clone + 'static,
    C: KafkaConsumer + 'static,
{
    topic: String,
    listener_id: String,
    yaml_path: String,
    consumer: C,
    offset_tracker: Arc<Mutex<HashMap<(String, i32), i64>>>,
    handler: Box<dyn Fn(T) -> Result<(), Error> + Send + Sync>,
    deserializer: Box<dyn Fn(&[u8]) -> Option<T> + Send + Sync>,
    is_running: Arc<AtomicBool>,
    retry_config: RetryConfig,
    metrics: Arc<Metrics>,
    // DLQ support is now abstracted to a boxed trait.
    dead_letter_producer: Option<Box<dyn DLQProducer>>,
    dlq_topic: Option<String>,
}

impl<T, C> KafkaListener<T, C>
where
    T: DeserializeOwned + Send + Clone + 'static,
    C: KafkaConsumer + Send + Sync + 'static,
{
    pub fn new<F, D>(
        topic: &str,
        listener_id: &str,
        yaml_path: &str,
        deserializer: D,
        handler: F,
        consumer: C,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: Fn(T) -> Result<(), Error> + Send + Sync + 'static,
        D: Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    {
        info!("Kafka listener initialized with topic: {}, listener_id: {}, yaml_path: {}", topic, listener_id, yaml_path);
        Ok(Self {
            topic: topic.to_string(),
            listener_id: listener_id.to_string(),
            yaml_path: yaml_path.to_string(),
            consumer,
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
        let listener = Arc::new(self);
        let _ = listener.clone(); // Unused clone.
        tokio::spawn(async move {
            if let Err(e) = listener.consumer.subscribe(&vec![listener.topic.clone()]).await {
                warn!("Failed to subscribe: {:?}", e);
            }
            listener.is_running.store(true, Ordering::SeqCst);
            info!("Kafka listener started for topic: {}", listener.topic);
            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let _ = listener.clone();
            tokio::spawn(async move {
                wait_for_shutdown().await;
                info!("Shutdown signal received, notifying Kafka consumers...");
                let _ = shutdown_tx.send(true);
            });
            'main: while listener.is_running.load(Ordering::SeqCst) {
                tokio::select! {
                    result = listener.consumer.recv() => {
                        match result {
                            Err(e) => warn!("Kafka error: {:?}", e),
                            Ok(msg) => {
                                if msg.topic != listener.topic {
                                    debug!("Received message for topic {} (expected {}), skipping", msg.topic, listener.topic);
                                    continue;
                                }
                                {
                                    let mut tracker = listener.offset_tracker.lock().unwrap();
                                    tracker.insert((msg.topic.clone(), msg.partition), msg.offset + 1);
                                }
                                if let Err(e) = listener.process_message(msg).await {
                                    if listener.dead_letter_producer.is_some() {
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
                        let tracker_snapshot: Vec<(String, i32, i64)> = {
                            let tracker = listener.offset_tracker.lock().unwrap();
                            tracker.iter().map(|((topic, partition), &offset)| (topic.clone(), *partition, offset)).collect()
                        };
                        for (topic, partition, offset) in tracker_snapshot {
                            if let Err(e) = listener.consumer.commit(&topic, partition, offset).await {
                                warn!("Failed to commit offset during shutdown: {:?}", e);
                            }
                        }
                        listener.is_running.store(false, Ordering::SeqCst);
                        break 'main;
                    }
                }
            }
            info!("Kafka listener for topic '{}' has stopped.", listener.topic);
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
        if total_messages == 0 { 0.0 } else { total_time as f64 / total_messages as f64 }
    }

    async fn send_to_dlq(&self, msg: &KafkaMessage, error: Error) -> Result<(), DLQError> {
        let dlq_topic = self.dlq_topic.as_ref().expect("DLQ topic must be set");
        let dlq_payload = json!({
            "original_topic": msg.topic,
            "original_partition": msg.partition,
            "original_offset": msg.offset,
            "error": format!("{:?}", error),
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "payload": msg.payload.as_ref().map(|p| String::from_utf8_lossy(p)).unwrap_or_default(),
        });
        let payload_str = serde_json::to_string(&dlq_payload).unwrap();
        warn!("DLQ payload prepared: {}", payload_str);
        if let Some(producer) = &self.dead_letter_producer {
            producer.send(&payload_str, dlq_topic).await.map(|(partition, offset)| {
                self.metrics.dead_letter_messages.fetch_add(1, Ordering::Relaxed);
                warn!("Successfully sent to DLQ topic {} (partition: {}, offset: {})", dlq_topic, partition, offset);
            })
        } else {
            Err(DLQError::ProducerError("No DLQ producer configured".into()))
        }
    }

    async fn process_message(&self, msg: KafkaMessage) -> Result<(), Error> {
        debug!("Processing message from topic: {}, partition: {}, offset: {}", msg.topic, msg.partition, msg.offset);
        if msg.topic != self.topic {
            debug!("Skipping message from topic {} (expected {})", msg.topic, self.topic);
            return Ok(());
        }
        let start = Instant::now();
        let payload = match msg.payload {
            Some(ref p) => p,
            None => {
                warn!("No payload found in message");
                let error = Error::NoPayload;
                if self.dead_letter_producer.is_some() {
                    let _ = self.send_to_dlq(&msg, error.clone()).await;
                }
                return Err(error);
            }
        };
        debug!("Deserializing message");
        let parsed_msg = match (self.deserializer)(payload) {
            Some(m) => { debug!("Message successfully deserialized"); m },
            None => {
                warn!("Deserialization failed");
                let error = Error::DeserializationFailed;
                if self.dead_letter_producer.is_some() {
                    let _ = self.send_to_dlq(&msg, error.clone()).await;
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
                    debug!("Message processed successfully on attempt {}", attempt + 1);
                    self.metrics.messages_processed.fetch_add(1, Ordering::Relaxed);
                    self.metrics.total_processing_time_ms.fetch_add(start.elapsed().as_millis() as u64, Ordering::Relaxed);
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
        let error = last_error.unwrap();
        debug!("Checking for DLQ configuration");
        if self.dead_letter_producer.is_some() {
            let _ = self.send_to_dlq(&msg, error.clone()).await;
        } else {
            warn!("No DLQ producer configured");
        }
        Err(error)
    }
}

impl<T, C> KafkaListener<T, C>
where
    T: DeserializeOwned + Send + Clone + 'static,
    C: KafkaConsumer + DLQProducerFactory + Send + Sync + 'static,
{
    /// Builder method that configures the DLQ by using the consumer’s default DLQ producer.
    pub fn with_dead_letter_queue(mut self, dlq_topic: &str) -> Self {
        self.dlq_topic = Some(dlq_topic.to_string());
        self.dead_letter_producer = Some(C::default_dlq_producer(&self.yaml_path, &self.listener_id));
        self
    }

    /// Builder method to configure retry policy.
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }
}      

// -----------------------------------------------------------------------------
// Helper for graceful shutdown
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
        let _ = tokio::signal::ctrl_c().await;
        info!("Received Ctrl+C or termination signal, shutting down...");
    }
}

// -----------------------------------------------------------------------------
// Unit tests using the in-memory (mock) implementation
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_process_message_success() {
        let mock_consumer = MockKafkaConsumer::new();
        let processed_count = Arc::new(AtomicUsize::new(0));
        let count_clone = processed_count.clone();
        let handler = move |msg: String| {
            count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };
        let deserializer = |payload: &[u8]| std::str::from_utf8(payload).ok().map(|s| s.to_string());
        let listener: KafkaListener<String, _> = KafkaListener::new(
            "test-topic",
            "test-listener",
            "dummy.yaml",
            deserializer,
            handler,
            mock_consumer,
        ).unwrap();
        let test_msg = KafkaMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            payload: Some(b"test message".to_vec()),
            key: None,
        };
        let result = listener.process_message(test_msg).await;
        assert!(result.is_ok());
        assert_eq!(processed_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_process_message_failure_and_retry() {
        let mock_consumer = MockKafkaConsumer::new();
        let attempt_count = Arc::new(AtomicUsize::new(1));
        let count_clone = attempt_count.clone();
        let handler = move |msg: String| {
            let attempts = count_clone.fetch_add(1, Ordering::SeqCst);
            println!("Processing message: {} for the {} time", msg, attempts);
            if attempts < 3 { Err(Error::ProcessingFailed("fail".into())) } else { Ok(()) }
        };
        let deserializer = |payload: &[u8]| std::str::from_utf8(payload).ok().map(|s| s.to_string());
        let listener: KafkaListener<String, _> = KafkaListener::new(
            "test-topic",
            "test-listener",
            "dummy.yaml",
            deserializer,
            handler,
            mock_consumer,
        ).unwrap();
        let test_msg = KafkaMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            payload: Some(b"retry test".to_vec()),
            key: None,
        };
        let result = listener.process_message(test_msg).await;
        assert!(result.is_ok());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 4);
    }
}

