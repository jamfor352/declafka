use std::{collections::HashMap, sync::{Arc, Mutex}, time::Duration};

use async_trait::async_trait;
use log::{info, warn};
use rdkafka::{consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer}, producer::{FutureProducer, FutureRecord}, ClientConfig, ClientContext, Message, Offset};

use crate::{kafka_config, DLQError, DLQProducer, DLQProducerFactory, KafkaConsumer, KafkaConsumerError, KafkaMessage};

/// Real DLQ producer implementation (only used in the real consumer).
pub struct RDKafkaDLQProducer {
    pub inner: FutureProducer,
}



#[async_trait]
impl DLQProducer for RDKafkaDLQProducer {
    async fn send(&self, key: &str, payload: &str, dlq_topic: &str) -> Result<(i32, i64), DLQError> {
        let record = FutureRecord::to(dlq_topic)
            .payload(payload)
            .key(key);
        self.inner.send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| DLQError::ProducerError(e.to_string()))
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
                key: m.key().map(|k| String::from_utf8_lossy(k).into_owned()),
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
    fn default_dlq_producer(&self, yaml_path: &str, listener_id: &str) -> Box<dyn DLQProducer> {
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
