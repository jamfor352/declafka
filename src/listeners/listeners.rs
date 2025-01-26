use log::{info, warn};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::task;

/// Generic Kafka listener that consumes messages of type `T`
pub struct KafkaListener<T> {
    topic: String,
    brokers: String,
    group_id: String,
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
        brokers: String,
        group_id: String,
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
            brokers,
            group_id,
            shutdown_rx,
            offset_tracker: Arc::new(Mutex::new(HashMap::new())),
            handler: Arc::new(Box::new(handler)),
            deserializer: Arc::new(Box::new(deserializer)), // ✅ Store deserializer
        }
    }

    /// Starts the Kafka consumer loop
    pub fn start(self) {
        task::spawn(async move {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", &self.group_id)
                .set("bootstrap.servers", &self.brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.offset.store", "false")  // ✅ Prevents unexpected in-memory overwrites
                .set("enable.auto.commit", "false")        // ✅ Ensures we always commit manually
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
                                let partition = m.partition();
                                let offset = m.offset();

                                let mut tracker = self.offset_tracker.lock().unwrap();
                                tracker.insert(partition, offset + 1);

                                if let Some(payload) = m.payload() {
                                    if let Some(parsed_msg) = (self.deserializer)(payload) {  // ✅ Call the custom deserializer
                                        (self.handler)(parsed_msg);
                                    } else {
                                        warn!("Deserializer failed, skipping message. Raw payload: '{}'", String::from_utf8_lossy(payload));
                                    }
                                }

                                consumer.store_offset_from_message(&m).expect("Failed to store offset");  // ✅ Ensure Kafka knows what we're committing
                                consumer.commit_message(&m, CommitMode::Async).expect("Failed to commit offset");
                                info!("Kafka listener committed offset {} for partition {}", m.offset(), m.partition());
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
                            info!("Kafka listener committed: partition {}, offset: {} for consumer group: {}", partition, offset, self.group_id);
                        }

                        break;
                    }
                }
            }

            info!("Kafka listener for topic '{}' has stopped.", self.topic);
        });
    }
}
