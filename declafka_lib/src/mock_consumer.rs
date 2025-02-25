use std::{collections::HashMap, sync::{Arc, Mutex}};

use async_trait::async_trait;
use log::info;
use tokio::sync::mpsc::{self, Receiver};

use crate::{DLQError, DLQProducer, DLQProducerFactory, KafkaConsumer, KafkaConsumerError, KafkaMessage};

/// Mock DLQ producer for testing.
pub struct MockDLQProducer {
    pub messages: Arc<Mutex<HashMap<String, String>>>,
}

#[async_trait]
impl DLQProducer for MockDLQProducer {
    async fn send(&self, key: &str, payload: &str, dlq_topic: &str) -> Result<(i32, i64), DLQError> {
        info!("(Mock) DLQ send to {}: key: {}, payload: {}", dlq_topic, key, payload);
        let mut messages = self.messages.lock().unwrap();
        messages.insert(key.to_string(), payload.to_string());
        info!("(Mock) DLQ messages: {:?}", messages);
        Ok((0, 0))
    }
}


/// In-memory Kafka consumer for testing.
#[derive(Clone)]
pub struct MockKafkaConsumer {
    topics: Arc<Mutex<Vec<String>>>,
    receiver: Arc<tokio::sync::Mutex<Receiver<KafkaMessage>>>,
    dlq_producer: Arc<MockDLQProducer>,
}

impl MockKafkaConsumer {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self {
            topics: Arc::new(Mutex::new(vec![])),
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
            dlq_producer: Arc::new(MockDLQProducer {
                messages: Arc::new(Mutex::new(HashMap::new()))
            }),
        }
    }

    pub fn get_dlq_messages(&self) -> HashMap<String, String> {
        self.dlq_producer.messages.lock().unwrap().clone()
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
    fn default_dlq_producer(&self, _yaml_path: &str, _listener_id: &str) -> Box<dyn DLQProducer> {
        Box::new(MockDLQProducer {
            messages: Arc::clone(&self.dlq_producer.messages)
        })
    }
}
