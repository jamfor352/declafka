use global_kafka::{create_producer, get_kafka_container};
use logging_setup::log_setup;
use declafka_macro::kafka_listener;
use rdkafka::producer::FutureRecord;
use serde::{Serialize, Deserialize};
use declafka_lib::{KafkaConfig, OffsetReset, Error};
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tokio::time::sleep;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use lazy_static::lazy_static;
use log::info;

mod global_kafka;
mod logging_setup;

// Static counters and shared state for our handlers.
lazy_static! {
    static ref PROCESSED_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref FAILED_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref GLOBAL_STATE: Arc<Mutex<HashMap<u32, TestMessage>>> =
        Arc::new(Mutex::new(HashMap::new()));
    static ref LOG_SET_UP: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

// Test message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage {
    id: u32,
    content: String,
}

// Helper function to create test configuration.
fn test_config() -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: "localhost:19092".to_string(),
        consumer_group: "test-group-original".to_string(),
        auto_offset_reset: OffsetReset::EARLIEST,
    }
}

// Helper function to deserialize JSON messages.
fn json_deserializer(payload: &[u8]) -> Option<TestMessage> {
    serde_json::from_slice(payload).ok()
}

fn string_deserializer(payload: &[u8]) -> Option<String> {
    let string = String::from_utf8_lossy(payload).to_string();
    Some(string)
}


// Define handlers outside of tests.
#[kafka_listener(
    topic = "test-topic",
    config = "test_config",
    deserializer = "json_deserializer",
    dlq_topic = "test-topic-dlq"
)]
fn test_handler(msg: TestMessage) -> Result<(), Error> {
    PROCESSED_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut state = GLOBAL_STATE.lock().unwrap();
    state.insert(msg.id, msg.clone());
    info!("Updated state with message: {:?}", msg);
    Ok(())
}

#[kafka_listener(
    topic = "test-topic-dlq",
    config = "test_config",
    deserializer = "string_deserializer"
)]
fn failing_handler(_msg: String) -> Result<(), Error> {
    info!("Received message from DLQ: {:?}", _msg);
    FAILED_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn get_state_for_id(id: u32) -> Option<TestMessage> {
    let state = GLOBAL_STATE.lock().unwrap();
    state.get(&id).cloned()
}

#[tokio::test]
async fn test_kafka_functionality() {
    log_setup();

    let container_info = get_kafka_container().await;
    let producer = create_producer(&container_info.bootstrap_servers);

    PROCESSED_COUNT.store(0, Ordering::SeqCst);
    
    let listener = test_handler_listener();
    listener.start();

    for i in 0..5 {
        let test_msg = TestMessage {
            id: i,
            content: format!("test message {}", i),
        };

        let actual_json_msg = serde_json::to_string(&test_msg).unwrap();
        info!("Sending message {}: {:?}", i, actual_json_msg);
        producer.send(
            FutureRecord::to("test-topic")
                .payload(&actual_json_msg)
                .key(&i.to_string()),
            Duration::from_secs(5),
        )
        .await
        .expect("Failed to send message");
    }

    sleep(Duration::from_secs(10)).await;

    for i in 0..5 {
        let current_state = get_state_for_id(i);
        assert!(current_state.is_some(), "Message with id {} should be in state", i);
        assert_eq!(
            current_state.unwrap().content,
            format!("test message {}", i),
            "Message content should match"
        );
    }
    info!("Basic message test completed!! 🚀");
} 

#[tokio::test]
async fn test_failing_listener() {
    log_setup();
    
    let container_info = get_kafka_container().await;
    let producer = create_producer(&container_info.bootstrap_servers);

    FAILED_COUNT.store(0, Ordering::SeqCst);
    let listener1 = test_handler_listener();
    listener1.start();
    let listener2 = failing_handler_listener();
    listener2.start();

    let actual_json_msg = "{\"id\":\"will fail as it is not a number\",\"content\":\"test message 0\"}";
    info!("Sending broken message: {:?}", actual_json_msg);
    producer.send(
        FutureRecord::to("test-topic")
            .payload(actual_json_msg)
            .key("1"),
        Duration::from_secs(5),
    )
    .await
    .expect("Failed to send message");

    sleep(Duration::from_secs(10)).await;
    assert_eq!(
        FAILED_COUNT.load(Ordering::SeqCst),
        1,
        "DLQ test failed"
    );
    info!("DLQ test completed!! 🚀");
}
