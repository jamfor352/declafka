use global_kafka::{create_producer, get_kafka_container};
use logging_setup::log_setup;
use declafka_macro::kafka_listener;
use rdkafka::producer::FutureRecord;
use serde::{Serialize, Deserialize};
use declafka_lib::Error;
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
    static ref DLQ_ACTIVATION_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref FAILED_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref GLOBAL_STATE: Arc<Mutex<HashMap<u32, TestMessage>>> =
        Arc::new(Mutex::new(HashMap::new()));
    static ref LOG_SET_UP: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

// Test message type.
#[derive(Clone, Serialize, Deserialize)]
struct TestMessage {
    id: u32,
    content: String,
}

impl TestMessage {
    fn to_string(&self) -> String {
        format!("id: {}, content: {}", self.id, self.content)
    }
}

// Helper function to deserialize JSON messages.
fn json_deserializer(payload: &[u8]) -> Option<TestMessage> {
    serde_json::from_slice(payload).ok()
}

fn string_deserializer(payload: &[u8]) -> Option<String> {
    String::from_utf8_lossy(payload).to_string().into()
}

// Define handlers outside of tests.
#[kafka_listener(
    topic = "test-topic",
    listener_id = "test-listener",
    yaml_path = "test-kafka.yaml",
    deserializer = "json_deserializer",
    dlq_topic = "test-topic-dlq"
)]
fn test_handler(msg: TestMessage) -> Result<(), Error> {
    PROCESSED_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut state = GLOBAL_STATE.lock().unwrap();
    state.insert(msg.id, msg.clone());
    info!("Updated state with message: {:?}", msg.to_string());
    Ok(())
}

#[kafka_listener(
    topic = "test-topic-dlq",
    listener_id = "dlq-listener",
    yaml_path = "test-kafka.yaml",
    deserializer = "string_deserializer"
)]
fn test_topic_dlq_handler(_msg: String) -> Result<(), Error> {
    info!("Received message from DLQ: {:?}", _msg);
    DLQ_ACTIVATION_COUNT.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[kafka_listener(
    topic = "testing-errors",
    listener_id = "erroring-listener",
    yaml_path = "test-kafka.yaml",
    deserializer = "string_deserializer",
    retry_max_attempts = 5,
    retry_initial_backoff = 100,
    retry_max_backoff = 10000,
    retry_multiplier = 2.0
)]
fn erroring_handler(_msg: String) -> Result<(), Error> {
    info!("Received erroring msg: {:?}", _msg);
    FAILED_COUNT.fetch_add(1, Ordering::SeqCst);
    Err(Error::ProcessingFailed("test".to_string()))
}

fn get_state_for_id(id: u32) -> Option<TestMessage> {
    let state = GLOBAL_STATE.lock().unwrap();
    state.get(&id).cloned()
}

#[tokio::test]
async fn test_kafka_functionality() {
    log_setup();

    let _container_info = get_kafka_container().await;
    let producer = create_producer();

    PROCESSED_COUNT.store(0, Ordering::SeqCst);

    let listener = test_handler_listener().expect("Failed to create test listener");
    let shutdown_tx = listener.start();

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

    // Send shutdown signal and wait a bit to ensure it's processed
    shutdown_tx.send(true).unwrap();
    sleep(Duration::from_secs(2)).await;  // Add small delay to ensure shutdown completes
}

#[tokio::test]
async fn test_failing_listener() {
    log_setup();

    let _container_info = get_kafka_container().await;
    let producer = create_producer();

    DLQ_ACTIVATION_COUNT.store(0, Ordering::SeqCst);
    let listener1 = test_handler_listener().expect("Failed to create test listener");
    let shutdown_tx = listener1.start();
    let listener2 = test_topic_dlq_handler_listener().expect("Failed to create DLQ listener");
    let shutdown_tx2 = listener2.start();

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
        DLQ_ACTIVATION_COUNT.load(Ordering::SeqCst),
        1,
        "DLQ test failed"
    );
    info!("DLQ test completed!! 🚀");

    // Send shutdown signals and wait a bit to ensure it's processed
    shutdown_tx.send(true).unwrap();
    shutdown_tx2.send(true).unwrap();
    sleep(Duration::from_secs(2)).await;  // Add small delay to ensure shutdown completes
}

#[tokio::test]
async fn test_erroring_listener() {
    log_setup();

    let _container_info = get_kafka_container().await;
    let producer = create_producer();

    FAILED_COUNT.store(0, Ordering::SeqCst);
    let erroring_listener = erroring_handler_listener().expect("Failed to create DLQ listener");
    let shutdown_tx = erroring_listener.start();

    let actual_json_msg = "{\"id\":\"will fail as it is not a number\",\"content\":\"test message 0\"}";
    info!("Sending broken message: {:?}", actual_json_msg);
    producer.send(
        FutureRecord::to("testing-errors")
            .payload(actual_json_msg)
            .key("1"),
        Duration::from_secs(5),
    )
    .await
    .expect("Failed to send message");  

    sleep(Duration::from_secs(10)).await;
    assert_eq!(
        FAILED_COUNT.load(Ordering::SeqCst),
        5,
        "Erroring listener test failed"
    );
    info!("Erroring listener test completed!! 🚀");
    
    // Send shutdown signal and wait a bit to ensure it's processed
    shutdown_tx.send(true).unwrap();
    sleep(Duration::from_secs(2)).await;  // Add small delay to ensure shutdown completes
}
