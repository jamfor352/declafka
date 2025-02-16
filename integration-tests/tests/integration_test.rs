use global_kafka::{create_producer, get_kafka_container};
use my_kafka_macros::kafka_listener;
use rdkafka::producer::FutureRecord;
use serde::{Serialize, Deserialize};
use my_kafka_lib::{KafkaConfig, OffsetReset, Error};
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tokio::time::sleep;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use env_logger;
use lazy_static::lazy_static;
use log::info;

mod global_kafka;

// Static counters and shared state for our handlers.
lazy_static! {
    static ref PROCESSED_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref FAILED_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref RETRY_ATTEMPTS: Arc<parking_lot::Mutex<Vec<std::time::Instant>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
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

/// Initialize logging once.
fn log_setup() {
    let mut log_setup = LOG_SET_UP.lock().unwrap();
    if !*log_setup {
        env_logger::builder()
            .format_timestamp_millis()
            .filter_level(log::LevelFilter::Info)
            .init();
        *log_setup = true;
    }
}


// Define handlers outside of tests.
#[kafka_listener(
    topic = "test-topic",
    config = "test_config",
    deserializer = "json_deserializer"
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
    deserializer = "json_deserializer",
    dlq_topic = "test-dlq-topic",
    retry_max_attempts = 3
)]
fn failing_handler(_msg: TestMessage) -> Result<(), Error> {
    FAILED_COUNT.fetch_add(1, Ordering::SeqCst);
    Err(Error::ProcessingFailed("Simulated failure".into()))
}

#[kafka_listener(
    topic = "test-topic-retry",
    config = "test_config",
    deserializer = "json_deserializer",
    retry_max_attempts = 3,
    retry_initial_backoff = 100,
    retry_multiplier = 2.0
)]
fn retry_handler(_msg: TestMessage) -> Result<(), Error> {
    let now = std::time::Instant::now();
    RETRY_ATTEMPTS.lock().push(now);
    Err(Error::ProcessingFailed("Simulated failure".into()))
}

fn get_state_for_id(id: u32) -> Option<TestMessage> {
    let state = GLOBAL_STATE.lock().unwrap();
    state.get(&id).cloned()
}

#[tokio::test]
async fn test_kafka_functionality() {
    log_setup();

    info!("Starting Kafka integration tests");
    info!("Setting up Kafka container...");
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
        
        info!("Sending message {}: {:?}", i, test_msg);
        producer.send(
            FutureRecord::to("test-topic")
                .payload(&serde_json::to_string(&test_msg).unwrap())
                .key(&i.to_string()),
            Duration::from_secs(5),
        )
        .await
        .expect("Failed to send message");
    }

    sleep(Duration::from_secs(5)).await;
    assert_eq!(
        PROCESSED_COUNT.load(Ordering::SeqCst),
        5,
        "Basic message test failed"
    );

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
    info!("Starting Kafka integration tests");
    info!("Setting up Kafka container...");
    let container_info = get_kafka_container().await;
    let producer = create_producer(&container_info.bootstrap_servers);

    FAILED_COUNT.store(0, Ordering::SeqCst);
    let listener = failing_handler_listener();
    listener.start();

    let test_msg = TestMessage {
        id: 1,
        content: "will fail".into(),
    };
    
    producer.send(
        FutureRecord::to("test-topic-dlq")
            .payload(&serde_json::to_string(&test_msg).unwrap())
            .key("1"),
        Duration::from_secs(5),
    )
    .await
    .expect("Failed to send message");

    sleep(Duration::from_secs(10)).await;
    assert_eq!(
        FAILED_COUNT.load(Ordering::SeqCst),
        3,
        "DLQ test failed"
    );
    info!("DLQ test completed!! 🚀");
}
