use testcontainers::{
    core::{ExecCommand, IntoContainerPort, WaitFor}, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt
};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Serialize, Deserialize};
use my_kafka_lib::{KafkaConfig, OffsetReset, Error};
use my_kafka_macros::kafka_listener;
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tokio::time::sleep;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use env_logger;
use lazy_static::lazy_static;
use log::{info, debug};

// Static counters for our handlers
lazy_static! {
    static ref PROCESSED_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref FAILED_COUNT: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));
    static ref RETRY_ATTEMPTS: Arc<parking_lot::Mutex<Vec<std::time::Instant>>> = 
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    static ref GLOBAL_STATE: Arc<Mutex<HashMap<u32, TestMessage>>> =
        Arc::new(Mutex::new(HashMap::new()));
    static ref KAFKA_CONTAINER_INFO: Arc<Mutex<Option<KafkaContainerInfo>>> =
        Arc::new(Mutex::new(None));
}

struct KafkaContainerInfo {
    pub container: ContainerAsync<GenericImage>,
    pub bootstrap_servers: String,
}

// Test message type
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage {
    id: u32,
    content: String,
}

// Helper function to create test configuration
fn test_config() -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: "localhost:19092".to_string(),
        consumer_group: "test-group".to_string(),
        auto_offset_reset: OffsetReset::EARLIEST,
    }
}

// Helper function to deserialise JSON messages
fn json_deserializer(payload: &[u8]) -> Option<TestMessage> {
    serde_json::from_slice(payload).ok()
}

async fn create_kafka_container() {
    if KAFKA_CONTAINER_INFO.lock().unwrap().is_some() {
        info!("Kafka container already exists, skipping creation");
        return;
    }

    let kafka_container_info = create_kafka_container_delegate().await;
    *KAFKA_CONTAINER_INFO.lock().unwrap() = Some(kafka_container_info);
}

async fn create_kafka_container_delegate() -> KafkaContainerInfo {
    info!("Starting Kafka container setup...");

    let mapped_port = 19092;
    let controller_port = 19093;

    let container = GenericImage::new("confluentinc/cp-kafka", "latest")
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", format!("1@127.0.0.1:{}", controller_port))
        .with_env_var("KAFKA_LISTENERS", format!("PLAINTEXT://0.0.0.0:{},CONTROLLER://0.0.0.0:{}", mapped_port, controller_port))
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!("PLAINTEXT://127.0.0.1:{}", mapped_port))
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_mapped_port(mapped_port, mapped_port.tcp())
        .with_mapped_port(controller_port, controller_port.tcp())
        .start()
        .await
        .expect("Failed to start Kafka");

    let host_port = container.get_host_port_ipv4(mapped_port).await.expect("Failed to get port");
    let bootstrap_servers = format!("127.0.0.1:{}", host_port);
    
    info!("Waiting for Kafka to be fully ready on port {}...", host_port);
    sleep(Duration::from_secs(10)).await;

    // Create topics
    let topics = ["test-topic", "test-topic-dlq", "test-dlq-topic", "test-topic-retry"];
    for topic in topics {
        info!("Creating topic: {}", topic);
        container.exec(ExecCommand::new([
            "kafka-topics",
            "--create", 
            "--topic", topic,
            "--partitions", "1",
            "--replication-factor", "1",
            "--bootstrap-server", &format!("localhost:{}", mapped_port)
        ])).await.expect("Failed to create topic");
    }
    
    info!("Kafka setup complete");
    KafkaContainerInfo{container, bootstrap_servers}
}

// Helper to create producer
fn create_producer(bootstrap_servers: &str) -> FutureProducer {
    info!("Creating producer with bootstrap servers: {}", bootstrap_servers);
    rdkafka::ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("debug", "all")  // Add debug logging
        .create()
        .expect("Failed to create producer")
}

// Define handlers outside of tests
#[kafka_listener(
    topic = "test-topic",
    config = "test_config",
    deserializer = "json_deserializer"
)]
fn test_handler(msg: TestMessage) -> Result<(), Error> {
    PROCESSED_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut state = GLOBAL_STATE.lock().unwrap();
    state.insert(msg.id, msg.clone());
    println!("Updated state with message: {:?}", msg);
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

    env_logger::builder()
        .format_timestamp_millis()
        .filter_level(log::LevelFilter::Info)
        .init();
    
    info!("Starting Kafka integration tests");
    // Setup Kafka once for all tests
    info!("Setting up Kafka container...");
    create_kafka_container().await;
    
    let producer = create_producer("localhost:19092");

    PROCESSED_COUNT.store(0, Ordering::SeqCst);
    
    let listener = test_handler_listener();
    listener.start();

    for i in 0..5 {
        let test_msg = TestMessage {
            id: i,
            content: format!("test message {}", i),
        };
        
        debug!("Sending message {}: {:?}", i, test_msg);
        producer.send(
            FutureRecord::to("test-topic")
                .payload(&serde_json::to_string(&test_msg).unwrap())
                .key(&i.to_string()),
            Duration::from_secs(5),
        ).await.expect("Failed to send message");
    }

    sleep(Duration::from_secs(5)).await;
    assert_eq!(PROCESSED_COUNT.load(Ordering::SeqCst), 5, "Basic message test failed");
    for i in 0..5 {

        let current_state = get_state_for_id(i);
        assert!(current_state.is_some(), "Message with id {} should be in state", i);
        assert_eq!(current_state.unwrap().content, format!("test message {}", i), "Message content should match");
    }
    info!("Basic message test completed!! 🚀");
} 

#[tokio::test]
async fn test_failing_listener() {
    
    info!("Starting Kafka integration tests");
    // Setup Kafka once for all tests
    info!("Setting up Kafka container...");
    create_kafka_container().await;
    
    let producer = create_producer("localhost:19092");

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
    ).await.expect("Failed to send message");

    sleep(Duration::from_secs(10)).await;
    assert_eq!(FAILED_COUNT.load(Ordering::SeqCst), 3, "DLQ test failed");
    info!("DLQ test completed!! 🚀");
}
