use rdkafka::producer::FutureProducer;
use testcontainers::{
    core::{
        ExecCommand, 
        IntoContainerPort, 
        WaitFor, 
    }, 
    runners::AsyncRunner, 
    ContainerAsync, 
    GenericImage, 
    ImageExt
};
use std::sync::Weak;
use std::sync::Arc;
use lazy_static::lazy_static;
use log::info;
// Use tokio's async Mutex so we can await while locking.
use tokio::sync::Mutex as AsyncMutex;

// Static counters and shared state for our handlers.
lazy_static! {
    // This global stores only a weak pointer to the container.
    // It does not keep the container alive on its own.
    static ref GLOBAL_KAFKA_CONTAINER: AsyncMutex<Weak<KafkaContainerInfo>> = AsyncMutex::new(Weak::new());
}

// Container info struct.
pub struct KafkaContainerInfo {
    pub _container: ContainerAsync<GenericImage>,
}

/// Creates the Kafka container and sets it up. This function is only called
/// when no live container exists.
pub async fn create_kafka_container_delegate() -> KafkaContainerInfo {
    info!("Starting Kafka container setup...");

    let mapped_port = 19092;
    let controller_port = 19093;

    let container = GenericImage::new("jamfor352/kafka-test-container", "latest")
        .with_wait_for(WaitFor::healthcheck())
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", format!("1@127.0.0.1:{}", controller_port))
        .with_env_var("KAFKA_LISTENERS", format!("PLAINTEXT://0.0.0.0:{},CONTROLLER://0.0.0.0:{}", mapped_port, controller_port))
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!("PLAINTEXT://127.0.0.1:{},CONTROLLER://127.0.0.1:{}", mapped_port, controller_port))
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_mapped_port(mapped_port, mapped_port.tcp())
        .with_mapped_port(controller_port, controller_port.tcp())
        // .with_log_consumer(LoggingConsumer::new().with_stderr_level(log::Level::Error))
        .start()
        .await
        .expect("Failed to start Kafka");

    // Create topics.
    let topics = ["test-topic", "test-topic-dlq"];
    for topic in topics.iter() {
        info!("Creating topic: {}", topic);
        container
            .exec(ExecCommand::new([
                "kafka-topics",
                "--create",
                "--topic", topic,
                "--partitions", "2",
                "--replication-factor", "1",
                "--bootstrap-server", &format!("localhost:{}", mapped_port)
            ]))
            .await
            .expect("Failed to create topic");
    }
    
    info!("Kafka setup complete");
    KafkaContainerInfo {
        _container:container,
    }
}

/// Returns an Arc holding the Kafka container. If a container is already running
/// (as determined by the weak global), it will be reused. Otherwise, a new container
/// is created. By holding the lock during the entire check-and-create process,
/// we avoid a race condition where two tasks try to create the container concurrently.
pub async fn get_kafka_container() -> Arc<KafkaContainerInfo> {
    let mut lock = GLOBAL_KAFKA_CONTAINER.lock().await;
    if let Some(container) = lock.upgrade() {
        return container;
    }
    // Create a new container while holding the lock.
    let container = Arc::new(create_kafka_container_delegate().await);
    *lock = Arc::downgrade(&container);
    container
}

// Helper to create a Kafka producer.
pub fn create_producer() -> FutureProducer {
    rdkafka::ClientConfig::new()
        .set("bootstrap.servers", "localhost:19092")
        .set("message.timeout.ms", "5000")
        .set("debug", "all")  // Add debug logging if needed.
        .create()
        .expect("Failed to create producer")
}
