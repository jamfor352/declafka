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
use log::info;

/// Container info struct with port information
pub struct KafkaContainerInfo {
    pub _container: ContainerAsync<GenericImage>,
}

/// Creates and returns a new Kafka container with specified ports and topics
pub async fn provision_kafka_container(mapped_port: u16, controller_port: u16, topics: &[&str]) -> KafkaContainerInfo {
    info!("Starting Kafka container setup with ports {}, {}", mapped_port, controller_port);

    let container = GenericImage::new("confluentinc/cp-kafka", "7.9.0")
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
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
        .start()
        .await
        .expect("Failed to start Kafka");

    // Create topics
    for topic in topics {
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
        _container: container,
    }
}

// Helper to create a Kafka producer with specific port
pub fn create_producer(mapped_port: u16) -> FutureProducer {
    rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &format!("localhost:{}", mapped_port))
        .set("message.timeout.ms", "5000")
        .set("debug", "all")
        .create()
        .expect("Failed to create producer")
}
