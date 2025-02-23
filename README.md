# Declafka - Declarative Kafka Listeners for Rust

## Disclaimer

This is my personal learning and research project! 
I'm new to Rust, figuring it out as I go, with a background in Kafka and Java (think Spring Kafka). 

---

## What is Declafka?

Declafka is a lightweight library for building Kafka consumers in Rust with a declarative, annotation-driven approach. It aims to cut boilerplate, simplify configuration, and offer type-safe message handling, retries, and dead letter queues (DLQs). The name blends "declarative" and "Kafka," with a wink to "declutter" for cleaner code.

Inspired by Java's Spring `@KafkaListener` and Micronaut Kafka, it brings that ease to Rust while leaning on Rust's performance and safety strengths. It's built on the `rdkafka` crate (via `librdkafka`), the go-to choice at the time.

This is an early-stage project. I'm still learning Rust, so it's a work in progress. Shared for anyone who finds it useful or wants to riff on it. PRs are welcome!

---

## Features

### Declarative Consumers
Define listeners with a simple macro:

```rust
use declafka_macro::kafka_listener
use serde::{Serialize, Deserialize}

#[derive(Clone, Serialize, Deserialize)]
struct Order
    id: String
    amount: f64

#[kafka_listener(
    topic = "orders",
    listener_id = "order-listener",
    yaml_path = "kafka.yaml",
    deserializer = "json_deserializer",
)]
fn handle_order(order: Order) -> Result<(), declafka_lib::Error>
    println!("Processing order: {}", order.id)
    Ok(())
```

### Core Features
- **Type-Safe Deserialization**: Messages auto-deserialized into your types.
- **Error Handling**: Configurable retries with exponential backoff, plus DLQ support.
- **Offset Management**: Manual tracking to avoid data loss, auto-commits on rebalance.
- **Graceful Shutdown**: Clean exits with offset commits, handles system signals.
- **Configuration**: YAML-based with environment variable overrides.

---

## Configuration

### YAML Config
Define listener settings in `kafka.yaml`. Any valid Kafka consumer property can be used - the library will automatically pass them through to the underlying consumer:

```yaml
kafka:
  # Default settings (optional) - inherited by all listeners unless overridden
  default:
    bootstrap.servers: "localhost:9092"
    auto.offset.reset: "earliest"
    enable.auto.commit: "false"
    
  # Specific listener configurations
  order-listener:
    # Override defaults or add specific settings
    bootstrap.servers: "kafka-prod:9092"
    group.id: "order-processing-group"
    
    # Security settings
    security.protocol: "SASL_SSL"
    sasl.mechanism: "PLAIN"
    sasl.username: "${KAFKA_USER}"  # Use env vars in values
    sasl.password: "${KAFKA_PASS}"
    
    # Performance tuning
    fetch.max.bytes: "52428800"
    max.partition.fetch.bytes: "1048576"
    
    # Any valid Kafka consumer property works automatically
    max.poll.records: "500"
    session.timeout.ms: "45000"
    heartbeat.interval.ms: "15000"

  # Another listener example
  notification-listener:
    bootstrap.servers: "kafka-dev:9092"
    group.id: "notification-group"
    # Minimal config, inherits remaining settings from default
```

### Environment Overrides
Override any Kafka property via environment variables, using either listener-specific or global overrides:

```bash
# Listener-specific overrides
export KAFKA_ORDER_LISTENER_BOOTSTRAP_SERVERS="kafka:9092"
export KAFKA_ORDER_LISTENER_GROUP_ID="custom-group"
export KAFKA_ORDER_LISTENER_MY_CUSTOM_PROPERTY="new-value"

# Global overrides (applies to all listeners)
export KAFKA_GLOBAL_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_GLOBAL_MAX_POLL_RECORDS="1000"
```

Environment variables take precedence in this order:
1. Listener-specific overrides
2. Global overrides
3. YAML configuration
4. Default settings

---

## Getting Started

### Requirements
- Rust 1.70+ (check your `rustc --version` and adjust as needed!)

### Setup
Add to your `Cargo.toml`:

```toml
[dependencies]
declafka_lib = { git = "https://github.com/jamfor352/declafka", tag = "v0.0.3-alpha" }
declafka_macro = { git = "https://github.com/jamfor352/declafka", tag = "v0.0.3-alpha" }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

### Example
Define a message handler:

```rust
use declafka_macro::kafka_listener
use serde::{Serialize, Deserialize}

#[derive(Clone, Serialize, Deserialize)]
struct MyMessage
    field1: String
    field2: i32

fn json_deserializer(payload: &[u8]) -> Option<MyMessage>
    serde_json::from_slice(payload).ok()

#[kafka_listener(
    topic = "my-topic",
    listener_id = "my-listener",
    yaml_path = "kafka.yaml",
    deserializer = "json_deserializer",
    dlq_topic = "my-topic-dlq",
)]
fn handle_message(msg: MyMessage) -> Result<(), declafka_lib::Error>
    println!("Got message: {}", msg.field1)
    Ok(())

#[tokio::main]
async fn main()
    let listener = handle_message_listener()
        .expect("Failed to initialize listener")
    listener.start()
    tokio::signal::ctrl_c().await.unwrap()
```

---

## Advanced Usage

### Retries
Add retry logic:

```rust
#[kafka_listener(
    topic = "my-topic",
    listener_id = "my-listener",
    yaml_path = "kafka.yaml",
    deserializer = "json_deserializer",
    retry_max_attempts = 5,
    retry_initial_backoff = 200,  // ms
    retry_max_backoff = 5000,     // ms
    retry_multiplier = 1.5,
)]
fn handle_message(msg: MyMessage) -> Result<(), declafka_lib::Error>
    println!("Got message: {}", msg.field1)
    Ok(())
```

### Web Integration (e.g., Actix)
Run alongside a web server:

```rust
use actix_web::{App, HttpServer}

#[actix_web::main]
async fn main() -> std::io::Result<()>
    let listener = handle_message_listener()
        .expect("Failed to initialize listener")
    listener.start()
    HttpServer::new(|| App::new())
        .bind("127.0.0.1:8080")?
        .run()
        .await
```

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See the file for details.
