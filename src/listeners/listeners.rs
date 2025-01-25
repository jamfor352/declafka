use log::{info, warn};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch::Receiver;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

pub async fn consume_and_print(brokers: &str, group_id: &str, mut shutdown_notify: Receiver<bool>, kafka_offset_tracker: Arc<Mutex<HashMap<i32, i64>>>) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["topic-a"])
        .expect("Can't subscribe to topic-a");


    loop {
        tokio::select! {
            result = consumer.recv() => {
                match result {
                    Err(e) => warn!("Kafka error: {}", e),
                    Ok(m) => {
                        let partition = m.partition();
                        let offset = m.offset();

                        // Store the latest offset for this partition
                        let mut tracker = kafka_offset_tracker.lock().unwrap();
                        tracker.insert(partition, offset);
                        drop(tracker);
                        let payload = match m.payload_view::<str>() {
                            None => "",
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                warn!("Error while deserializing message payload: {:?}", e);
                                ""
                            }
                        };

                        info!(
                            "key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                            m.key(), payload, m.topic(), partition, offset, m.timestamp()
                        );

                        if let Some(headers) = m.headers() {
                            for header in headers.iter() {
                                info!("  Header {:#?}: {:?}", header.key, header.value);
                            }
                        }

                        // Commit asynchronously as usual
                        consumer.commit_message(&m, CommitMode::Async).unwrap();
                    }
                }
            },
            _ = shutdown_notify.changed() => {
                warn!("Kafka consumer shutting down gracefully...");

                // Commit stored offsets synchronously
                let tracker = kafka_offset_tracker.lock().unwrap();
                for (&partition, &offset) in tracker.iter() {
                    let mut topic_partition_list = rdkafka::TopicPartitionList::new();
                    topic_partition_list.add_partition_offset("topic-a", partition, Offset::Offset(offset)).unwrap();

                    consumer.commit(&topic_partition_list, CommitMode::Sync).unwrap();
                    info!("Committed offset {} for partition {}", offset, partition);
                }
                break;
            }
        }
    }
}
