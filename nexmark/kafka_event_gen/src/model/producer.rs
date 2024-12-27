use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::time::Duration;

pub struct KafkaEventProducer {
    producer: FutureProducer
}

impl KafkaEventProducer {
    pub fn new(brokers: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("Producer creation failed");

            KafkaEventProducer {
            producer
        }
    }

    pub async fn send_message(&self, topic: &str, payload : &str, key : &str) {
        self.producer
            .send(
                FutureRecord::to(topic)
                    .payload(payload)
                    .key(key),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .unwrap();
    }
}