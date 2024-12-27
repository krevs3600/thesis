use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio_stream::StreamExt;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use serde_json::Value;

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    incoming_events: Arc<Mutex<VecDeque<(u64, u64)>>>,
}

impl KafkaConsumer {
    pub fn new(broker: &str, topic: &str, group_id: &str, incoming_events: Arc<Mutex<VecDeque<(u64, u64)>>>) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&[topic])
            .expect("Subscribing to topic failed");

        KafkaConsumer { consumer, incoming_events }
    }

    pub async fn consume_messages(&self) {
        let mut stream = self.consumer.stream();
        let timeout_duration = tokio::time::Duration::from_secs(60);
    
        while let Ok(Some(result)) = tokio::time::timeout(timeout_duration, stream.next()).await {
            match result {
                Ok(message) => {
                    let payload = match message.payload_view::<str>() {
                        Some(Ok(payload)) => payload,
                        Some(Err(e)) => {
                            eprintln!("Error while deserializing message payload: {:?}", e);
                            continue;
                        }
                        None => {
                            eprintln!("Failed to get message payload");
                            continue;
                        }
                    };
    
                    let json_value: Value = match serde_json::from_str(payload) {
                        Ok(json) => json,
                        Err(e) => {
                            eprintln!("Failed to parse JSON payload: {:?}", e);
                            continue;
                        }
                    };
    
                    let event_time = match json_value["date_time"].as_u64() {
                        Some(time) => time,
                        None => {
                            eprintln!("Missing or invalid event_time in payload");
                            continue;
                        }
                    };
    
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;
    
                    let mut incoming = self.incoming_events.lock().unwrap();
                    incoming.push_back((event_time, timestamp));
                }
                Err(error) => {
                    eprintln!("Error while consuming message: {}", error);
                }
            }
        }
    
        eprintln!("No messages received for 60 seconds. Assuming stream has ended.");
    }
    
}