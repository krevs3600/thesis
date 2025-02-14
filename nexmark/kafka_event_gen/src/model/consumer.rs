use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio_stream::StreamExt;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use serde_json::Value;

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    incoming_events: Arc<Mutex<VecDeque<(u64, u64)>>>,
    event_generation_complete: Arc<AtomicBool>, 
}

impl KafkaConsumer {
    pub fn new(
        broker: &str, 
        topic: &str, 
        group_id: &str, 
        incoming_events: Arc<Mutex<VecDeque<(u64, u64)>>>,
        event_generation_complete: Arc<AtomicBool>,
    ) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&[topic])
            .expect("Subscribing to topic failed");

        KafkaConsumer { 
            consumer, 
            incoming_events,
            event_generation_complete
        }
    }

    pub async fn consume_messages(&self) {
        let mut stream = self.consumer.stream();
        let timeout_duration = tokio::time::Duration::from_secs(20);
        let mut last_message_time = SystemTime::now();
    
        while !self.event_generation_complete.load(Ordering::SeqCst) || SystemTime::now()
            .duration_since(last_message_time)
            .unwrap()
            .as_secs()
            < 10
        {
            match tokio::time::timeout(timeout_duration, stream.next()).await {
                Ok(Some(Ok(message))) => {
                    last_message_time = SystemTime::now();

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

                    let idx = match json_value["idx"].as_u64() {
                        Some(idx) => idx,
                        None => {
                            eprintln!("Missing or invalid index in payload");
                            continue;
                        }
                    };

                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    let mut incoming = self.incoming_events.lock().unwrap();
                    incoming.push_back((idx, timestamp));
                }
                Ok(Some(Err(e))) => {
                    eprintln!("Error while consuming message: {}", e);
                }
                Ok(None) | Err(_) => {
                    // Timeout occurred
                }
            }
        }
    
        eprintln!("No messages received for 60 seconds. Assuming stream has ended.");
    }
    
}