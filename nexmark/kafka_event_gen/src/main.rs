mod model;


use clap::{Parser, Subcommand};

use model::consumer::KafkaConsumer;
use model::events::KafkaEvent;
use model::producer::KafkaEventProducer;
use nexmark::config::NexmarkConfig;
use model::admin;

use tokio::task;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::{collections::VecDeque, sync::Arc, sync::Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::fs::{File, create_dir_all};
use std::io::Write;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Reset Kafka by clearing and recreating all topics
    Reset,
    /// Generate data with specified parameters
    Generate {
        /// Number of generators
        #[arg(short, long, default_value = "1")]
        generators: usize,
        /// Number of events per generator
        #[arg(short, long, default_value = "100000")]
        events: usize,
        /// Number of steps in the process
        #[arg(short, long, default_value = "50")]
        steps: usize,
        /// Topic to read from
        #[arg(short, long)]
        topic: Option<String>,
        /// Query that it's been executed
        #[arg(short, long, default_value = "1")]
        query: Option<usize>
        
    },
}

#[tokio::main]
async fn main() {
    // Parse command line arguments using Clap
    let args = Cli::parse();

    // Define the Kafka brokers and the list of topics to be used
    let brokers = "localhost:19092";
    let topics = vec![
        "person-topic", 
        "auction-topic", 
        "bid-topic", 
        "renoir-topic", 
        "flink-topic", 
        "risingwave-topic"
    ];

    // Create Kafka admin object for topic management
    let admin = admin::Admin::new(brokers);

    // Match the user's command and perform the respective actions
    match args.command {
        Commands::Reset => {
            // Clear and recreate topics if --reset flag is provided
            match admin.empty_all_topics().await {
                Ok(_) => println!("Emptied all topics"),
                Err(error) => println!("{}", error),
            };

            // Recreate topics that are defined in the list
            for topic in &topics {
                if let Ok(exists) = admin.topic_exists(topic).await {
                    if exists {
                        println!("Topic {} already exists", topic);
                    } else if let Err(err) = admin.create_topic(topic).await {
                        eprintln!("Failed to create topic {}: {:?}", topic, err);
                    }
                } else {
                    eprintln!("Failed to check if topic {} exists", topic);
                }
            }
        }
        Commands::Generate {
            generators,
            events,
            steps,
            topic,
            query
        } => {
            // Proceed to generate events if --generate flag is provided
            let producer = Arc::new(KafkaEventProducer::new(brokers));
            let incoming_events = Arc::new(Mutex::new(VecDeque::new()));
            let event_generation_complete = Arc::new(AtomicBool::new(false));
            
            let consumer_event_complete = Arc::clone(&event_generation_complete);
            let consumer = KafkaConsumer::new(
                brokers,
                format!("{}-topic", topic.clone().unwrap()).as_str(), 
                "res-consumer",  // Consumer group name
                Arc::clone(&incoming_events),
                consumer_event_complete
            );

            // Spawn consumer task to consume messages from Kafka
            let consumer_handle = task::spawn(async move {
                consumer.consume_messages().await;
            });
            
            // Data structure to store outgoing events for later saving
            let outgoing_events: Arc<Mutex<VecDeque<(u64, u64)>>> = Arc::new(Mutex::new(VecDeque::new()));

            // Event generation parameters
            let total_events = events;
            let num_generators = generators;

            // Spawn multiple generator tasks to generate events
            let generator_handles: Vec<_> = (0..num_generators)
                .map(|i| {
                    let producer = Arc::clone(&producer);
                    let outgoing_events = Arc::clone(&outgoing_events);

                    task::spawn(async move {
                        // Configure event generator
                        let conf = NexmarkConfig {
                            num_event_generators: num_generators,
                            first_rate: steps,
                            next_rate: steps,
                            first_event_id: 50,
                            ..Default::default()
                        };
                        let generator = nexmark::EventGenerator::new(conf)
                            .with_offset(i as u64)
                            .with_step(num_generators as u64);

                        // Calculate events per generator and handle remainder
                        let events_per_generator = total_events / num_generators;
                        let extra_event = (i < total_events % num_generators) as usize;

                        // Generate events and send them to Kafka
                        for (idx, event) in generator.take(events_per_generator + extra_event).enumerate() {
                            let json_value = event.to_json(idx as u64);  // Convert event to JSON
                            let topic = event.topic();  // Kafka topic for the event
                            let key = event.key();  // Event key for partitioning
                            let json_str = serde_json::to_string(&json_value).expect("Failed to serialize event to JSON");

                            // Get current timestamp
                            let time_stamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_nanos() as u64;

                            // Send event message to Kafka
                            producer.send_message(topic, &json_str, &key).await;

                            // Store event data in outgoing events queue
                            let mut outgoing = outgoing_events.lock().unwrap();
                            outgoing.push_back((idx as u64, time_stamp));
                        }
                    })
                })
                .collect();

            // Wait for some time to allow event generation
            // sleep(Duration::new(10, 0));

            

            // Wait for all generator tasks to complete
            for handle in generator_handles {
                handle.await.unwrap();
            }

            event_generation_complete.store(true, Ordering::SeqCst);

            // Wait for consumer to finish
            consumer_handle.await.unwrap();

            // Save events to CSV and ARC if paths are provided
            let path = format!("{}/{}", topic.clone().unwrap(), query.unwrap());
            create_dir_all(&path).expect("Failed to create directories");
            save_events_to_csv(format!("{}/outgoing_events.csv", path).as_str(), &outgoing_events.lock().unwrap());
            save_events_to_csv(format!("{}/incoming_events.csv", path).as_str(), &incoming_events.lock().unwrap());
        }
    }
}

/// Function to save events to a CSV file
fn save_events_to_csv(filename: &str, events: &VecDeque<(u64, u64)>) {
    let mut file = File::create(filename).expect("Failed to create file");
    writeln!(file, "event_time,timestamp").expect("Failed to write header");
    for (event_time, timestamp) in events {
        writeln!(file, "{},{}", event_time, timestamp).expect("Failed to write event");
    }
}
