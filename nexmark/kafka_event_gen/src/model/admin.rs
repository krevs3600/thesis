use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication, };
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use std::time::Duration;

pub struct Admin {
    client: AdminClient<DefaultClientContext>,
}

impl Admin {
    pub fn new(brokers: &str) -> Self {
        let client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("Admin client creation error");

        Admin { client }
    }

    pub async fn topic_exists(&self, topic: &str) -> KafkaResult<bool> {
        let metadata = self.client.inner().fetch_metadata(None, Timeout::Never)?;
        Ok(metadata.topics().iter().any(|t| t.name() == topic))
    }

    pub async fn create_topic(&self, topic: &str) -> KafkaResult<()> {
        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        let res = self
            .client
            .create_topics(
                &[new_topic],
                &AdminOptions::new()
                    .operation_timeout(Some(Timeout::After(Duration::from_secs(10)))),
            )
            .await?;

        for result in res {
            match result {
                Ok(_) => println!("Topic {} created successfully", topic),
                Err((err, _)) => eprintln!("Failed to create topic {}: {:?}", topic, err),
            }
        }
        Ok(())
    }

       
    
    pub async fn empty_all_topics(&self) -> KafkaResult<()> {
        // Fetch metadata to get the list of all topics
        let metadata = self.client.inner().fetch_metadata(None, Timeout::Never)?;
        let topic_names: Vec<String> = metadata
            .topics()
            .iter()
            .filter(|topic| !topic.name().starts_with("__") && topic.name() != "_schemas") // Exclude internal topics
            .map(|topic| topic.name().to_string())
            .collect();
        
        let topic_names: Vec<&str> = topic_names.iter().map(String::as_str).collect();
        // Delete all topics
        let delete_res = self
            .client
            .delete_topics(
                &topic_names,
                &AdminOptions::new().operation_timeout(Some(Timeout::After(Duration::from_secs(10)))),
            )
            .await?;

        // Check for deletion errors
        for result in delete_res {
            if let Err((err, _)) = result {
                eprintln!("Failed to delete topic: {:?}", err);
            }
        }

        // Recreate the topics with default settings
        for topic in &topic_names {
            let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1)); // Adjust replication as needed
            let create_res = self
                .client
                .create_topics(
                    &[new_topic],
                    &AdminOptions::new()
                        .operation_timeout(Some(Timeout::After(Duration::from_secs(10)))),
                )
                .await?;

            for result in create_res {
                match result {
                    Ok(_) => println!("Topic {} recreated successfully", topic),
                    Err((err, _)) => eprintln!("Failed to recreate topic {}: {:?}", topic, err),
                }
            }
        }

        Ok(())
    }
    
    
}