use axum::{
    extract::{State, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, sync::{Arc, Mutex}};
use chrono::Utc;

// EventData struct to hold event time and timestamp
#[derive(Debug, Serialize, Deserialize, Clone)]
struct EventData {
    event_time: String,
    timestamp: String,
}

// DataStore to hold separate vectors for pre-processed and post-processed events
struct DataStore {
    pre_processed_event: Mutex<Vec<EventData>>,
    post_processed_event: Mutex<Vec<EventData>>,
}

impl DataStore {
    fn new() -> Self {
        Self {
            pre_processed_event: Mutex::new(Vec::new()),
            post_processed_event: Mutex::new(Vec::new()),
        }
    }
}

#[tokio::main]
async fn main() {
    let shared_state = Arc::new(DataStore::new());

    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/pre_proc", post(pre_proc))
        .route("/post_proc", post(post_proc))
        .route("/save_data", get(save_data))
        .with_state(shared_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// Handler for `/pre_proc`
async fn pre_proc(
    State(state): State<Arc<DataStore>>,
    Json(payload): Json<String>, // Accept event_time as plain text or JSON
) -> &'static str {
    let timestamp = Utc::now().to_rfc3339();
    let event = EventData {
        event_time: payload,
        timestamp,
    };

    let mut pre_processed = state.pre_processed_event.lock().unwrap();
    pre_processed.push(event);

    "Event added to pre-processed data"
}

// Handler for `/post_proc`
async fn post_proc(
    State(state): State<Arc<DataStore>>,
    Json(payload): Json<String>, // Accept event_time as plain text or JSON
) -> &'static str {
    let timestamp = Utc::now().to_rfc3339();
    let event = EventData {
        event_time: payload,
        timestamp,
    };

    let mut post_processed = state.post_processed_event.lock().unwrap();
    post_processed.push(event);

    "Event added to post-processed data"
}

// Handler for `/save_data`
async fn save_data(State(state): State<Arc<DataStore>>) -> &'static str {
    let pre_processed = state.pre_processed_event.lock().unwrap();
    let post_processed = state.post_processed_event.lock().unwrap();

    save_to_csv("pre_processed.csv", &pre_processed);
    save_to_csv("post_processed.csv", &post_processed);

    "Data saved to CSV files"
}

// Utility function to save events to a CSV file
fn save_to_csv(filename: &str, data: &[EventData]) {
    let mut file = File::create(filename).expect("Unable to create file");
    writeln!(file, "event_time,timestamp").expect("Unable to write header");

    for event in data {
        writeln!(file, "{},{}", event.event_time, event.timestamp)
            .expect("Unable to write data");
    }
}
