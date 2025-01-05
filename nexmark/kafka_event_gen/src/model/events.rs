
use serde_json::{json, Value};
use nexmark::event::{Event, Person, Auction, Bid};
use chrono::{prelude::DateTime, Local};
use chrono::Utc;
use std::time::{UNIX_EPOCH, Duration};


// Define the trait to convert events to JSON and specify their topics


pub trait  KafkaEvent {
    fn to_json(&self, idx : u64) -> Value;
    fn topic(&self) -> &str;
    fn key(&self) -> String;
    fn get_event_time(&self) -> u64;
    fn get_id(&self) -> usize;
    
}

// Implement `KafkaEvent` for `Person`
impl KafkaEvent for Person {
    fn to_json(&self, idx : u64) -> Value {
        let d = UNIX_EPOCH + Duration::from_millis(self.date_time);
        let datetime = DateTime::<Utc>::from(d);
        let timestamp_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        json!({
            "idx" : idx,
            "id": self.id,
            "name": self.name,
            "email_address": self.email_address,
            "credit_card": self.credit_card,
            "city": self.city,
            "state": self.state,
            "date_time": timestamp_str,
            "extra": self.extra
        })
    }

    fn topic(&self) -> &str {
        "person-topic"
    }

    fn key(&self) -> String {
        self.id.to_string()
    }

    fn get_event_time(&self) -> u64{
        self.date_time
    }

    fn get_id(&self) -> usize {
        self.id   
    }
}

impl KafkaEvent for Auction {
    fn to_json(&self, idx : u64) -> Value {
        let d = UNIX_EPOCH + Duration::from_millis(self.date_time);
        let datetime = DateTime::<Local>::from(d);
        let time_date_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();

        let d = UNIX_EPOCH + Duration::from_millis(self.date_time);
        let datetime = DateTime::<Local>::from(d);
        let expire_date_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        json!({
            "idx" : idx,
            "id": self.id,
            "item_name": self.item_name,
            "description": self.description,
            "initial_bid": self.initial_bid,
            "reserve": self.reserve,
            "date_time": time_date_str,
            "expires": expire_date_str,
            "seller": self.seller,
            "category": self.category,
            "extra": self.extra
        })
    }

    fn topic(&self) -> &str {
        "auction-topic"
    }

    fn key(&self) -> String {
        self.id.to_string()
    }

    fn get_event_time(&self) -> u64{
        self.date_time
    }

    fn get_id(&self) -> usize {
        self.id   
    }
}


impl KafkaEvent for Bid {
    fn to_json(&self, idx : u64) -> Value {
        let d = UNIX_EPOCH + Duration::from_millis(self.date_time);
        let datetime = DateTime::<Local>::from(d);
        let timestamp_str = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        json!({
            "idx" : idx,
            "auction": self.auction,
            "bidder": self.bidder,
            "price": self.price,
            "channel": self.channel,
            "url": self.url,
            "date_time": timestamp_str,
            "extra": self.extra
        })
    }

    fn topic(&self) -> &str {
        "bid-topic"
    }

    fn key(&self) -> String {
        self.auction.to_string()
    }

    fn get_event_time(&self) -> u64{
        self.date_time
    }

    fn get_id(&self) -> usize {
        0
    }
}


impl KafkaEvent for Event {
    fn to_json(&self, idx : u64) -> Value {
        match self {
            Event::Person(p) => p.to_json(idx),
            Event::Auction(a) => a.to_json(idx),
            Event::Bid(b) => b.to_json(idx),
        }
    }

    fn topic(&self) -> &str {
        match self {
            Event::Person(p) => p.topic(),
            Event::Auction(a) => a.topic(),
            Event::Bid(b) => b.topic(),
        }
    }

    fn key(&self) -> String {
        match self {
            Event::Person(p) => p.key(),
            Event::Auction(a) => a.key(),
            Event::Bid(b) => b.key(),
        }
    }

    fn get_event_time(&self) -> u64 {
        
        match self {
            Event::Person(p) => p.get_event_time(),
            Event::Auction(a) => a.get_event_time(),
            Event::Bid(b) => b.get_event_time(),
        }
    }

    fn get_id(&self) -> usize {
        
        match self {
            Event::Person(p) => p.get_id(),
            Event::Auction(a) => a.get_id(),
            Event::Bid(b) => b.get_id(),
        }
    }
}
