
use serde_json::{json, Value};
use nexmark::event::{Event, Person, Auction, Bid};

// Define the trait to convert events to JSON and specify their topics


pub trait  KafkaEvent {
    fn to_json(&self) -> Value;
    fn topic(&self) -> &str;
    fn key(&self) -> String;
    
}

// Implement `KafkaEvent` for `Person`
impl KafkaEvent for Person {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "email_address": self.email_address,
            "credit_card": self.credit_card,
            "city": self.city,
            "state": self.state,
            "date_time": self.date_time,
            "extra": self.extra
        })
    }

    fn topic(&self) -> &str {
        "person-topic"
    }

    fn key(&self) -> String {
        self.id.to_string()
    }
}

impl KafkaEvent for Auction {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "item_name": self.item_name,
            "description": self.description,
            "initial_bid": self.initial_bid,
            "reserve": self.reserve,
            "date_time": self.date_time,
            "expires": self.expires,
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
}


impl KafkaEvent for Bid {
    fn to_json(&self) -> Value {
        json!({
            "auction": self.auction,
            "bidder": self.bidder,
            "price": self.price,
            "channel": self.channel,
            "url": self.url,
            "date_time": self.date_time,
            "extra": self.extra
        })
    }

    fn topic(&self) -> &str {
        "bid-topic"
    }

    fn key(&self) -> String {
        self.auction.to_string()
    }
}


impl KafkaEvent for Event {
    fn to_json(&self) -> Value {
        match self {
            Event::Person(p) => p.to_json(),
            Event::Auction(a) => a.to_json(),
            Event::Bid(b) => b.to_json(),
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
}
