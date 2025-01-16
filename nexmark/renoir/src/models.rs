use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Person {
    pub idx: usize,
    pub id: usize,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: String,
    pub extra: String,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Auction {
    pub idx: usize,
    pub id: usize,
    pub item_name: String,
    pub description: String,
    pub initial_bid: usize,
    pub reserve: usize,
    pub date_time: String,
    pub expires: String,
    pub seller: usize,
    pub category: usize,
    pub extra: String,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Bid {
    pub idx: usize,
    pub auction: usize,
    pub bidder: usize,
    pub price: usize,
    pub channel: String,
    pub url: String,
    pub date_time: String,
    pub extra: String,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}
