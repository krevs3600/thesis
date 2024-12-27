mod events;
mod models;

use models::*;
use renoir::prelude::*;
use renoir::{operator::Timestamp, Replication, StreamContext};
use rdkafka::{config::RDKafkaLogLevel, ClientConfig, Message};
use tokio;
use std::{fs::File, io::Read, time::{Instant, SystemTime, UNIX_EPOCH}};


const WATERMARK_INTERVAL: usize = 64;


fn _parse_json(payload: &[u8]) -> Option<Event> {
    let payload_str = String::from_utf8_lossy(payload).to_string();
    serde_json::from_str::<Event>(&payload_str).ok()
}

fn watermark_gen(ts: &Timestamp, count: &mut usize, interval: usize) -> Option<Timestamp> {
    *count = (*count + 1) % interval;
    if *count == 0 {
        Some(*ts)
    } else {
        None
    }
}

fn create_consumer_config(broker: &str, group_id: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("group.id", group_id)
        .set("bootstrap.servers", broker)
        .set_log_level(RDKafkaLogLevel::Info)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest");
    config
}




fn query1() -> StreamContext{

    let ctx = StreamContext::new_local();
    let broker : &str = "localhost:19092";
    let bid_consumer = create_consumer_config(broker, "bid-group");
    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");
    
    
    let bid = ctx.stream_kafka(bid_consumer, &["bid-topic"], Replication::Unlimited);
    
    bid
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Bid>(&payload_str).ok()
        }))
        .filter_map(|event| event)// Filter out invalid messages
        .map(|mut bid| {
            bid.price = (bid.price as f32 * 0.908) as usize;
            bid
        })
        .map(|bid| {
            let json = serde_json::to_string(&bid).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic");
    ctx
}


fn query2() -> StreamContext {
    let ctx = StreamContext::new_local();

    let broker : &str = "localhost:19092";
    let bid_consumer = create_consumer_config(broker, "bid-group");
    
    let bid = ctx.stream_kafka(bid_consumer, &["bid-topic"], Replication::Unlimited);

    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");
    
    bid
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Bid>(&payload_str).ok()
        }))
        .filter_map(|bid| bid)
        .filter(|bid| {
            // Keep only bids with specified auction IDs
            let relevant_auctions = vec![1007, 1020, 2001, 2019, 2087];
            relevant_auctions.contains(&bid.auction)
        })
        .map(|bid| {
            let json = serde_json::to_string(&bid).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic");
    ctx
}

fn query3() -> StreamContext {
    let ctx = StreamContext::new_local();

    let broker : &str = "localhost:19092";
    let auction_consumer = create_consumer_config(broker, "auction-group");
    let person_consumer = create_consumer_config(broker, "person-group");
    
    let auction = ctx.stream_kafka(auction_consumer, &["auction-topic"], Replication::Unlimited);
    let person = ctx.stream_kafka(person_consumer, &["person-topic"], Replication::Unlimited);

    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");

    auction
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Auction>(&payload_str).ok()
        }))
        .filter_map(|auction| auction)
        .filter(|auction| auction.category == 10)
        .join(
            person
                .map(|m| m.payload().and_then(|payload| {
                    let payload_str = String::from_utf8_lossy(payload).to_string();
                    serde_json::from_str::<Person>(&payload_str).ok()
                }))
                .filter_map(|person| person)
                .filter(|person| {
                    let filter_state = vec!["OR", "ID", "CA"];
                    filter_state.contains(&person.state.as_str())
                }), 
            |auction| auction.seller , |person| person.id)
        .map(|(_, (auction, person))| (person.name, person.city, person.state, auction.id))
        .drop_key()
        .map(|row| {
            let json = serde_json::to_string(&row).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic");
    ctx
}

fn query4() -> StreamContext {

    let ctx = StreamContext::new_local();

    let broker : &str = "localhost:19092";
    let auction_consumer = create_consumer_config(broker, "auction-group");
    let bid_consumer = create_consumer_config(broker, "bid-group");
    
    let auction = ctx.stream_kafka(auction_consumer, &["auction-topic"], Replication::Unlimited);
    let bid = ctx.stream_kafka(bid_consumer, &["bid-topic"], Replication::Unlimited);

    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");
    /*SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
    GROUP BY A.id, A.category*/
    auction
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Auction>(&payload_str).ok()
        }))
        .filter_map(|auction| auction)
        .join(
            bid
                .map(|m| m.payload().and_then(|payload| {
                    let payload_str = String::from_utf8_lossy(payload).to_string();
                    serde_json::from_str::<Bid>(&payload_str).ok()
                }))
                .filter_map(|bid| bid), 
        |auction| auction.id,
        |bid| bid.auction)
        //.filter(|(_,(auction, bid))|
        //    bid.date_time >= auction.date_time && bid.date_time <= auction.expires
        //)
        .unkey()
        .map(|(_, (auction, bid))| (auction.id, auction.category, bid.price))
        .group_by_avg(|(id, category, _)| (*id, *category), |(_, _, price)| *price as f64)
        .unkey()
        .map(|row| {
            let json = serde_json::to_string(&row).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic");
    ctx
    
}


fn query5() -> StreamContext {

    let ctx = StreamContext::new_local();
    let broker : &str = "localhost:19092";

    let bid_consumer = create_consumer_config(broker, "bid-group");
    let bid = ctx.stream_kafka(bid_consumer, &["bid-topic"], Replication::Unlimited);

    let window_descr = EventTimeWindow::sliding(4, 2);
    let window_count = CountWindow::new(10, 5, true);
    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");
    
    // count how bids in each auction, for every window
    let counts = bid
        .batch_mode(BatchMode::single())
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Bid>(&payload_str).ok()
        }))
        .filter_map(|bid| bid)
        
        .add_timestamps(|bid| bid.date_time as i64,  {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        })
        
        .group_by(|a| a.auction)
        //.inspect(|row| println!("TS: {:?}", row))
        .window(window_descr.clone())
        .count()
        .unkey()
        .inspect(|row| println!("Item: {:?}", row));
 
    counts
        .window_all(window_descr)
        .max_by_key(|(_, v)| *v)
        .unkey()
        //.inspect(|row| println!("Item: {:?}", row)).for_each(std::mem::drop);
        
        .map(|(_, (auction, count))| {
            let json = serde_json::to_string(&(auction,count)).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic"); 
    ctx

}

 
fn query6() -> StreamContext {
    let ctx = StreamContext::new_local();
    let broker : &str = "localhost:19092";

    let auction_consumer = create_consumer_config(broker, "auction-group");
    let bid_consumer = create_consumer_config(broker, "bid-group");
    
    let bid = ctx.stream_kafka(bid_consumer, &["bid-topic"], Replication::Unlimited);
    let auction = ctx.stream_kafka(auction_consumer, &["auction-topic"], Replication::Unlimited);
    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");
    /*/// Query 6: Average Selling Price by Seller
///
/// ```text
/// SELECT Istream(AVG(Q.final), Q.seller)
/// FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
///       FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
///       WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
///       GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
/// GROUP BY Q.seller;
/// ``` */

    bid
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Bid>(&payload_str).ok()
        }))
        .flat_map(|bid| bid)
        .add_timestamps(|bid| bid.date_time as i64,  {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        })
        .join(
            auction.map(|m| m.payload().and_then(|payload| {
                let payload_str = String::from_utf8_lossy(payload).to_string();
                serde_json::from_str::<Auction>(&payload_str).ok()
            }))
            .flat_map(|auction| auction)
            .add_timestamps(|auction| auction.date_time as i64,  {
                let mut count = 0;
                move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
            }), 
            |bid| bid.auction, 
            |auction| auction.id)
        .filter(|(_, (bid, auction))| {
            let start = SystemTime::now();
            let since_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let millis = since_epoch.as_millis() as u64;
            bid.date_time <= auction.expires && auction.expires <= millis
        })
        .unkey()
        .map(|(_, (bid, auction))| (auction.seller, bid.price))
        .group_by(|(seller, _)| *seller)
        .window(CountWindow::sliding(10, 1))
        // AVG(Q.final)
        .fold((0, 0), |(sum, count), (_, price)| {
            *sum += price;
            *count += 1;
        })
        .map(|(_, (sum, count))| sum as f32 / count as f32)
        .unkey()
        .map(|bid| {
            let json = serde_json::to_string(&bid).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic");



    ctx
}


fn query7() -> StreamContext {
    
    let ctx = StreamContext::new_local();
    let broker : &str = "localhost:19092";

    let bid_consumer = create_consumer_config(broker, "bid-group");
    let bid = ctx.stream_kafka(bid_consumer, &["bid-topic"], Replication::Unlimited);

    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");
    
    let window_descr = EventTimeWindow::tumbling(10 * 1000);

    bid.map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Bid>(&payload_str).ok()
        }))
        .filter_map(|bid| bid )
        .map(|b| (b.auction, b.price, b.bidder))
        .key_by(|_| ())
        .window(window_descr.clone())
        .max_by_key(|(_, price, _)| *price)
        .drop_key()
        .window_all(window_descr)
        .max_by_key(|(_, price, _)| *price)
        .unkey()
        .map(|bid| {
            let json = serde_json::to_string(&bid).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic");
    ctx
}



/// Query 8: Monitor New Users
///
/// ```text
/// SELECT Rstream(P.id, P.name, A.reserve)
/// FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
/// WHERE P.id = A.seller;
/// ```
fn query8() -> StreamContext {
    let window_descr = EventTimeWindow::tumbling(10 * 1000);

    let ctx = StreamContext::new_local();
    let broker : &str = "localhost:19092";

    let auction_consumer = create_consumer_config(broker, "auction-group");
    let person_consumer = create_consumer_config(broker, "person-group");
    
    let person = ctx.stream_kafka(person_consumer, &["person-topic"], Replication::Unlimited);
    let auction = ctx.stream_kafka(auction_consumer, &["auction-topic"], Replication::Unlimited);

    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");

    let person = person
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Person>(&payload_str).ok()
        }))
        .filter_map(|person| person )
        .map(|p| (p.id, p.name));

    let auction = auction
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Auction>(&payload_str).ok()
        }))
        .filter_map(|auction| auction )
        .map(|a| (a.seller, a.reserve));

    person
        .group_by(|(id, _)| *id)
        .window_join(window_descr, auction.group_by(|(seller, _)| *seller))
        .drop_key()
        .map(|((id, name), (_, reserve))| (id, name, reserve))
        .map(|bid| {
            let json = serde_json::to_string(&bid).expect("Failed to serialize Bid");
            json.into_bytes()
        })
        .write_kafka(producer, "renoir-topic");
    ctx
}   


fn test() -> StreamContext{

    let ctx = StreamContext::new_local();
    

    let broker : &str = "localhost:19092";
    let bid_consumer = create_consumer_config(broker, "bid-group");
    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000");
    
    
    let bid = ctx.stream_kafka(bid_consumer, &["bid-topic"], Replication::Unlimited);
    
    bid
        .map(|m| m.payload().and_then(|payload| {
            let payload_str = String::from_utf8_lossy(payload).to_string();
            serde_json::from_str::<Bid>(&payload_str).ok()
        }))
        .filter_map(|event| event)// Filter out invalid messages
        .add_timestamps(|bid| bid.date_time as i64,  {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        })
        .map(|mut bid| {
            bid.price = (bid.price as f32 * 0.908) as usize;
            bid
        })
        .inspect(|row| println!("Item: {:?}", row)).for_each(std::mem::drop);  // (i32 ps_partkey, f64 mis_supply_cost) 
        
        
        
        //.write_kafka(producer, "renoir-topic");
    ctx
}

fn _read_json_to_vec(file_path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Open the file
    let mut file = File::open(file_path)?;

    // Read the file contents into a string
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    // Parse the JSON string into a Vec<String>
    let data: Vec<String> = serde_json::from_str(&contents)?;

    Ok(data)
}

fn _bids_test() {
    // 1 read json file
    // 2 move all raw string to a vec
    // start deserializing
    // apply operator
    // sink and measure time

    // then measure time without des and operations
    
    let file_path = "/home/carlo/Desktop/thesis/tests/des_time_streaming/bids_row.json";
    let data = _read_json_to_vec(file_path).unwrap();
    let ctx : StreamContext = StreamContext::new_local();
    let bid = ctx.stream_iter(data.clone().into_iter());
    bid
        .map(|m| {
            serde_json::from_str::<Bid>(&m).ok()
        })
        .filter_map(|event| event)// Filter out invalid messages
        .map(|mut bid| {
            bid.price = (bid.price as f32 * 0.908) as usize;
            bid
        })
        .for_each(|x| std::mem::drop(x));
    
    let start = Instant::now();
    ctx.execute_blocking();
    let op = Instant::now() - start;
    println!("with op elapsed time is: {}", op.as_secs_f32());

    let ctx : StreamContext = StreamContext::new_local();
    let bid = ctx.stream_iter(data.clone().into_iter());
    bid
        .map(|m| {
            serde_json::from_str::<Bid>(&m).ok()
        })
        .filter_map(|event| event)
        .for_each(|x| std::mem::drop(x));
    let start = Instant::now();
    ctx.execute_blocking();
    let no_op = Instant::now() - start;
    println!("without op elapsed time is: {}", no_op.as_secs_f32());

    println!("op/no_op: {}", op.as_secs_f32()/no_op.as_secs_f32());
}


use clap::Parser;

/// A simple program to execute queries based on a provided parameter.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The query number to execute (0 to 8)
    #[arg(short, long)]
    query: u8,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    
    match args.query {
        0 => test().execute().await,
        1 => query1().execute().await,
        2 => query2().execute().await,
        3 => query3().execute().await,
        4 => query4().execute().await,
        5 => query5().execute().await,
        6 => query6().execute().await,
        7 => query7().execute().await,
        8 => query8().execute().await,
        _ => {
            eprintln!("Error: query number must be between 0 and 8.");
            std::process::exit(1);
        }
    }
}
