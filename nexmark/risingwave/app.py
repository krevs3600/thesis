from psycopg2 import connect
from psycopg2._psycopg import connection
import time
from pandas import DataFrame
import asyncio
import asyncpg
from nexmark import *

from confluent_kafka import Consumer, KafkaException, TopicPartition


def is_kafka_queue_empty():

    bootstrap_server = "localhost:19092"
    topics = ["person-topic", "auction-topic", "bid-topic"]
    group_id = "rw-consumer"

    # Configure the consumer
    consumer = Consumer({
        'bootstrap.servers': bootstrap_server,
        'group.id': group_id,
        'enable.auto.commit': False,  # Avoid auto-committing offsets
        'auto.offset.reset': 'earliest'  # Start from earliest if no offset is stored
    })
    
    try:
        # Fetch metadata for the topic
        for topic in topics:
            partitions = consumer.list_topics(topic).topics[topic].partitions
            topic_partitions = [TopicPartition(topic, p) for p in partitions.keys()]
            
            # Get the end offsets for the topic partitions
            end_offsets = consumer.get_watermark_offsets(topic_partitions[0])
            
            # Get the committed offsets for the consumer group
            committed_offsets = consumer.committed(topic_partitions)
            
            # Compare offsets for each partition
            for tp, committed in zip(topic_partitions, committed_offsets):
                if committed is None or committed.offset < end_offsets[1]:
                    return False  # There are messages to consume
            return True
    except KafkaException as e:
        print(f"Kafka error: {e}")
        return None
    finally:
        consumer.close()


def create_db():
    conn = connect(
        host="localhost",      # Assuming RisingWave is mapped to localhost
        port="4566",           # Adjust if RisingWave is mapped to a different port
        user="root", 
        dbname="dev"    # Default database to connect first
    )

    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS streaming;")
    conn.commit()

def get_conn() -> connection:
    conn = connect(
        host="localhost",  
        port=4566,         
        user="root",
        dbname="streaming"
    )
    return conn

def create_sources(conn : connection) -> None:
    source_sql_statements = [
        """
        CREATE SOURCE IF NOT EXISTS bid (
            auction BIGINT,
            bidder BIGINT,
            price DOUBLE,
            channel VARCHAR,
            url VARCHAR,
            date_time TIMESTAMP,
            extra VARCHAR
            )
        WITH (
            connector='kafka',
            topic='bid-topic',
            properties.bootstrap.server='redpanda-0:9092',
            scan.startup.mode='earliest'
        ) FORMAT PLAIN ENCODE JSON;

        """,
        """
        CREATE SOURCE IF NOT EXISTS person (
            id BIGINT,
            name VARCHAR,
            email_address VARCHAR,
            credit_card VARCHAR,
            city VARCHAR,
            state VARCHAR,
            date_time TIMESTAMP,
            extra VARCHAR
        )
        WITH (
            connector='kafka',
            topic='person-topic',
            properties.bootstrap.server='redpanda-0:9092',
            scan.startup.mode='earliest'
        ) FORMAT PLAIN ENCODE JSON;
        """,
        """
        CREATE SOURCE IF NOT EXISTS auction (
            id BIGINT,
            item_name VARCHAR,
            description VARCHAR,
            initial_bid DOUBLE,
            reserve DOUBLE,
            date_time TIMESTAMP,
            expires TIMESTAMP,
            seller BIGINT,
            category INTEGER,
            extra VARCHAR
        )
        WITH (
            connector='kafka',
            topic='auction-topic',
            properties.bootstrap.server='redpanda-0:9092',
            scan.startup.mode='earliest'
        ) FORMAT PLAIN ENCODE JSON;
        """
    ]

    with conn.cursor() as cur:
        for sql in source_sql_statements:
            cur.execute(sql)
            print("Source created successfully.")
    conn.commit()

def create_materialized_view(conn : connection) -> None:
    view_sql_statements = [
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query1 AS
        SELECT auction, price * 0.908 AS price_dol, bidder, date_time
        FROM bid;
        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query2 AS
        SELECT auction, price
        FROM bid
        WHERE 
            (auction = 1007 OR
            auction = 1020 OR
            auction = 2001 OR
            auction = 2019 OR
            auction = 2087);
        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query3 AS
        SELECT person.name, person.city, person.state, auction.id
        FROM auction, person
        WHERE 
            auction.seller = person.id
            AND (person.state = 'or' OR person.state = 'id' OR person.state = 'ca')
            AND auction.category = 10;
        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS winning_bids AS
        SELECT 
            A.id AS auction_id,
            A.category AS category_id,
            MAX(B.price) AS final_price
        FROM 
            auction A
        JOIN 
            bid B
        ON 
            A.id = B.auction
        WHERE 
            B.date_time BETWEEN A.date_time AND A.expires
        GROUP BY 
            A.id, A.category;
        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query4 AS
        SELECT 
            category_id,
            AVG(final_price) AS average_price
        FROM 
            winning_bids
        GROUP BY 
            category_id;
        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query5 AS
        WITH hop_table AS (
            SELECT auction, COUNT(*) AS num
            FROM HOP(bid, date_time, INTERVAL '1 MINUTE', INTERVAL '60 MINUTES')
            GROUP BY auction
        ),
        max_count AS (
            SELECT MAX(num) AS max_num
            FROM hop_table
        )
        SELECT auction
        FROM hop_table, max_count
        WHERE num = max_num;
        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query6 AS
        SELECT AVG(Q.final) AS avg_final_price, Q.seller
        FROM (
            SELECT MAX(b.price) AS final, a.seller
            FROM auction a
            JOIN bid b ON a.id = b.auction
            WHERE b.date_time < a.expires
            AND a.expires < NOW()
            GROUP BY a.id, a.seller
        ) Q
        GROUP BY Q.seller;
        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query7 AS
        SELECT B.auction, B.price, B.bidder
        FROM (
            SELECT auction, price, bidder, MAX(price) AS max_price
            FROM HOP(
                bid,              
                date_time,
                INTERVAL '60 SECONDS',  
                INTERVAL '60 SECONDS'  
            )
            GROUP BY auction, price, bidder
        ) B
        WHERE B.price = B.max_price;

        """,
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query8 AS
        SELECT P.id, P.name, A.reserve
        FROM person P, auction A
        WHERE P.id = A.seller
        AND P.date_time >= NOW() - INTERVAL '12 HOURS'
        AND A.date_time >= NOW() - INTERVAL '12 HOURS';

        """
    ]
    
    with conn.cursor() as cur:
        for sql in view_sql_statements:
            start = time.time()
            cur.execute(sql)
            duration = time.time() - start
            print(f"Materialized view created successfully in {duration} seconds")
    conn.commit()
     
def create_subscribers(conn : connection) -> None:
    with conn.cursor() as cur:
        for i in range(1,9):
            cur.execute(f"""CREATE SUBSCRIPTION subscription{i} 
                            FROM query{i} WITH (
                                retention = '1 MINUTE'
                            );""")

            cur.execute(f"""DECLARE cursor{i} SUBSCRIPTION CURSOR FOR subscription{i} FULL;""")
            



async def process_subscription(conn, cursor_name):
    """
    Asynchronously fetch results from a RisingWave subscription cursor until the Kafka queue is empty.
    """
    start_time = None
    end_time = None
    all_results = []

    while True:
        async with conn.transaction():
            results = await conn.fetch(f"FETCH NEXT FROM {cursor_name};")
            
            if results:
                if not start_time:
                    # Mark the start time when the first results are fetched
                    start_time = time.time()
                all_results.extend(results)
            else:
                # Simulate Kafka monitoring to check if topic is empty
                if await is_kafka_queue_empty():  # Implement this function
                    end_time = time.time()
                    break
        await asyncio.sleep(0.5)  # Add a small delay to avoid busy polling

    return start_time, end_time, all_results  


def main():
    conn = get_conn()

    create_sources(conn)
    create_materialized_view(conn)
   
    
    for i in range(5):
        with conn.cursor() as cur:
            for query in ["query1", "query2", "query3", "query4", "query5", "query6", "query7", "query8"]:
                start = time.time() 
                cur.execute(f"select * from {query};")
                data = cur.fetchall()
                duration = time.time() - start
                print(DataFrame(data))
                print(f"{query} time: {duration}")
                time.sleep(1)
    

    conn.close()


def bench(debug=False):
    create_db()
    conn = get_conn()
    query : Query = Query8(conn)
    query.create_sources()
    query.drop_materialized_view()
    query.create_materialized_view()
    query.create_sink()
    #query.create_subscriber()
    if debug:
        for i in range(5):
            data = query.debug()
            print(data)
            time.sleep(10)
    conn.close()

if __name__ == "__main__":
    bench()
