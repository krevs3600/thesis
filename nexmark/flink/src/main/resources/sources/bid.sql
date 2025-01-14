CREATE TABLE bid (
    idx BIGINT,
    auction BIGINT,
    bidder BIGINT,
    price DOUBLE,
    channel VARCHAR,
    url VARCHAR,
    date_time TIMESTAMP,
    extra VARCHAR,
    WATERMARK FOR date_time AS date_time - INTERVAL '5 SECOND'
)
WITH (
    'connector'='kafka',
    'topic'='bid-topic',
    'properties.bootstrap.server'='localhost:9092',
    'scan.startup.mode'='earliest',
    'format' = 'json',
);