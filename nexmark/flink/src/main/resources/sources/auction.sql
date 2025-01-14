CREATE TABLE auction (
    idx BIGINT,
    id BIGINT,
    item_name STRING,
    description STRING,
    initial_bid BIGINT,
    reserve BIGINT,
    date_time TIMESTAMP_LTZ(3),
    expires TIMESTAMP_LTZ(3),
    seller BIGINT,
    category INT,
    extra STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'auction-topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);