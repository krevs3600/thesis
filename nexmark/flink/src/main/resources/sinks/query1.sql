CREATE TABLE kafka_sink (
    idx BIGINT,
    auction BIGINT,
    bidder BIGINT,
    price DECIMAL(24, 3),
    channel STRING,
    url STRING,
    date_time TIMESTAMP_LTZ(3),
    extra STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);