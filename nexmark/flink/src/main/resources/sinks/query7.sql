CREATE TABLE kafka_sink (
    auction BIGINT,
    price BIGINT,
    bidder BIGINT,
    idx BIGINT,
    window_start TIMESTAMP_LTZ(3),
    window_end TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);