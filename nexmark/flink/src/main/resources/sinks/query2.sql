CREATE TABLE kafka_sink (
    idx BIGINT,
    auction BIGINT,
    price BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);