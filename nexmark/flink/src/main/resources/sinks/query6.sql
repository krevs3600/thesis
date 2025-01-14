CREATE TABLE kafka_sink (
    avg_price DECIMAL(24,3),
    seller BIGINT,
    idx BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);