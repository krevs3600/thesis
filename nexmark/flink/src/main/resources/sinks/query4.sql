CREATE TABLE kafka_sink (
    category_id BIGINT,
    average_price DECIMAL(24, 3),
    idx BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);