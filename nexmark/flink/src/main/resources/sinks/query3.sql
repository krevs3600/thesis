CREATE TABLE kafka_sink (
    name VARCHAR,
    city VARCHAR,
    state VARCHAR,
    id BIGINT,
    idx BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);