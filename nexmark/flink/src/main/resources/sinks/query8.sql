CREATE TABLE kafka_sink (
    id BIGINT,
    name VARCHAR,
    reserve BIGINT,
    idx BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'format' = 'json',
    'sink.partitioner' = 'round-robin'
);