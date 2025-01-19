CREATE TABLE kafka_sink (
    avg_price DECIMAL(24,3),
    seller BIGINT,
    idx BIGINT,
    PRIMARY KEY (seller) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'value.format' = 'json',
    'key.format' = 'json'
);