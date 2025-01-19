CREATE TABLE kafka_sink (
    category_id BIGINT,
    average_price DECIMAL(24, 3),
    idx BIGINT,
    PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'flink-topic',
    'properties.bootstrap.servers' = 'localhost:19092',
    'key.format' = 'json',
    'value.format' = 'json'
);