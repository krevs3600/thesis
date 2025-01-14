CREATE TABLE person (
    idx BIGINT,
    id BIGINT,
    name VARCHAR,
    email_address VARCHAR,
    credit_card VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP,
    extra VARCHAR,
    WATERMARK FOR date_time AS date_time - INTERVAL '5 SECOND'
)
WITH (
    'connector'='kafka',
    'topic'='person-topic',
    'properties.bootstrap.server'='localhost:9092',
    'scan.startup.mode'='earliest',
    'format' = 'json',
);