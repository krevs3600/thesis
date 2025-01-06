CREATE TABLE IF NOT EXISTS execution_times (
    benchmark TEXT,
    backend TEXT,
    test TEXT,
    query_id INTEGER,
    execution_time REAL,
    run_id INT,
    PRIMARY KEY (benchmark, backend, test, query_id, run_id)
);