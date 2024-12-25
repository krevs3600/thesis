-- Create table for CSV data
CREATE TABLE IF NOT EXISTS csvt (
    idx SERIAL PRIMARY KEY, -- otherwise there would not be any primary key 
    int1 INT,
    string1 VARCHAR,
    int4 INT
);

-- Copy data from CSV into the table
COPY csvt (int1, string1, int4)
FROM '/csv_data/ints_string.csv'
DELIMITER ','
CSV HEADER;