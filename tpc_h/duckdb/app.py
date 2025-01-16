import duckdb
import json
import requests
import time
import pandas as pd
from pathlib import Path
import os
import argparse


INPUT_PATH = Path(os.environ.get("INPUT_PATH"))
OUTPUT_PATH = Path(os.environ.get("OUTPUT_PATH")) / "duckdb"
QUERIES_PATH = Path(os.environ.get("QUERIES_PATH"))

# make sure the output path exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

BENCHMARK = "TPC-H"
BACKEND = "duckdb"

pd.set_option('display.max_rows', None)    # Show all rows
pd.set_option('display.max_columns', None) # Show all columns
pd.set_option('display.expand_frame_repr', False)  # Prevent wrapping of columns


def create_database():
    with open(INPUT_PATH / 'database_schema.json') as f:
        schema = json.load(f)
    # local connection to duckdb file
    conn = duckdb.connect(str(INPUT_PATH) + '/' + schema["database"]["name"] + '.duckdb')

    for table in schema['database']['tables']:
        table_name = table['name']
        primary_key = table['primary_key']
        
        # Create table SQL statement
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        for column in table['columns']:
            column_name = column['name']
            data_type = column['data_type']
            create_table_sql += f"    {column_name} {data_type},\n"
        create_table_sql += f"    PRIMARY KEY ({primary_key})\n"
        create_table_sql += ")"
        
        conn.execute(create_table_sql)
        # Load data from the corresponding CSV file into the table.
        # Data is loaded if it was never done before and this is checked through content of region table.
        if not conn.execute("select * from REGION").fetchone():
            csv_path = str(INPUT_PATH) + '/' + table['file']
            conn.execute(f"COPY {table_name} FROM '{csv_path}'")

    # Example query to verify data
    result = conn.execute('SELECT * FROM PART LIMIT 5').fetchall()
    # Save changes and close the connection
    conn.commit()
    return conn

def send_execution_time(query_id, run_id, execution_time, test_name):
    url = "http://127.0.0.1:5000/execution_time"
    payload = {
        "benchmark": BENCHMARK,
        "backend": BACKEND,
        "test": test_name,
        "query_id": query_id,
        "run_id" : run_id,
        "execution_time": execution_time
    }
    response = requests.post(url=url, json=payload)

def measure_db_creation_time():
    start = time.time()
    create_database()
    creation_time = time.time() - start
    print(creation_time)

def main(query_idx = 0, iteration = 1, test_name = "tpch"):
    conn = create_database()   
    
    if (query_idx == 0):
        
        for id, query in enumerate(os.listdir(QUERIES_PATH)):
            with open(str(QUERIES_PATH) + "/" + query, "r") as f:
                query = f.read()
            for i in range (1,iteration+1):
                start = time.time()
                conn.execute(query)
                df = conn.fetch_df()
                print(df)
                execution_time = time.time() - start
                send_execution_time(id+1, i, execution_time, test_name)

    else:
        with open(str(QUERIES_PATH) + f"/query{query_idx}.sql", "r") as f:
            query = f.read()
        start = time.time()
        conn.execute(query)
        df = conn.fetch_df()
        print(df)
        execution_time = time.time() - start
        print(execution_time)
        

def valid_query(value):
    if value.isdigit() and 1 <= int(value) <= 22:
        return int(value)
    elif value.lower() == "all":
        return 0
    else:
        raise argparse.ArgumentTypeError(
            "Query must be a number between 1 and 22 or 'all'."
        )



if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="A script to execute duckdb tpc_h test")
    # Add arguments
    parser.add_argument('query', type=valid_query, help="Specify the query to execute: a number between 1 and 22, or 'all' to execute all queries.")
    parser.add_argument('iteration', type=int, help="Number of time to execute each query")
    parser.add_argument('test_name', type=str, help="Name of the test")

    # Parse the arguments
    args = parser.parse_args()
    main(query_idx=args.query, iteration=args.iteration, test_name=args.test_name)

