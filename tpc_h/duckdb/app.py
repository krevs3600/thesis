import duckdb
import json
import requests
import time
import pandas as pd
from pathlib import Path
import os


INPUT_PATH = Path(os.environ.get("INPUT_PATH"))
OUTPUT_PATH = Path(os.environ.get("OUTPUT_PATH")) / "duckdb"
QUERIES_PATH = Path(os.environ.get("QUERIES_PATH"))

# make sure the output path exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

BENCHMARK = "TPC-H"
BACKEND = "DuckDB"
TEST = "tcph_1_gb"

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
        print(conn.fetchone())
        # Load data from the corresponding CSV file into the table.
        # Data is loaded if it was never done before and this is checked through content of region table.
        if not conn.execute("select * from REGION").fetchone():
            csv_path = str(INPUT_PATH) + '/' + table['file']
            conn.execute(f"COPY {table_name} FROM '{csv_path}'")

    # Example query to verify data
    result = conn.execute('SELECT * FROM PART LIMIT 5').fetchall()
    print(result)
    
    # Save changes and close the connection
    conn.commit()
    return conn

def send_execution_time(query_id, run_id, execution_time):
    url = "http://127.0.0.1:5000/execution_time"
    payload = {
        "benchmark": BENCHMARK,
        "backend": BACKEND,
        "test": TEST,
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

def main(full_test = True, query_idx = None):
    conn = create_database()   
    
    if (full_test):
        
        for id, query in enumerate(os.listdir(QUERIES_PATH)):
            with open(str(QUERIES_PATH) + "/" + query, "r") as f:
                query = f.read()
            start = time.time()
            conn.execute(query)
            df = conn.fetch_df()
            print(df)
            execution_time = time.time() - start
            send_execution_time(id+1, 1, execution_time)

    elif query_idx <= 22 and query_idx >= 1:
        with open(str(QUERIES_PATH) + f"/query{query_idx}.sql", "r") as f:
            query = f.read()
        start = time.time()
        conn.execute(query)
        df = conn.fetch_df()
        print(df)
        execution_time = time.time() - start
        print(execution_time)
        

if __name__ == "__main__":
   main(True)
   # todo: execute from command line with parameters
