from psycopg2 import connect
import json
import pandas as pd
import time
import requests
from pathlib import Path
import os

INPUT_PATH = Path(os.environ.get("INPUT_PATH"))
OUTPUT_PATH = Path(os.environ.get("OUTPUT_PATH")) / "duckdb"
QUERIES_PATH = Path(os.environ.get("QUERIES_PATH"))

# make sure the output path exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

BENCHMARK = "TPC-H"
BACKEND = "risingwave"
TEST = "tcph_1_gb"


def build_db_table_query(table) -> str:
    table_name = table['name']
    primary_key = table['primary_key']
    
    # Create table SQL statement
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"

    # Iterate through columns to build the CREATE TABLE statement
    for column in table['columns']:
        column_name = column['name']
        if "VARCHAR" not in column['data_type'] and "CHAR" in column['data_type']:
            data_type = column['data_type'].replace("CHAR", "VARCHAR")
        else:
            data_type = column['data_type']
        create_table_sql += f"    {column_name} {data_type},\n"
    
    # Add primary key constraint
    create_table_sql += f"    PRIMARY KEY ({primary_key})\n"
    create_table_sql += ")"

    return create_table_sql

def postgres_connect():
    # connecting to psql
    conn_psql = connect(
            host="localhost",
            port="5432",
            user="user",
            dbname="BusinessDB",
            password="password"
        )
    return conn_psql

def connect_rw():
    rw_conn = connect(
        host="localhost",
        port="4566",
        user="root",
        dbname="dev"
    )
    return rw_conn

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

def app():
    # loading the data
    with open(str(INPUT_PATH) + '/database_schema.json') as f:
        schema = json.load(f)
    # load database to postgres
    conn = postgres_connect()
    cur = conn.cursor()
    for table in schema['database']['tables']:
        cur.execute(build_db_table_query(table))
    conn.commit()

    for table in schema['database']['tables']:
        table_name = table['name']
        csv_path = "/csv_data" + '/' + table['file']
        cur.execute("select * from REGION")
        if not cur.fetchone(): # should be useless
            cur.execute(f"COPY {table_name} FROM '{csv_path}' WITH (FORMAT csv)")
    conn.commit()
    conn.close()

    # hopefully data should be loaded know
    conn = connect_rw()
    cur = conn.cursor()

    # need to create sources and tables for riwisngwave
    db = "BusinessDB"
    for table in schema['database']['tables']:

        table_name = table['name']
        cur.execute(f"""CREATE SOURCE IF NOT EXISTS {table_name + "_source"}
                        WITH (
                            connector = 'postgres-cdc',
                            hostname = 'postgres_db_tpc',
                            port = '5432',
                            database.name = '{db}',
                            username = 'user',
                            password = 'password',
                            table.name = '{table_name}'
                        )
                        FORMAT PLAIN
                        ENCODE JSON;
                        """)
        # Create table SQL statement
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    
        # Iterate through columns to build the CREATE TABLE statement
        for column in table['columns']:
            column_name = column['name']
            if "CHAR" in column['data_type']:
                data_type = "TEXT"
            else:
                data_type = column['data_type']
            create_table_sql += f"    {column_name} {data_type},\n"
        
        # Add primary key constraint
        primary_key = table['primary_key']
        create_table_sql += f"    PRIMARY KEY ({primary_key})\n"
        create_table_sql += ")"
        create_table_sql += f"""FROM {table_name.lower() + "_source"} TABLE 'public.{table_name.lower()}'"""   
        #print(create_table_sql)
        cur.execute(create_table_sql)
    conn.commit()

    print("Loading data")
    while (cur.execute("select count(*) from lineitem") or cur.fetchone()[0] < 6e6):
        time.sleep(5)
    
    for id, query in enumerate(os.listdir(QUERIES_PATH)):
        with open(str(QUERIES_PATH) + "/" + query, "r") as f:
            query = f.read()
        try:
            start = time.time()
            cur.execute(query)
            # Fetch result into Pandas DataFrame
            result = pd.DataFrame(cur.fetchall())
            duration = time.time() - start
            send_execution_time(id+1, 1, duration)
            print(result)
            
        except Exception as e:
            print(f"Query failed: {query} Error: {e}")
    conn.close()
    
if __name__ == "__main__":
    app()