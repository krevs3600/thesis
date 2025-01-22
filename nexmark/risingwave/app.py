from psycopg2 import connect
from psycopg2._psycopg import connection
import time
from pandas import DataFrame
import asyncio
import asyncpg
from nexmark import *
import argparse



def create_db():
    conn = connect(
        host="localhost",      # Assuming RisingWave is mapped to localhost
        port="4566",           # Adjust if RisingWave is mapped to a different port
        user="root", 
        dbname="dev"    # Default database to connect first
    )

    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS streaming;")
    conn.commit()

def get_conn() -> connection:
    conn = connect(
        host="localhost",  
        port=4566,         
        user="root",
        dbname="streaming"
    )
    return conn




def bench(query=1,debug=False):

    query_classes = {
        1: Query1,
        2: Query2,
        3: Query3,
        4: Query4,
        5: Query5,
        6: Query6,
        7: Query7,
        8: Query8,
        9: Query9
    }

    create_db()
    conn = get_conn()
    query : Query = query_classes[query](conn)
    query.create_sources()
    query.drop_materialized_view()
    query.create_materialized_view()
    query.create_sink()
    #query.create_subscriber()
    if debug:
        for i in range(5):
            data = query.debug()
            print(data)
            time.sleep(10)
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a specific query benchmark.")
    parser.add_argument(
        "query_number",
        type=int,
        help="The query number to execute (1-9)."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode to print debug information."
    )

    # Parse arguments
    args = parser.parse_args()

    # Run the bench function with the provided arguments
    bench(query=args.query_number, debug=args.debug)
