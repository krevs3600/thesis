import duckdb
import pandas as pd
from pathlib import Path
import os


# read paths from environment variables
INPUT_PATH = Path(os.environ.get("INPUT_PATH"))
OUTPUT_PATH = Path(os.environ.get("OUTPUT_PATH")) / "duckdb"

# make sure the output path exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# SQL queries
queries = [
    "SELECT * FROM data WHERE data.int4 is NOT NULL",  # filter nan
    "SELECT data.int1, data.string1, COALESCE(data.int4, 0) AS int4 FROM data",  # fill nan
    "SELECT * FROM data WHERE int4 is NOT NULL and int4 % 2 = 0", # filter with computation
    "SELECT string1, sum(int4) as sum FROM data WHERE data.string1 LIKE '%a%' GROUP BY string1",  # filter and sum 
    "SELECT string1, avg(int4) as mean FROM data GROUP BY string1"  # avg
]

def init_db(csv_path: str) -> duckdb.DuckDBPyConnection:
    """
    Create db in-memory connection and load data from the input file
    returning the connection
    """
    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{csv_path}')")
    conn.commit()
    return conn

def run_and_save_query(conn, query, output_path, debug=False):
    """
    Execute the given SQL query and save the result to the specified output path.
    If debug=True, print the result instead of saving it to a file.
    """
    result_df = conn.execute(query).fetchdf()
    if debug:
        print(result_df)
    else:
        result_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    # construct input CSV path
    input_csv = INPUT_PATH / "ints_string.csv"
    
    # check if the input file exists
    if not input_csv.is_file():
        print(f"Please check input file path: {input_csv}")
        exit(1)
    
    # initialize database
    conn = init_db(str(input_csv))
    
    # run queries and save results
    for i, query in enumerate(queries):
        save_to = OUTPUT_PATH / f"query_{i}.csv"
        run_and_save_query(conn, query, save_to, debug=False)
    
    # close connection
    conn.close()
