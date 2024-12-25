from Risingwave import Risingwave
import os
from pathlib import Path
# data structure containing query and output columns
queries = [
    ("SELECT int1, string1, int4 FROM csvt_table WHERE int4 is NOT NULL",  ['int1', 'string1', 'int4']),# filter nan
    ("SELECT int1, string1, COALESCE(int4, 0) AS int4 FROM csvt_table", ['int1', 'string1', 'int4']), # fill nan
    ("SELECT int1, string1, int4 FROM csvt_table WHERE int4 is NOT NULL and int4 % 2 = 0", ['int1', 'string1', 'int4']), # filter with computation
    ("SELECT string1, sum(int4) as sum FROM csvt_table WHERE string1 LIKE '%a%' GROUP BY string1", ['string1', 'sum']), # filter and sum 
    ("SELECT string1, avg(int4) as mean FROM csvt_table GROUP BY string1", ['string1', 'mean'] )# avg
]


def main():
    rw_app = Risingwave()
    rw_app.connect()
    rw_app.create_ps_source()
    rw_app.create_ps_table()

    OUTPUT_PATH = os.environ.get("OUTPUT_PATH")
    if not OUTPUT_PATH:
        print("Couldn't read OUTPUT_PATH from ENV variables.")
        exit(1) 

    OUTPUT_PATH = Path(OUTPUT_PATH) / "risingwave"
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


    for i, query in enumerate(queries):
        
        rw_app.run_and_save_csv(query[0], query[1], str(OUTPUT_PATH / f"query_{i}.csv"))
    
    rw_app.close_connection()

if __name__ == "__main__":
    main()