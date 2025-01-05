from nexmark import Query
from psycopg2._psycopg import connection

class Query5(Query):
   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            WITH hop_table AS (
                SELECT 
                    auction, 
                    COUNT(*) AS num, 
                    MAX(idx) AS last_idx,
                    window_start,
                    window_end
                FROM HOP(bid, date_time, INTERVAL '10 SECONDS', INTERVAL '20 SECONDS')
                GROUP BY auction, window_start, window_end
            )
            SELECT 
                auction,
                last_idx AS idx,
                window_start,
                window_end
            FROM hop_table;"""

        self.execute_sql(query)


   
    def query_subscriber(self):
        pass