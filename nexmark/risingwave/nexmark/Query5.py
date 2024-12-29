from nexmark import Query
from psycopg2._psycopg import connection

class Query5(Query):
   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            WITH hop_table AS (
                SELECT auction, COUNT(*) AS num
                FROM HOP(bid, date_time, INTERVAL '1 MINUTE', INTERVAL '60 MINUTES')
                GROUP BY auction
            ),
            max_count AS (
                SELECT MAX(num) AS max_num
                FROM hop_table
            ) 
            SELECT auction
            FROM hop_table, max_count
            WHERE num = max_num;
        """
        self.execute_sql(query)


   
    def query_subscriber(self):
        pass