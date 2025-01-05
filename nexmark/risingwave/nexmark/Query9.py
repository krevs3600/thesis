from nexmark import Query
from psycopg2._psycopg import connection

class Query9(Query):

   
    def create_sources(self):
        self.execute_sql(self.person_source_sql)
        self.execute_sql(self.auction_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT MAX(price) as max_price, MAX(idx) as idx, window_start, window_end
            FROM HOP(
                    bid,              
                    date_time,
                    INTERVAL '60 SECONDS',  
                    INTERVAL '120 SECONDS'  
                )
            GROUP BY window_start, window_end
            
        """
        self.execute_sql(query)

   
    def query_subscriber(self):
        pass