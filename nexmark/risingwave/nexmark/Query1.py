from nexmark import Query
from psycopg2._psycopg import connection

class Query1(Query):


    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
        
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT auction, price * 0.908 AS price_dol, bidder, idx
            FROM bid;
        """
        self.execute_sql(query)

   
    def query_subscriber(self):
        pass