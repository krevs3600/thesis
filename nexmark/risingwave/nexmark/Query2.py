from nexmark import Query
from psycopg2._psycopg import connection

class Query2(Query):

   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
        
    
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT auction, price, idx
            FROM bid
            WHERE 
                MOD(auction, 123) = 0;
        """
        self.execute_sql(query)

    def query_subscriber(self):
        pass