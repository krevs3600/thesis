from nexmark import Query
from psycopg2._psycopg import connection

class Query2(Query):

   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
        
    
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT auction, price, date_time
            FROM bid
            WHERE 
                (auction = 1007 OR
                auction = 1020 OR
                auction = 2001 OR
                auction = 2019 OR
                auction = 2087);
        """
        self.execute_sql(query)

    def query_subscriber(self):
        pass