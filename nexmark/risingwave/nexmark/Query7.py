from nexmark import Query
from psycopg2._psycopg import connection

class Query7(Query):


   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT B.auction, B.price, B.bidder
            FROM (
                SELECT auction, price, bidder, MAX(price) AS max_price
                FROM HOP(
                    bid,              
                    date_time,
                    INTERVAL '60 SECONDS',  
                    INTERVAL '60 SECONDS'  
                )
                GROUP BY auction, price, bidder
            ) B
            WHERE B.price = B.max_price;
        """
        self.execute_sql(query)

   
    def query_subscriber(self):
        pass