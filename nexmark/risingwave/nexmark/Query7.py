from nexmark import Query
from psycopg2._psycopg import connection

class Query7(Query):


   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT B.auction, B.price, B.bidder, max_date_time as date_time
            FROM (
                SELECT auction, price, bidder, MAX(price) AS max_price, MAX(date_time) AS max_date_time
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