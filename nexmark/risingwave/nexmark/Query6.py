from nexmark import Query
from psycopg2._psycopg import connection

class Query6(Query):

   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
        self.execute_sql(self.auction_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT AVG(Q.final) AS avg_final_price, Q.seller, MAX(Q.max_date_time) as date_time
            FROM (
                SELECT MAX(b.price) AS final, a.seller, MAX(b.date_time) as max_date_time
                FROM auction a
                JOIN bid b ON a.id = b.auction
                WHERE b.date_time < a.expires
                AND a.expires < NOW()
                GROUP BY a.id, a.seller
            ) Q
            GROUP BY Q.seller;
        """
        self.execute_sql(query)


    def query_subscriber(self):
        pass