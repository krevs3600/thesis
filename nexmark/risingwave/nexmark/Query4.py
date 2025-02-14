from nexmark import Query
from psycopg2._psycopg import connection

class Query4(Query):
   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
        self.execute_sql(self.auction_source_sql)
    
   
    def create_materialized_view(self):
        winning_bids = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS winning_bids AS
            SELECT 
                A.id AS auction_id,
                A.category AS category_id,
                MAX(B.price) AS final_price,
                MAX(B.idx) AS max_bid_idx
            FROM 
                auction A
            JOIN 
                bid B
            ON 
                A.id = B.auction
            WHERE 
                B.date_time BETWEEN A.date_time AND A.expires
            GROUP BY 
                A.id, A.category;
        """
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT 
                category_id,
                AVG(final_price) AS average_price,
                MAX(max_bid_idx) AS idx
            FROM 
                winning_bids
            GROUP BY 
                category_id;
        """

        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT
                Q.category_id,
                AVG(Q.final_price),
                MAX(max_bid_idx) AS idx 
            FROM (
                SELECT 
                    A.id as auction_id, 
                    A.category as category_id, 
                    MAX(B.price) AS final_price, 
                    MAX(B.idx) as max_bid_idx -- can'd directly do this but in flink there is no other way I guess, but is likely probable that new bids have larger price
                FROM auction A, bid B
                WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
                GROUP BY A.id, A.category
            ) Q
            GROUP BY Q.category_id;
        """
        self.execute_sql(winning_bids)
        self.execute_sql(query)


   
    def query_subscriber(self):
        pass