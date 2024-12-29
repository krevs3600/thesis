from nexmark import Query
from psycopg2._psycopg import connection

class Query4(Query):

    def __init__(self, conn : connection):
        super().__init__(conn)

    def execute_sql(self, sqlStatement : str):
        super().__init__(sqlStatement)

   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
        self.execute_sql(self.auction_source_sql)
        pass
    
   
    def create_materialized_view(self):
        winning_bids = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS winning_bids AS
            SELECT 
                A.id AS auction_id,
                A.category AS category_id,
                MAX(B.price) AS final_price
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
                AVG(final_price) AS average_price
            FROM 
                winning_bids
            GROUP BY 
                category_id;
        """
        self.execute_sql(winning_bids)
        self.execute_sql(query)


   
    def query_subscriber(self):
        pass