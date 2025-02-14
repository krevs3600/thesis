from nexmark import Query
from psycopg2._psycopg import connection

class Query6(Query):

   
    def create_sources(self):
        self.execute_sql(self.bid_source_sql)
        self.execute_sql(self.auction_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT AVG(Q.final) AS avg_final_price, Q.seller, MAX(Q.max_idx) as idx
            FROM (
                SELECT MAX(b.price) AS final, a.seller, MAX(b.idx) as max_idx
                FROM auction a
                JOIN bid b ON a.id = b.auction
                WHERE b.date_time < a.expires
                AND a.expires < NOW()
                GROUP BY a.id, a.seller
            ) Q
            GROUP BY Q.seller;
        """

        query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
        WITH ranked_bids AS (
            SELECT
                a.seller,
                b.price,
                b.idx,
                b.date_time,
                ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum
            FROM
                auction a
            JOIN
                bid b
            ON
                a.id = b.auction
            WHERE
                b.date_time BETWEEN a.date_time AND a.expires
        ),
        filtered_bids AS (
            SELECT
                seller,
                price,
                idx,
                date_time
            FROM
                ranked_bids
            WHERE
                rownum <= 1
        )
        SELECT
            seller,
            AVG(price) OVER (
                PARTITION BY seller
                ORDER BY date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS avg_price,
            MAX(idx) OVER (
                PARTITION BY seller
                ORDER BY date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS idx
        FROM
            filtered_bids;
        """
        self.execute_sql(query)


    def query_subscriber(self):
        pass