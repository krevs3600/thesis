from nexmark import Query
from psycopg2._psycopg import connection

class Query8(Query):

   
    def create_sources(self):
        self.execute_sql(self.person_source_sql)
        self.execute_sql(self.auction_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT P.id, P.name, A.reserve, P.date_time
            FROM person P, auction A
            WHERE P.id = A.seller
            AND P.date_time >= NOW() - INTERVAL '12 HOURS'
            AND A.date_time >= NOW() - INTERVAL '12 HOURS';
        """
        self.execute_sql(query)

   
    def query_subscriber(self):
        pass