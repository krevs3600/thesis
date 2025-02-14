from nexmark import Query
from psycopg2._psycopg import connection

class Query3(Query):
   
    def create_sources(self):
        self.execute_sql(self.auction_source_sql)
        self.execute_sql(self.person_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT person.name, person.city, person.state, auction.id, person.idx
            FROM auction
            JOIN person ON auction.seller = person.id
            WHERE 
                (person.state = 'or' OR person.state = 'id' OR person.state = 'ca')
                AND auction.category = 10;
        """
        self.execute_sql(query)


   
    def query_subscriber(self):
        pass