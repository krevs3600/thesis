from nexmark import Query
from psycopg2._psycopg import connection

class Query3(Query):
   
    def create_sources(self):
        self.execute_sql(self.auction_source_sql)
        self.execute_sql(self.person_source_sql)
    
   
    def create_materialized_view(self):
        query = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS query AS
            SELECT person.name, person.city, person.state, auction.id
            FROM auction, person
            WHERE 
                auction.seller = person.id
                AND (person.state = 'OR' OR person.state = 'ID' OR person.state = 'CA')
                AND auction.category = 10;
        """
        self.execute_sql(query)


   
    def query_subscriber(self):
        pass