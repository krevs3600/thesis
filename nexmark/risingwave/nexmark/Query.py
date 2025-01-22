from abc import ABC, abstractmethod
from psycopg2._psycopg import connection


class Query(ABC):

    def __init__(self, conn : connection):
        self.conn = conn
        self.bid_source_sql = """
        CREATE SOURCE IF NOT EXISTS bid (
            idx BIGINT,
            auction BIGINT,
            bidder BIGINT,
            price DOUBLE,
            channel VARCHAR,
            url VARCHAR,
            date_time TIMESTAMP,
            extra VARCHAR,
            WATERMARK FOR date_time AS date_time - INTERVAL '1 SECOND'
        )
        WITH (
            connector='kafka',
            topic='bid-topic',
            properties.bootstrap.server='redpanda-0:9092',
            scan.startup.mode='earliest'
        ) FORMAT PLAIN ENCODE JSON;

        """
        self.person_source_sql = """
        CREATE SOURCE IF NOT EXISTS person (
            idx BIGINT,
            id BIGINT,
            name VARCHAR,
            email_address VARCHAR,
            credit_card VARCHAR,
            city VARCHAR,
            state VARCHAR,
            date_time TIMESTAMP,
            extra VARCHAR,
            WATERMARK FOR date_time AS date_time - INTERVAL '1 SECOND'
        )
        WITH (
            connector='kafka',
            topic='person-topic',
            properties.bootstrap.server='redpanda-0:9092',
            scan.startup.mode='earliest'
        ) FORMAT PLAIN ENCODE JSON;
        """
        self.auction_source_sql = """
        CREATE SOURCE IF NOT EXISTS auction (
            idx BIGINT,
            id BIGINT,
            item_name VARCHAR,
            description VARCHAR,
            initial_bid DOUBLE,
            reserve DOUBLE,
            date_time TIMESTAMP,
            expires TIMESTAMP,
            seller BIGINT,
            category INTEGER,
            extra VARCHAR,
            WATERMARK FOR date_time AS date_time - INTERVAL '1 SECOND'
        )
        WITH (
            connector='kafka',
            topic='auction-topic',
            properties.bootstrap.server='redpanda-0:9092',
            scan.startup.mode='earliest'
        ) FORMAT PLAIN ENCODE JSON;
        """

    def execute_sql(self, sqlStatement : str):
        with self.conn.cursor() as cur:
            cur.execute(sqlStatement)
        self.conn.commit()
    
    def retrieve_data(self, sqlStatement : str) -> list:
        with self.conn.cursor() as cur:
            cur.execute(sqlStatement)
            return cur.fetchall()


    @abstractmethod
    def create_sources(self):
        pass

    @abstractmethod
    def create_materialized_view(self):
        pass

    
    def create_subscriber(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""CREATE SUBSCRIPTION subscription 
                                FROM query WITH (
                                    retention = '1 MINUTE'
                                );""")

            cur.execute(f"""DECLARE cursor SUBSCRIPTION CURSOR FOR subscription FULL;""")
        self.conn.commit()


    @abstractmethod
    def query_subscriber(self):
        pass

    
    def debug(self) -> list:
        return self.retrieve_data("select * from query;")
    
    def create_sink(self) -> None:
        self.execute_sql("""CREATE SINK IF NOT EXISTS kafka_sink 
                            AS SELECT * FROM query
                            WITH (
                                connector='kafka',
                                topic='risingwave-topic',
                                properties.bootstrap.server='redpanda-0:9092',
                                include.event.watermark = 'true'
                            ) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');"""
        )

    def drop_materialized_view(self):
        self.execute_sql("""DROP SINK IF EXISTS kafka_sink;""")
        self.execute_sql("""DROP MATERIALIZED VIEW IF EXISTS query;""")
        self.execute_sql("""DROP MATERIALIZED VIEW IF EXISTS winning_bids;""")
        
        
        