import psycopg2
import pandas as pd
import time


class Risingwave:
    def __init__(self):
        self.conn = None

    def connect(self, host="localhost", port="4566", user="root", password="", dbname="dev") -> None:
        """
        Connect to the RisingWave instance.
        """
        try:
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname
            )
            print("Connected to RisingWave.")
        except Exception as e:
            print(f"Error connecting to RisingWave: {e}")
            raise

    def create_ps_source(self) -> None:
        """
        Create a source in RisingWave that connects to PostgreSQL data.
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""
                CREATE SOURCE IF NOT EXISTS csvt_source
                WITH (
                    connector = 'postgres-cdc',
                    hostname = 'postgres',
                    port = '5432',
                    database.name = 'testdb',
                    username = 'user',
                    password = 'password',
                    table.name = 'csvt'
                )
                FORMAT PLAIN
                ENCODE JSON;
            """)
            print("RisingWave source created!")
            self.conn.commit()
            cur.close()
        except Exception as e:
            print(f"Error creating source: {e}")
            raise

    def create_ps_table(self) -> None:
        """
        Create a RisingWave table from the source.
        """
        try:
            cur = self.conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS csvt_table (
                        idx integer primary key,
                        int1 integer,
                        string1 varchar,
                        int4 integer
                    )
                FROM csvt_source TABLE 'public.csvt';
            """)
            print("RisingWave table created!")
            self.conn.commit()
            
            # continously fetching the db to make sure source data is loaded
            count = 0
            while not count:
                cur.execute("SELECT count(*) from csvt_table")
                count = int(cur.fetchone()[0])
            cur.close()
            


        except Exception as e:
            print(f"Error creating table: {e}")
            raise

    def run_and_save_csv(self, query: str, columns: list, path: str) -> None:
        """
        Run a query on the RisingWave instance and save the result to a CSV file.
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            # Fetch result into Pandas DataFrame
            result = pd.DataFrame(cur.fetchall(), columns=columns)
            cur.close()
            print("Data processing completed.")
            # Save result to CSV
            result.to_csv(path, index=False)
            print(f"Result saved to {path}.")
        except Exception as e:
            print(f"Error running query or saving CSV: {e}")
            raise

    def close_connection(self) -> None:
        """
        Close the connection to the RisingWave instance.
        """
        if self.conn:
            self.conn.close()
            print("Connection to RisingWave closed.")