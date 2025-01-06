import sqlite3

if __name__ == "__main__":
    
    with open("create_db.sql", "r") as f:
        query = f.read()
    with sqlite3.connect("results.db") as conn:
        conn.cursor().execute(query)
        conn.commit()
    