import sqlite3
import os

class SQLiteHandler:
    def __init__(self, db_path):
        self.db_path = db_path

        # Create DB file if missing
        if not os.path.exists(db_path):
            print(f"[INFO] Database not found. Creating: {db_path}")
            open(db_path, "w").close()

        # Open connection
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        print("[INFO] Database connected.")

    def execute(self, query, params=None):
        """Execute a write query (INSERT, UPDATE, DELETE)"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.conn.commit()
            return True
        except Exception as e:
            print("[ERROR] Query failed:", e)
            return False

    def fetch(self, query, params=None):
        """Execute a read query (SELECT)"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print("[ERROR] Fetch failed:", e)
            return None

    def close(self):
        self.conn.close()
        print("[INFO] Database closed.")
    
    def initDatabase(self):
        cursor = self.conn.cursor()
        # Open and execute the SQL file
        with open("schema.sql", "r") as sql_file:
            sql_script = sql_file.read()

        cursor.executescript(sql_script)  # executes multiple SQL statements at once

        # Commit changes and close
        self.conn.commit()

if __name__ == "__main__":
    Handler = SQLiteHandler("mobile_shop_dw.db")
    while True:
        query = input("\nSQL> ").strip()

        if query.lower() == "exit":
            break

        try:
            if query.lower() == 'init':
                Handler.initDatabase()
            elif query.lower().startswith('pragma'):
                result = Handler.fetch(query)
                print(result)
            elif query.lower().startswith("select"):
                rows = Handler.fetch(query)
                print("\nResult:")
                for row in rows:
                    print(row)
            else:
                Handler.execute(query)
                print("Query executed successfully.")

        except Exception as e:
            print(f"Error: {e}")

    Handler.close()
    print("Database closed.")