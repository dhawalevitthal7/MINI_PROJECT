import sys
import psycopg2
from config import settings


def test_connection():
    db_url = settings.CACHE_DB_URL
    if not db_url:
        print("Set CACHE_DB_URL in a repo-root `.env` file (see `.env.example`).")
        sys.exit(1)
    print("Testing connection using CACHE_DB_URL from environment (value not printed).")
    
    try:
        conn = psycopg2.connect(db_url)
        print("✅ Success! Connection established.")
        
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"Database version: {version[0]}")
        
        cur.close()
        conn.close()
        print("✅ Connection closed successfully.")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nPossible issues:")
        if "missing \"=\"" in str(e):
            print("- The connection string seems to have extra characters (like 'psql' at the start).")
        if "could not translate host name" in str(e):
            print("- DNS issue or no internet connection.")
        if "password authentication failed" in str(e):
            print("- Incorrect username or password.")

if __name__ == "__main__":
    test_connection()
