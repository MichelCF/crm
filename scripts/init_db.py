import os
import sys

# Garante que o diretório raiz está no path para importar src
sys.path.append(os.getcwd())

from src.db.database import init_db, get_connection
from src.config import Config


def main():
    print("--- DATABASE INITIALIZATION ---")
    print(f"Environment: {Config.ENVIRONMENT.upper()}")
    print(f"Database Path: {Config.DB_NAME}")

    # Garante que as pastas pai existem
    db_dir = os.path.dirname(Config.DB_NAME)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        print(f"Created directory: {db_dir}")

    try:
        conn = get_connection()
        print("Connected to database. Initializing tables...")
        init_db(conn)
        conn.close()
        print("Database initialization successful!")
    except Exception as e:
        print(f"Error initializing database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
