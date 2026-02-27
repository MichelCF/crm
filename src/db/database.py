import sqlite3
from typing import List, Optional
from src.config import Config
from src.models.schemas import Customer, Product, Sale

def get_connection(db_path: str = Config.DB_NAME) -> sqlite3.Connection:
    """Returns a connection to the SQLite database defined by the config."""
    conn = sqlite3.connect(db_path)
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON;")
    # Return rows as dictionaries
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: sqlite3.Connection):
    """Initializes the SQLite database with the required tables."""
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS hotmart_customers (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            phone TEXT,
            document TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS manychat_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT,
            email TEXT,
            instagram TEXT,
            whatsapp TEXT,
            data_remarketing TEXT,
            agendamento TEXT,
            data_agendamento TEXT,
            contactar TEXT,
            data_contactar TEXT,
            ultima_interacao TEXT,
            data_registro TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            master_email TEXT UNIQUE,
            master_phone TEXT UNIQUE,
            name TEXT,
            instagram TEXT,
            document TEXT,
            hotmart_id TEXT UNIQUE,
            manychat_id INTEGER UNIQUE,
            FOREIGN KEY (hotmart_id) REFERENCES hotmart_customers (id),
            FOREIGN KEY (manychat_id) REFERENCES manychat_contacts (id)
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            transaction_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            total_price REAL NOT NULL,
            net_price REAL NOT NULL,
            currency TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            purchased_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP,
            customer_id TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES hotmart_customers (id),
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS hotmart_sales_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_transaction_id TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            commission REAL NOT NULL,
            FOREIGN KEY (sale_transaction_id) REFERENCES sales (transaction_id),
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS hotmart_sales_commissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_transaction_id TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            value REAL NOT NULL,
            currency TEXT NOT NULL,
            processed_at TIMESTAMP,
            FOREIGN KEY (sale_transaction_id) REFERENCES sales (transaction_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS hotmart_sales_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_transaction_id TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            changed_at TIMESTAMP NOT NULL,
            FOREIGN KEY (sale_transaction_id) REFERENCES sales (transaction_id)
        )
    ''')
    
    conn.commit()

def upsert_customer(conn: sqlite3.Connection, customer: Customer):
    """Inserts or updates a customer record using id as PK."""
    conn.execute('''
        INSERT INTO hotmart_customers (id, email, name, phone, document, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            email=excluded.email,
            name=excluded.name,
            phone=excluded.phone,
            document=excluded.document,
            updated_at=excluded.updated_at
    ''', (
        customer.id, customer.email, customer.name, customer.phone, customer.document, 
        customer.created_at.isoformat(), customer.updated_at.isoformat() if customer.updated_at else None
    ))

def upsert_product(conn: sqlite3.Connection, product: Product):
    """Inserts or updates a product record."""
    conn.execute('''
        INSERT INTO products (id, name)
        VALUES (?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name
    ''', (product.id, product.name))

def upsert_sale(conn: sqlite3.Connection, sale: Sale):
    """Inserts or updates a sale record."""
    conn.execute('''
        INSERT INTO sales (
            transaction_id, status, total_price, net_price, currency, payment_method, purchased_at, updated_at, customer_id, product_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(transaction_id) DO UPDATE SET
            status=excluded.status,
            total_price=excluded.total_price,
            net_price=excluded.net_price,
            currency=excluded.currency,
            payment_method=excluded.payment_method,
            purchased_at=excluded.purchased_at,
            updated_at=excluded.updated_at,
            customer_id=excluded.customer_id,
            product_id=excluded.product_id
    ''', (
        sale.transaction_id, sale.status, sale.total_price, sale.net_price, sale.currency, sale.payment_method, 
        sale.purchased_at.isoformat(), sale.updated_at.isoformat() if sale.updated_at else None, sale.customer_id, sale.product_id
    ))

def get_max_sale_date(conn: sqlite3.Connection) -> Optional[str]:
    """Retrieves the latest purchased_at date from the sales table."""
    row = conn.execute("SELECT MAX(purchased_at) as max_date FROM sales").fetchone()
    if row and row["max_date"]:
        return row["max_date"]  # returns isoformat string
    return None
