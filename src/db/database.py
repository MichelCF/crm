import sqlite3
from datetime import datetime
from typing import Optional
from src.config import Config
from src.models.schemas import Customer, Product, Sale

# SQL Templates Gold Layer
SQL_CREATE_AUDIENCE_ILPI = """
    CREATE TABLE IF NOT EXISTS audience_ilpi (
        name TEXT,
        email TEXT PRIMARY KEY,
        phone TEXT,
        country TEXT,
        state TEXT,
        value REAL,
        updated_at TIMESTAMP
    )
"""

SQL_CREATE_AUDIENCE_ESTETICA = """
    CREATE TABLE IF NOT EXISTS audience_estetica (
        name TEXT,
        email TEXT PRIMARY KEY,
        phone TEXT,
        country TEXT,
        state TEXT,
        value REAL,
        updated_at TIMESTAMP
    )
"""

SQL_UPSERT_AUDIENCE = """
    INSERT INTO {} (name, email, phone, country, state, value, updated_at)
    VALUES (:name, :email, :phone, :country, :state, :value, :updated_at)
    ON CONFLICT(email) DO UPDATE SET
        name = excluded.name,
        phone = excluded.phone,
        country = excluded.country,
        state = excluded.state,
        value = excluded.value,
        updated_at = excluded.updated_at
"""

SQL_UPSERT_SALE = """
    INSERT INTO sales (
        transaction_id, status, total_price, currency, payment_method,
        payment_type, installments, approved_date, order_date,
        purchased_at, updated_at, customer_id, product_id, imported_at
    ) VALUES (
        :transaction, :status, :total_price, :currency, :payment_method,
        :payment_type, :installments, :approved_date, :order_date,
        :purchased_at, :updated_at, :customer_id, :product_id, :imported_at
    )
"""


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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hotmart_customers (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT,
            email TEXT NOT NULL,
            name TEXT NOT NULL,
            phone TEXT,
            document TEXT,
            zip_code TEXT,
            address TEXT,
            number TEXT,
            neighborhood TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            master_email TEXT UNIQUE,
            master_phone TEXT UNIQUE,
            name TEXT,
            instagram TEXT,
            document TEXT,
            hotmart_id TEXT UNIQUE,
            manychat_id INTEGER UNIQUE,
            source TEXT DEFAULT 'HOTMART',
            has_purchased BOOLEAN DEFAULT 0,
            segment TEXT,
            updated_at TIMESTAMP
        )
    """)

    # Migrações para colunas novas caso a tabela já exista
    try:
        cur.execute("ALTER TABLE customers ADD COLUMN source TEXT DEFAULT 'HOTMART'")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE customers ADD COLUMN has_purchased BOOLEAN DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE customers ADD COLUMN segment TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE customers ADD COLUMN updated_at TIMESTAMP")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute(
            "ALTER TABLE sales ADD COLUMN imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
    except sqlite3.OperationalError:
        pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            transaction_id TEXT,
            status TEXT NOT NULL,
            total_price REAL NOT NULL,
            currency TEXT NOT NULL,
            payment_method TEXT,
            payment_type TEXT,
            installments INTEGER,
            approved_date INTEGER,
            order_date INTEGER,
            purchased_at TIMESTAMP,
            updated_at TIMESTAMP,
            customer_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hotmart_sales_products (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_transaction_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            commission REAL NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hotmart_sales_commissions (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_transaction_id TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            value REAL NOT NULL,
            currency TEXT NOT NULL,
            processed_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hotmart_sales_history (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_transaction_id TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            changed_at TIMESTAMP NOT NULL
        )
    """)

    # Gold Layer: Audiences
    cur.execute(SQL_CREATE_AUDIENCE_ILPI)
    cur.execute(SQL_CREATE_AUDIENCE_ESTETICA)

    conn.commit()


def upsert_customer(conn: sqlite3.Connection, customer: Customer):
    """Inserts a customer record as a Raw log (Append)."""
    conn.execute(
        """
        INSERT INTO hotmart_customers (
            id, email, name, phone, document, 
            zip_code, address, number, neighborhood, city, state, country, 
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            customer.id,
            customer.email,
            customer.name,
            customer.phone,
            customer.document,
            customer.zip_code,
            customer.address,
            customer.number,
            customer.neighborhood,
            customer.city,
            customer.state,
            customer.country,
            customer.created_at.isoformat(),
            customer.updated_at.isoformat() if customer.updated_at else None,
        ),
    )


def upsert_product(conn: sqlite3.Connection, product: Product):
    """Inserts or updates a product record."""
    conn.execute(
        """
        INSERT INTO products (id, name)
        VALUES (?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name
    """,
        (product.id, product.name),
    )


def upsert_sale(conn: sqlite3.Connection, sale: Sale, imported_at: str = None):
    """Upserts a sale record using its transaction ID. Merges with existing records."""
    cur = conn.cursor()

    if imported_at is None:
        imported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Prepare data dict for named parameters mapping transaction -> transaction_id
    data = {
        **sale.model_dump(),
        "transaction": sale.transaction,
        "imported_at": imported_at,
    }

    # SQLite named parameters don't handle datetime well in :memory: tests sometimes
    # if not properly converted, but schemas.py uses datetime objects.
    # Pydantic's model_dump() with mode='json' or manual string conversion handles it.
    for k, v in data.items():
        if isinstance(v, datetime):
            data[k] = v.isoformat()

    cur.execute(SQL_UPSERT_SALE, data)
    conn.commit()


def upsert_audience_member(conn: sqlite3.Connection, table_name: str, data: dict):
    """
    Upserts a member into a specific audience table (Gold layer).
    """
    cur = conn.cursor()
    # Sanitize table name (should only be internal constants)
    if table_name not in ("audience_ilpi", "audience_estetica"):
        raise ValueError(f"Invalid audience table name: {table_name}")

    cur.execute(SQL_UPSERT_AUDIENCE.format(table_name), data)
    conn.commit()


def get_max_sale_date(conn: sqlite3.Connection) -> Optional[str]:
    """Retrieves the latest purchased_at date from the sales table."""
    row = conn.execute("SELECT MAX(purchased_at) as max_date FROM sales").fetchone()
    if row and row["max_date"]:
        return row["max_date"]
    return None


def upsert_master_customer(
    conn: sqlite3.Connection,
    source: str,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    name: Optional[str] = None,
    instagram: Optional[str] = None,
    hotmart_id: Optional[str] = None,
    manychat_id: Optional[int] = None,
    has_purchased: bool = False,
    segment: Optional[str] = None,
):
    """
    Consolidates data into the 'customers' table following business rules:
    - Hotmart is the Source of Truth.
    - ManyChat data only flows to Master if phone is present.
    - If user exists as HOTMART, ManyChat only updates missing fields (like Instagram).
    """
    cur = conn.cursor()
    from datetime import datetime

    now = datetime.now().isoformat()

    # 1. Tentar localizar usuário existente
    existing = None
    if email:
        cur.execute(
            "SELECT * FROM customers WHERE master_email = ?", (email.lower().strip(),)
        )
        existing = cur.fetchone()

    if not existing and phone:
        cur.execute("SELECT * FROM customers WHERE master_phone = ?", (phone.strip(),))
        existing = cur.fetchone()

    if not existing and hotmart_id:
        cur.execute("SELECT * FROM customers WHERE hotmart_id = ?", (hotmart_id,))
        existing = cur.fetchone()

    # 2. Lógica de Upsert
    if existing:
        user_id = existing["id"]
        current_source = existing["source"]

        if source == "HOTMART":
            # Hotmart sobrescreve quase tudo
            cur.execute(
                """
                UPDATE customers SET
                    master_email = COALESCE(?, master_email),
                    master_phone = COALESCE(?, master_phone),
                    name = COALESCE(?, name),
                    hotmart_id = COALESCE(?, hotmart_id),
                    source = 'HOTMART',
                    has_purchased = ?,
                    segment = ?,
                    updated_at = ?
                WHERE id = ?
            """,
                (
                    email,
                    phone,
                    name,
                    hotmart_id,
                    1 if has_purchased else existing["has_purchased"],
                    segment or existing["segment"],
                    now,
                    user_id,
                ),
            )
        else:
            # ManyChat só atualiza se a fonte atual não for Hotmart, ou preenche Instagram
            if current_source == "MANYCHAT":
                cur.execute(
                    """
                    UPDATE customers SET
                        master_email = COALESCE(master_email, ?),
                        master_phone = COALESCE(master_phone, ?),
                        name = COALESCE(name, ?),
                        instagram = COALESCE(instagram, ?),
                        manychat_id = COALESCE(manychat_id, ?),
                        updated_at = ?
                    WHERE id = ?
                """,
                    (email, phone, name, instagram, manychat_id, now, user_id),
                )
            else:
                # Fonte é HOTMART, só permitimos atualizar Instagram e ManyChat ID
                cur.execute(
                    """
                    UPDATE customers SET
                        instagram = COALESCE(instagram, ?),
                        manychat_id = COALESCE(manychat_id, ?),
                        updated_at = ?
                    WHERE id = ?
                """,
                    (instagram, manychat_id, now, user_id),
                )
    else:
        # 3. Criar novo Registro (Follow ManyChat Phone-only rule)
        if source == "MANYCHAT" and not phone:
            return  # Ignora ManyChat sem telefone no Master

        cur.execute(
            """
            INSERT INTO customers (
                master_email, master_phone, name, instagram, hotmart_id, 
                manychat_id, source, has_purchased, segment, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                email,
                phone,
                name,
                instagram,
                hotmart_id,
                manychat_id,
                source,
                1 if has_purchased else 0,
                segment,
                now,
            ),
        )


def consolidate_all_to_master(conn: sqlite3.Connection):
    """
    Complete consolidation:
    1. Rebuild Master from Hotmart (Source of Truth).
    2. Supplement with ManyChat (Only if has Phone).
    """
    from src.logic.user_logic import get_segment_for_products

    cur = conn.cursor()

    # 1. Processar Hotmart (Prioridade)
    # Pegamos a versão mais RECENTE de cada cliente no Raw
    cur.execute("""
        WITH LatestHotmart AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY imported_at DESC) as rn
            FROM hotmart_customers
        )
        SELECT 
            c.id as hotmart_id,
            c.email,
            c.name,
            c.phone,
            c.document,
            (SELECT GROUP_CONCAT(DISTINCT s.product_id) FROM sales s WHERE s.customer_id = c.id) as product_ids,
            (SELECT MAX(CASE WHEN s.status IN ('APPROVED', 'COMPLETE') THEN 1 ELSE 0 END) FROM sales s WHERE s.customer_id = c.id) as bought
        FROM LatestHotmart c
        WHERE c.rn = 1
    """)
    hotmart_users = cur.fetchall()

    for row in hotmart_users:
        p_ids = row["product_ids"].split(",") if row["product_ids"] else []
        segment = get_segment_for_products(p_ids)

        upsert_master_customer(
            conn,
            source="HOTMART",
            email=row["email"],
            phone=row["phone"],
            name=row["name"],
            hotmart_id=row["hotmart_id"],
            has_purchased=bool(row["bought"]),
            segment=segment,
        )

    # 2. Processar ManyChat (Suplemento)
    cur.execute(
        "SELECT * FROM manychat_contacts WHERE whatsapp IS NOT NULL AND whatsapp != ''"
    )
    manychat_users = cur.fetchall()

    for row in manychat_users:
        upsert_master_customer(
            conn,
            source="MANYCHAT",
            email=row["email"],
            phone=row["whatsapp"],
            name=row["nome"],
            instagram=row["instagram"],
            manychat_id=row["id"],
        )

    conn.commit()
