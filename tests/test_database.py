import pytest
import sqlite3
from datetime import datetime
from src.db.database import init_db, get_connection, upsert_customer, upsert_product, upsert_sale
from src.models.schemas import Customer, Product, Sale

@pytest.fixture
def memory_db():
    db_path = ":memory:"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn
    conn.close()

def test_database_initialization(memory_db):
    cursor = memory_db.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row["name"] for row in cursor.fetchall()]
    assert "customers" in tables
    assert "hotmart_customers" in tables
    assert "manychat_contacts" in tables
    assert "products" in tables
    assert "sales" in tables

def test_upsert_customer(memory_db):
    now = datetime.now()
    cust = Customer(id="USER123", email="test@example.com", name="Test User", phone="123", created_at=now, updated_at=now)
    upsert_customer(memory_db, cust)
    memory_db.commit()
    
    # Verify insert
    row = memory_db.execute("SELECT * FROM hotmart_customers WHERE id = ?", (cust.id,)).fetchone()
    assert row["name"] == "Test User"
    assert row["phone"] == "123"
    assert row["email"] == "test@example.com"
    
    # Verify update
    cust.name = "Updated User"
    upsert_customer(memory_db, cust)
    memory_db.commit()
    
    row = memory_db.execute("SELECT * FROM hotmart_customers WHERE id = ?", (cust.id,)).fetchone()
    assert row["name"] == "Updated User"

def test_upsert_sale_with_foreign_keys(memory_db):
    now = datetime.now()
    # Insert dependencies first due to PRAGMA foreign_keys = ON
    cust = Customer(id="BUYER1", email="buyer@example.com", name="Buyer", created_at=now)
    prod = Product(id=1, name="My Product")
    
    upsert_customer(memory_db, cust)
    upsert_product(memory_db, prod)
    
    sale = Sale(
        transaction_id="TX001",
        status="APPROVED",
        total_price=100.0,
        net_price=90.0,
        payment_method="CREDIT_CARD",
        purchased_at=now,
        updated_at=now,
        customer_id="BUYER1",
        product_id=1
    )
    
    upsert_sale(memory_db, sale)
    memory_db.commit()
    
    row = memory_db.execute("SELECT * FROM sales WHERE transaction_id = 'TX001'").fetchone()
    assert row["status"] == "APPROVED"
    assert row["total_price"] == 100.0
    assert row["net_price"] == 90.0
    assert row["payment_method"] == "CREDIT_CARD"
    assert row["updated_at"] is not None
