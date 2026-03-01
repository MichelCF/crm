import pytest
import sqlite3
from src.db.database import (
    init_db,
    upsert_customer,
    consolidate_all_to_master,
    upsert_sale,
)
from src.models.schemas import Customer, Sale
from datetime import datetime


@pytest.fixture
def temp_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    yield conn
    conn.close()


def test_consolidation_priority_hotmart_over_manychat(temp_db):
    """
    Scenario: User exists in ManyChat, then is ingested from Hotmart.
    Assertion: Master data should reflect Hotmart (Source of Truth).
    """
    # 1. Insert into ManyChat Raw
    temp_db.execute("""
        INSERT INTO manychat_contacts (nome, email, whatsapp) 
        VALUES ('User ManyChat', 'user@test.com', '5511999999999')
    """)

    # 2. Consolidate (initial)
    consolidate_all_to_master(temp_db)

    # Check Master
    row = temp_db.execute(
        "SELECT * FROM customers WHERE master_email = 'user@test.com'"
    ).fetchone()
    assert row["name"] == "User ManyChat"
    assert row["source"] == "MANYCHAT"

    # 3. Ingest via Hotmart (different name)
    item = Customer(
        id="HOT123",
        email="user@test.com",
        name="User Hotmart",
        phone="5511999999999",
        created_at=datetime.now(),
    )
    upsert_customer(temp_db, item)

    # 4. Consolidate again
    consolidate_all_to_master(temp_db)

    # 5. Verify Priority
    row = temp_db.execute(
        "SELECT * FROM customers WHERE master_email = 'user@test.com'"
    ).fetchone()
    assert row["name"] == "User Hotmart"
    assert row["source"] == "HOTMART"
    assert row["hotmart_id"] == "HOT123"


def test_manychat_phone_requirement_rule(temp_db):
    """
    Scenario: ManyChat contact without phone.
    Assertion: Should NOT be created in Master.
    """
    temp_db.execute("""
        INSERT INTO manychat_contacts (nome, email, whatsapp) 
        VALUES ('No Phone', 'nophone@test.com', '')
    """)
    consolidate_all_to_master(temp_db)

    row = temp_db.execute(
        "SELECT * FROM customers WHERE master_email = 'nophone@test.com'"
    ).fetchone()
    assert row is None


def test_segmentation_logic_ambos(temp_db):
    """
    Scenario: User buys an ILPI product and an ESTETICA product.
    Assertion: Segment should be 'AMBOS'.
    """
    hotmart_user = Customer(
        id="U1", email="both@test.com", name="Mix User", created_at=datetime.now()
    )
    upsert_customer(temp_db, hotmart_user)

    # Sale 1: ILPI (Product ID not in estética list)
    sale1 = Sale(
        transaction="T1",
        status="APPROVED",
        total_price=10.0,
        currency="BRL",
        customer_id="U1",
        product_id="999",
    )
    # Sale 2: Estética (Product ID from list, e.g. 5587176)
    sale2 = Sale(
        transaction="T2",
        status="APPROVED",
        total_price=20.0,
        currency="BRL",
        customer_id="U1",
        product_id="5587176",
    )

    upsert_sale(temp_db, sale1)
    upsert_sale(temp_db, sale2)

    consolidate_all_to_master(temp_db)

    row = temp_db.execute(
        "SELECT * FROM customers WHERE master_email = 'both@test.com'"
    ).fetchone()
    assert row["segment"] == "AMBOS"
    assert bool(row["has_purchased"]) is True
