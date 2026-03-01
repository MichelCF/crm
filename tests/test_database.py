import sqlite3
import pytest
from datetime import datetime
from src.db.database import init_db, upsert_customer, upsert_product, upsert_sale
from src.models.schemas import Customer, Product, Sale


# Setup do Banco de Dados em Memória para Testes Rápidos
@pytest.fixture
def mock_db():
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    yield conn
    conn.close()


# =====================================================================
# BOUNDARY TESTS & MC/DC
# =====================================================================


def test_full_pipeline_insert_happy_path(mock_db):
    """
    Happy Path Test: Verifies that V2 enriched fields (Address, Payment)
    are correctly persisted in the SQLite tables.

    This test covers the 'All valid inputs' case for both
    upsert_customer and upsert_sale.
    """
    # 1. Insert Full Customer (V2)
    cust = Customer(
        id="CUST-999",
        name="Sr. Madruga",
        email="madruga@vila.com",
        phone="552199999999",
        document="11122233344",
        zip_code="12345-678",
        address="Rua 8",
        number="71",
        neighborhood="Vila",
        city="Cidade do México",
        state="CDMX",
        country="BR",
        created_at=datetime.now(),
    )
    upsert_customer(mock_db, cust)

    # 2. Insert Basic Product
    prod = Product(id="PROD-1", name="Curso de Violão")
    upsert_product(mock_db, prod)

    # 3. Insert Full Sale (V2)
    sale = Sale(
        transaction="TXN-12345",
        status="APPROVED",
        payment_method="PIX",
        payment_type="PIX",
        installments=1,
        total_price=99.90,
        currency="BRL",
        customer_id=cust.id,
        product_id=prod.id,
    )
    upsert_sale(mock_db, sale)

    # 4. Verify Persistence (Assertions)
    cur = mock_db.cursor()
    cur.execute(
        "SELECT neighborhood, zip_code FROM hotmart_customers WHERE id = ?",
        ("CUST-999",),
    )
    row_cust = cur.fetchone()
    assert row_cust[0] == "Vila"
    assert row_cust[1] == "12345-678"

    cur.execute(
        "SELECT payment_type, installments FROM sales WHERE transaction_id = ?",
        ("TXN-12345",),
    )
    row_sale = cur.fetchone()
    assert row_sale[0] == "PIX"
    assert row_sale[1] == 1


def test_insert_sale_negative_case(mock_db):
    """
    Negative/Boundary Test: Verifies SQLite constraints (NOT NULL).

    MC/DC consideration:
    In the database schema, 'status' and 'customer_id' are NOT NULL.
    This test verifies that missing a required field triggers an IntegrityError.
    """
    prod = Product(id="PROD-1", name="Curso de Violão")
    upsert_product(mock_db, prod)

    # Attempting to insert a sale with a NULL status (which is NOT NULL in DB)
    sale_invalid = Sale(
        transaction="TXN-INVALID",
        status=None,  # triggers IntegrityError on INSERT
        total_price=99.90,
        currency="BRL",
        customer_id="CUST-GHOST",
        product_id=prod.id,
    )

    with pytest.raises(sqlite3.IntegrityError):
        upsert_sale(mock_db, sale_invalid)


def test_upsert_customer_boundary_update(mock_db):
    """
    Boundary Test: Verifies that upserting an existing customer correctly updates
    their information instead of creating a duplicate.
    """
    # 1. Initial Insert
    cust1 = Customer(id="B-1", email="a@b.com", name="A", created_at=datetime.now())
    upsert_customer(mock_db, cust1)

    # 2. Update via Upsert (Same ID, different data)
    cust2 = Customer(
        id="B-1", email="new@b.com", name="New Name", created_at=datetime.now()
    )
    upsert_customer(mock_db, cust2)

    cur = mock_db.cursor()
    cur.execute("SELECT email, name FROM hotmart_customers WHERE id = 'B-1'")
    row = cur.fetchone()

    assert row[0] == "new@b.com"
    assert row[1] == "New Name"

    # Ensure there is still only 1 customer
    cur.execute("SELECT count(*) FROM hotmart_customers")
    assert cur.fetchone()[0] == 1
