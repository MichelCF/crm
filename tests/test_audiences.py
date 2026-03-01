import pytest
from src.db.database import get_connection, init_db, upsert_master_customer, upsert_sale
from src.logic.audiences import refresh_audiences
from src.models.schemas import Sale
from src.logic.user_logic import ESTETICA_PRODUCT_IDS


@pytest.fixture
def db_conn():
    conn = get_connection(":memory:")
    init_db(conn)
    yield conn
    conn.close()


def test_audience_segmentation_logic(db_conn):
    # Setup IDs for testing
    ILPI_PROD = "9999999"
    ESTETICA_PROD = list(ESTETICA_PRODUCT_IDS)[0]

    # Products
    db_conn.execute(
        "INSERT INTO products (id, name) VALUES (?, ?)", (ILPI_PROD, "ILPI Prod")
    )
    db_conn.execute(
        "INSERT INTO products (id, name) VALUES (?, ?)",
        (ESTETICA_PROD, "Estetica Prod"),
    )

    # Customers
    upsert_master_customer(
        db_conn, "HOTMART", email="a@test.com", name="User A", hotmart_id="H_A"
    )
    upsert_master_customer(
        db_conn, "HOTMART", email="b@test.com", name="User B", hotmart_id="H_B"
    )
    upsert_master_customer(
        db_conn, "HOTMART", email="c@test.com", name="User C", hotmart_id="H_C"
    )

    # helper for sale
    def add_s(cid, pid, val):
        s = Sale(
            transaction=f"TX_{cid}_{pid}",
            status="APPROVED",
            total_price=val,
            currency="BRL",
            customer_id=cid,
            product_id=pid,
        )
        upsert_sale(db_conn, s)

    add_s("H_A", ESTETICA_PROD, 100.0)
    add_s("H_B", ILPI_PROD, 50.0)
    add_s("H_C", ESTETICA_PROD, 200.0)
    add_s("H_C", ILPI_PROD, 30.0)

    refresh_audiences(db_conn)

    cur = db_conn.cursor()
    cur.execute("SELECT email, value FROM audience_estetica ORDER BY email")
    estetica = {row["email"]: row["value"] for row in cur.fetchall()}
    assert estetica["a@test.com"] == 100.0
    assert estetica["c@test.com"] == 200.0

    cur.execute("SELECT email, value FROM audience_ilpi ORDER BY email")
    ilpi = {row["email"]: row["value"] for row in cur.fetchall()}
    assert ilpi["b@test.com"] == 50.0
    assert ilpi["c@test.com"] == 30.0


def test_audience_ltv_upsert(db_conn):
    ESTETICA_PROD = list(ESTETICA_PRODUCT_IDS)[0]
    upsert_master_customer(
        db_conn, "HOTMART", email="ltv@test.com", name="Luser", hotmart_id="H_L"
    )

    s1 = Sale(
        transaction="T1",
        status="APPROVED",
        total_price=10.0,
        currency="BRL",
        customer_id="H_L",
        product_id=ESTETICA_PROD,
    )
    upsert_sale(db_conn, s1)
    refresh_audiences(db_conn)

    cur = db_conn.cursor()
    cur.execute("SELECT value FROM audience_estetica WHERE email='ltv@test.com'")
    assert cur.fetchone()["value"] == 10.0

    s2 = Sale(
        transaction="T2",
        status="APPROVED",
        total_price=15.5,
        currency="BRL",
        customer_id="H_L",
        product_id=ESTETICA_PROD,
    )
    upsert_sale(db_conn, s2)
    refresh_audiences(db_conn)

    cur.execute("SELECT value FROM audience_estetica WHERE email='ltv@test.com'")
    assert cur.fetchone()["value"] == 25.5
