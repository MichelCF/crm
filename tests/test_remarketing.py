import pytest
from datetime import datetime, timedelta
from src.db.database import get_connection, init_db, upsert_master_customer
from src.logic.remarketing import generate_remarketing_batch


@pytest.fixture
def db_conn():
    conn = get_connection(":memory:")
    init_db(conn)
    yield conn
    conn.close()


def test_remarketing_eligibility_window_30_days(db_conn):
    # Rule: Eligible if last_remarketing_at is NULL or > 30 days ago
    # AND last_purchase_at is NULL or > 30 days ago

    now = datetime.now()
    recent = (now - timedelta(days=10)).isoformat()
    old = (now - timedelta(days=40)).isoformat()

    # Customer 1: Never contacted, never bought (Eligible)
    upsert_master_customer(db_conn, "MANYCHAT", email="e1@test.com", phone="111")

    # Customer 2: Contacted 40 days ago, bought 40 days ago (Eligible)
    upsert_master_customer(
        db_conn,
        "MANYCHAT",
        email="e2@test.com",
        phone="222",
        last_remarketing_at=old,
        last_purchase_at=old,
    )

    # Customer 3: Contacted 10 days ago (Ineligible)
    upsert_master_customer(
        db_conn,
        "MANYCHAT",
        email="i3@test.com",
        phone="333",
        last_remarketing_at=recent,
    )

    # Customer 4: Bought 10 days ago (Ineligible)
    upsert_master_customer(
        db_conn, "MANYCHAT", email="i4@test.com", phone="444", last_purchase_at=recent
    )

    # Run
    generate_remarketing_batch(db_conn, limit=10)

    cur = db_conn.cursor()
    cur.execute("SELECT email FROM remarketing_history")
    emails = [r["email"] for r in cur.fetchall()]

    assert "e1@test.com" in emails
    assert "e2@test.com" in emails
    assert "i3@test.com" not in emails
    assert "i4@test.com" not in emails


def test_remarketing_limit_and_random(db_conn):
    # Create 60 eligible customers
    for i in range(60):
        upsert_master_customer(
            db_conn, "MANYCHAT", email=f"u{i}@test.com", phone=f"p{i}"
        )

    # Generate batch with limit 50
    generate_remarketing_batch(db_conn, limit=50)

    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM remarketing_history")
    assert cur.fetchone()[0] == 50

    # Verify Master was updated
    cur.execute("SELECT COUNT(*) FROM customers WHERE last_remarketing_at IS NOT NULL")
    assert cur.fetchone()[0] == 50


def test_remarketing_phone_requirement(db_conn):
    # Eligible if has phone
    upsert_master_customer(db_conn, "MANYCHAT", email="phone@test.com", phone="123")
    # Ineligible if no phone (Rule 2. in implementation_plan)
    # Master logic already prevents ManyChat without phone, but let's double check SQL filter
    db_conn.execute(
        "INSERT INTO customers (master_email, master_phone) VALUES (?, ?)",
        ("nophone@test.com", ""),
    )

    generate_remarketing_batch(db_conn, limit=10)

    cur = db_conn.cursor()
    cur.execute("SELECT email FROM remarketing_history")
    emails = [r["email"] for r in cur.fetchall()]
    assert "phone@test.com" in emails
    assert "nophone@test.com" not in emails
