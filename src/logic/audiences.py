import sqlite3
import csv
import os
from datetime import datetime
from src.logic.user_logic import ESTETICA_PRODUCT_IDS
from src.db.database import upsert_audience_member

# SQL Templates
SQL_FETCH_SALES_WITH_CUSTOMERS = """
    SELECT 
        c.name,
        c.master_email as email,
        c.master_phone as phone,
        s.product_id,
        s.total_price,
        s.status
    FROM sales s
    JOIN customers c ON s.customer_id = c.hotmart_id
    WHERE s.status IN ('APPROVED', 'COMPLETE')
"""

SQL_COUNT_AUDIENCE = "SELECT COUNT(*) FROM {}"

SQL_EXPORT_AUDIENCE = """
    SELECT name, email, phone, country, state, value 
    FROM {} 
    ORDER BY value DESC
"""


def refresh_audiences(conn: sqlite3.Connection):
    """
    Orchestrates the audience refresh process.
    """
    audiences_data = _get_aggregated_audience_data(conn)
    _persist_audience_data(conn, audiences_data)
    print(
        f"Audiences refreshed successfully for {len(audiences_data)} unique customers."
    )


def _get_aggregated_audience_data(conn: sqlite3.Connection) -> dict:
    """
    Extracts sales and aggregates LTV per customer and segment.
    """
    cur = conn.cursor()
    cur.execute(SQL_FETCH_SALES_WITH_CUSTOMERS)
    rows = cur.fetchall()

    aggregated = {}
    now_str = datetime.now().isoformat()

    for row in rows:
        email = row["email"].lower().strip()
        if not email:
            continue

        if email not in aggregated:
            aggregated[email] = {
                "name": row["name"],
                "email": email,
                "phone": row["phone"],
                "ilpi_value": 0.0,
                "estetica_value": 0.0,
                "updated_at": now_str,
            }

        is_estetica = row["product_id"] in ESTETICA_PRODUCT_IDS
        val = row["total_price"] or 0.0

        if is_estetica:
            aggregated[email]["estetica_value"] += val
        else:
            aggregated[email]["ilpi_value"] += val

    return aggregated


def _persist_audience_data(conn: sqlite3.Connection, audiences_data: dict):
    """
    Persists the aggregated data into Gold tables.
    """
    for email, data in audiences_data.items():
        common_info = {
            "name": data["name"],
            "email": data["email"],
            "phone": data["phone"],
            "country": "BR",
            "state": "",
            "updated_at": data["updated_at"],
        }

        if data["ilpi_value"] > 0:
            upsert_audience_member(
                conn,
                "audience_ilpi",
                {**common_info, "value": round(data["ilpi_value"], 2)},
            )

        if data["estetica_value"] > 0:
            upsert_audience_member(
                conn,
                "audience_estetica",
                {**common_info, "value": round(data["estetica_value"], 2)},
            )


def generate_audience_report(conn: sqlite3.Connection):
    """
    Prints a simple report about audience sizes using SQL templates.
    """
    cur = conn.cursor()

    cur.execute(SQL_COUNT_AUDIENCE.format("audience_ilpi"))
    ilpi_count = cur.fetchone()[0]

    cur.execute(SQL_COUNT_AUDIENCE.format("audience_estetica"))
    estetica_count = cur.fetchone()[0]

    print("\n" + "=" * 50)
    print("         RELATORIO DE PUBLICOS (GOLD)")
    print("=" * 50)
    print(f"Publico ILPI:       {ilpi_count} registros")
    print(f"Publico Estetica:   {estetica_count} registros")
    print("=" * 50 + "\n")


def export_audiences_to_csv(conn: sqlite3.Connection):
    """
    Exports Gold tables to CSV files using SQL templates.
    """
    output_dir = os.path.join("data", "output", "publico")
    os.makedirs(output_dir, exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    tables = ["audience_ilpi", "audience_estetica"]

    for table in tables:
        filename = f"publico_{table.replace('audience_', '')}_{today_str}.csv"
        filepath = os.path.join(output_dir, filename)

        cur = conn.cursor()
        cur.execute(SQL_EXPORT_AUDIENCE.format(table))
        rows = cur.fetchall()

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "email", "phone", "country", "state", "value"])
            for row in rows:
                writer.writerow(row)

        print(f"Publico exportado para: {filepath} ({len(rows)} registros)")
