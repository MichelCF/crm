import sqlite3
import csv
import os
from datetime import datetime
from typing import List

# SQL Templates
SQL_FIND_ELIGIBLE_REMARKETING = """
    SELECT 
        id as customer_id,
        master_email as email,
        master_phone as phone,
        last_remarketing_at,
        last_purchase_at
    FROM customers
    WHERE (
        last_remarketing_at IS NULL 
        OR datetime(last_remarketing_at) <= datetime('now', '-30 days')
    )
    AND (
        last_purchase_at IS NULL 
        OR datetime(last_purchase_at) <= datetime('now', '-30 days')
    )
    AND master_phone IS NOT NULL AND master_phone != ''
    ORDER BY RANDOM()
    LIMIT :limit
"""

SQL_INSERT_REMARKETING_HISTORY = """
    INSERT INTO remarketing_history (customer_id, email, phone, last_remarketing_at, last_purchase_at)
    VALUES (:customer_id, :email, :phone, :last_remarketing_at, :last_purchase_at)
"""


def generate_remarketing_batch(conn: sqlite3.Connection, limit: int = 50):
    """
    Identifies eligible customers, saves them to Gold history, and exports to CSV.
    """
    cur = conn.cursor()

    # 1. Fetch eligible
    cur.execute(SQL_FIND_ELIGIBLE_REMARKETING, {"limit": limit})
    eligible = cur.fetchall()

    if not eligible:
        print("Nenhum contato elegivel para remarketing hoje.")
        return

    # 2. Persist in Gold History
    for row in eligible:
        data = dict(row)
        cur.execute(SQL_INSERT_REMARKETING_HISTORY, data)

        # Update Master and ManyChat (simulated through Master for now)
        # In a real sync back to manychat, we would need an API call,
        # but here we update our local tracking.
        now_str = datetime.now().isoformat()
        cur.execute(
            "UPDATE customers SET last_remarketing_at = ? WHERE id = ?",
            (now_str, data["customer_id"]),
        )

    conn.commit()
    print(f"Lote de remarketing gerado com {len(eligible)} registros.")

    # 3. Export to CSV
    export_remarketing_csv(eligible)


def export_remarketing_csv(batch: List[sqlite3.Row]):
    """
    Exports a batch of eligible records to CSV.
    """
    output_dir = os.path.join("data", "output", "remarketing")
    os.makedirs(output_dir, exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"remarketing_{today_str}.csv"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["email", "phone", "last_remarketing_at", "last_purchase_at"])
        for row in batch:
            writer.writerow(
                [
                    row["email"],
                    row["phone"],
                    row["last_remarketing_at"],
                    row["last_purchase_at"],
                ]
            )

    print(f"Lote de remarketing exportado para: {filepath}")


def generate_remarketing_report(conn: sqlite3.Connection):
    """
    Prints a simple report on remarketing history.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM remarketing_history")
    total_total = cur.fetchone()[0]

    today_start = datetime.now().strftime("%Y-%m-%d 00:00:00")
    cur.execute(
        "SELECT COUNT(*) FROM remarketing_history WHERE created_at >= ?", (today_start,)
    )
    total_today = cur.fetchone()[0]

    print("\n" + "=" * 50)
    print("         RELATORIO DE REMARKETING (GOLD)")
    print("=" * 50)
    print(f"Total Historico:     {total_total} disparos")
    print(f"Gerados Hoje:        {total_today} contatos")
    print("=" * 50 + "\n")
