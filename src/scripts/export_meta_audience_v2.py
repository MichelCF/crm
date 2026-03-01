import sqlite3
import csv
import argparse
from pathlib import Path
from collections import defaultdict
from src.scripts.export_meta_audience import (
    get_estetica_product_ids,
    normalize_phone_and_get_state,
)
from src.config import Config


def export_meta_audience_v2(product_ids: list[str], output_file: str):
    db_path = Config.DB_NAME
    if not Path(db_path).exists():
        print(f"Erro: Banco de dados {db_path} não encontrado.")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Query all sales joining with customers
    cur.execute("""
        SELECT 
            c.email, 
            c.name, 
            c.phone, 
            s.product_id, 
            s.status, 
            s.total_price
        FROM sales s
        JOIN hotmart_customers c ON s.customer_id = c.id
    """)
    rows = cur.fetchall()

    # customers[email] = { "name": ..., "phone_raw": ..., "ltv": 0.0, "interacted": False }
    customers = defaultdict(
        lambda: {"name": "", "phone_raw": "", "ltv": 0.0, "interacted": False}
    )

    target_product_ids = set(product_ids)

    for row in rows:
        email = (row["email"] or "").strip().lower()
        if not email:
            continue

        prod_id = str(row["product_id"]).strip()
        status = (row["status"] or "").strip().upper()
        name = (row["name"] or "").strip()
        phone_raw = (row["phone"] or "").strip()
        valor_pago = float(row["total_price"] or 0.0)

        # Check interaction
        if prod_id in target_product_ids:
            customers[email]["interacted"] = True

        if name and not customers[email]["name"]:
            customers[email]["name"] = name
        if phone_raw and not customers[email]["phone_raw"]:
            customers[email]["phone_raw"] = phone_raw

        # Add to LTV if Approved or Complete
        # Status in DB are uppercase (e.g. APPROVED, COMPLETE, COMPLETED)
        if status in ("APPROVED", "COMPLETE", "COMPLETED"):
            customers[email]["ltv"] += valor_pago

    headers = ["name", "email", "phone", "country", "state", "value"]

    exported_count = 0
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for email, data in customers.items():
            if not data["interacted"]:
                continue

            name = data["name"]
            raw_phone = data["phone_raw"]
            value = round(data["ltv"], 2)

            phone, state = normalize_phone_and_get_state(raw_phone)
            country = "BR"

            writer.writerow([name, email, phone, country, state, value])
            exported_count += 1

    print(
        f"Exportação V2 concluída! {exported_count} contatos salvos em '{output_file}'."
    )
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera uma lista de público V2 lendo do SQLite."
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/meta_audience_v2.csv",
        help="Caminho do CSV de saída",
    )
    args = parser.parse_args()

    product_ids = get_estetica_product_ids()
    export_meta_audience_v2(product_ids, args.output)
