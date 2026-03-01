import sqlite3
from typing import Dict, Any


def get_stats_for_load(
    conn: sqlite3.Connection, imported_at_filter: str = None
) -> Dict[str, Any]:
    """
    Calculates stats for a specific load or for all data if filter is None.
    Metrics: Unique buyers, Total Sales Value (Approved/Complete), Cancellations (Count/Value).
    """
    cur = conn.cursor()

    where_clause = ""
    params = []
    if imported_at_filter:
        where_clause = "WHERE imported_at = ?"
        params = [imported_at_filter]

    # 1. Unique buyers (unique customer_id in this batch)
    cur.execute(f"SELECT COUNT(DISTINCT customer_id) FROM sales {where_clause}", params)
    unique_buyers = cur.fetchone()[0] or 0

    # 2. Total Approved/Complete Sales Value
    # Note: Using IN for common positive statuses
    positive_statuses = ("APPROVED", "COMPLETE", "BILLET_PRINTED", "WAITING_PAYMENT")
    status_placeholders = ", ".join("?" for _ in positive_statuses)

    query_approved = f"""
        SELECT SUM(total_price) 
        FROM sales 
        {where_clause} {"AND" if where_clause else "WHERE"} status IN ({status_placeholders})
    """
    cur.execute(query_approved, params + list(positive_statuses))
    total_value = cur.fetchone()[0] or 0.0

    # 3. Cancellations
    negative_statuses = ("CANCELED", "REFUNDED", "CHARGEBACK", "PARTIALLY_REFUNDED")
    neg_placeholders = ", ".join("?" for _ in negative_statuses)

    query_cancelled = f"""
        SELECT COUNT(*), SUM(total_price)
        FROM sales
        {where_clause} {"AND" if where_clause else "WHERE"} status IN ({neg_placeholders})
    """
    cur.execute(query_cancelled, params + list(negative_statuses))
    row = cur.fetchone()
    cancelled_count = row[0] or 0
    cancelled_value = row[1] or 0.0

    return {
        "buyers": unique_buyers,
        "value": total_value,
        "cancelled_count": cancelled_count,
        "cancelled_value": cancelled_value,
    }


def generate_delta_report(conn: sqlite3.Connection):
    """
    Prints a report comparing the current (latest) load with the penultimate one.
    """
    cur = conn.cursor()

    # Find distinct import timestamps
    cur.execute(
        "SELECT DISTINCT imported_at FROM sales ORDER BY imported_at DESC LIMIT 2"
    )
    loads = [row[0] for row in cur.fetchall()]

    print("\n" + "=" * 50)
    print("         RELATÓRIO DE CONSOLIDAÇÃO (METRICS)")
    print("=" * 50)

    if len(loads) < 1:
        print("Nenhum dado encontrado para gerar relatório.")
        return

    current_load_time = loads[0]
    current_stats = get_stats_for_load(conn, current_load_time)

    if len(loads) == 2:
        prev_load_time = loads[1]
        prev_stats = get_stats_for_load(conn, prev_load_time)

        print(
            f"Comparativo: Atual ({current_load_time}) vs Anterior ({prev_load_time})"
        )
        print("-" * 50)

        def print_delta(label, curr, prev, is_currency=False):
            delta = curr - prev
            fmt = "R$ {:,.2f}" if is_currency else "{:,.0f}"
            delta_prefix = "+" if delta > 0 else ""
            print(
                f"{label:<25} | {fmt.format(curr):>10} | Delta: {delta_prefix}{fmt.format(delta)}"
            )

        print_delta("Compradores Únicos", current_stats["buyers"], prev_stats["buyers"])
        print_delta(
            "Valor Total (Vendas)", current_stats["value"], prev_stats["value"], True
        )
        print_delta(
            "Qtd Cancelamentos",
            current_stats["cancelled_count"],
            prev_stats["cancelled_count"],
        )
        print_delta(
            "Valor Cancelado",
            current_stats["cancelled_value"],
            prev_stats["cancelled_value"],
            True,
        )
    else:
        # Initial load or after DB wipe
        print(f"Carga Inicial detectada em: {current_load_time}")
        print("-" * 50)
        print(f"{'Compradores Únicos:':<25} {current_stats['buyers']}")
        print(f"{'Valor Total (Vendas):':<25} R$ {current_stats['value']:,.2f}")
        print(f"{'Qtd Cancelamentos:':<25} {current_stats['cancelled_count']}")
        print(f"{'Valor Cancelado:':<25} R$ {current_stats['cancelled_value']:,.2f}")

    print("=" * 50 + "\n")
