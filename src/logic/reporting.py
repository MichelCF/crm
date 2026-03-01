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
    Prints a report comparing the current (latest) load with the penultimate one
    and saves it to a .txt file.
    """
    from datetime import datetime
    import os

    cur = conn.cursor()

    # Find distinct import timestamps
    cur.execute(
        "SELECT DISTINCT imported_at FROM sales ORDER BY imported_at DESC LIMIT 2"
    )
    loads = [row[0] for row in cur.fetchall()]

    report_lines = []
    report_lines.append("\n" + "=" * 50)
    report_lines.append("         RELATORIO DE CONSOLIDACAO (METRICAS)")
    report_lines.append("=" * 50)

    if len(loads) < 1:
        report_lines.append("Nenhum dado encontrado para gerar relatorio.")
    else:
        current_load_time = loads[0]
        current_stats = get_stats_for_load(conn, current_load_time)

        if len(loads) == 2:
            prev_load_time = loads[1]
            prev_stats = get_stats_for_load(conn, prev_load_time)

            report_lines.append(
                f"Comparativo: Atual ({current_load_time}) vs Anterior ({prev_load_time})"
            )
            report_lines.append("-" * 50)

            def add_delta(label, curr, prev, is_currency=False):
                delta = curr - prev
                fmt = "R$ {:,.2f}" if is_currency else "{:,.0f}"
                delta_prefix = "+" if delta > 0 else ""
                report_lines.append(
                    f"{label:<25} | {fmt.format(curr):>10} | Delta: {delta_prefix}{fmt.format(delta)}"
                )

            add_delta(
                "Compradores Unicos", current_stats["buyers"], prev_stats["buyers"]
            )
            add_delta(
                "Valor Total (Vendas)",
                current_stats["value"],
                prev_stats["value"],
                True,
            )
            add_delta(
                "Qtd Cancelamentos",
                current_stats["cancelled_count"],
                prev_stats["cancelled_count"],
            )
            add_delta(
                "Valor Cancelado",
                current_stats["cancelled_value"],
                prev_stats["cancelled_value"],
                True,
            )
        else:
            # Initial load or after DB wipe
            report_lines.append(f"Carga Inicial detectada em: {current_load_time}")
            report_lines.append("-" * 50)
            report_lines.append(
                f"{'Compradores Unicos:':<25} {current_stats['buyers']}"
            )
            report_lines.append(
                f"{'Valor Total (Vendas):':<25} R$ {current_stats['value']:,.2f}"
            )
            report_lines.append(
                f"{'Qtd Cancelamentos:':<25} {current_stats['cancelled_count']}"
            )
            report_lines.append(
                f"{'Valor Cancelado:':<25} R$ {current_stats['cancelled_value']:,.2f}"
            )

    report_lines.append("=" * 50 + "\n")

    # Final string
    full_report = "\n".join(report_lines)

    # Print to console
    print(full_report)

    # Save to file
    today_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"relatorio_geral_{today_str}.txt"
    report_path = os.path.join("data", "reports", filename)

    try:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "a", encoding="utf-8") as f:
            f.write(full_report)
        print(f"Relatorio salvo em: {report_path}")
    except Exception as e:
        print(f"Erro ao salvar relatorio em arquivo: {e}")
