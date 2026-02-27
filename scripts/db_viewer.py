import os
import sys

# Adiciona o diretório raiz do projeto ao PYTHONPATH para ele achar a pasta 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.database import get_connection

def print_recent_sales(limit=5):
    """Obtém as últimas vendas processadas e seus respectivos clientes e produtos."""
    conn = get_connection()
    
    query = """
        SELECT 
            s.transaction_id, 
            s.status, 
            s.total_price,
            c.name as buyer_name,
            c.email as buyer_email,
            s.purchased_at
        FROM sales s
        JOIN customers c ON s.customer_id = c.id
        ORDER BY s.purchased_at DESC
        LIMIT ?
    """
    
    rows = conn.execute(query, (limit,)).fetchall()
    
    print("-" * 80)
    print(f"{'TRANSACTION':<15} | {'STATUS':<10} | {'PRICE':<10} | {'BUYER NAME':<20} | {'PURCHASED AT'}")
    print("-" * 80)
    
    if not rows:
        print("Nenhuma venda encontrada no banco.")
    
    for row in rows:
        print(f"{row['transaction_id']:<15} | {row['status']:<10} | R$ {row['total_price']:<7.2f} | {row['buyer_name'][:18]:<20} | {row['purchased_at']}")
        
    print("-" * 80)
    conn.close()

def db_stats():
    """Traz uma contagem rápida de volume do banco."""
    conn = get_connection()
    c_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    p_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    s_count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    
    print(f"📊 Resumo do Banco de Dados:")
    print(f"   👥 Clientes: {c_count}")
    print(f"   📦 Produtos: {p_count}")
    print(f"   🛒 Vendas:   {s_count}\n")
    conn.close()

if __name__ == "__main__":
    print("\n--- VISUALIZADOR RÁPIDO DO CRM ---")
    db_stats()
    
    print("Últimas 5 vendas (Ordem Decrescente):")
    print_recent_sales(5)
