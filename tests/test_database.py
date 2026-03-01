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
# Objetivo do Teste: Validar inserção no SQLite com campos enriquecidos de Endereço e Pagamento
# Valores esperados:
# 1. No Ponto: Cliente e Venda com dados completos de V2 (Bairro, CEP, Pix, etc) inseridos corretamente.
# 2. Falha: Tentativa de inserir uma Venda sem Customer associado ou com nulos indevidos (violando a FK/constraints).
# =====================================================================


def test_full_pipeline_insert_happy_path(mock_db):
    """Verifica se os novos campos de V2 são de fato gravados no banco nas tabelas certas."""
    # 1. Insere Cliente Completo
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

    # 2. Insere Produto Básico
    prod = Product(id="PROD-1", name="Curso de Violão")
    upsert_product(mock_db, prod)

    # 3. Insere Venda Completa
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

    # 4. Verifica na Tabela (Assert)
    cur = mock_db.cursor()
    cur.execute(
        "SELECT neighborhood, zip_code FROM hotmart_customers WHERE id = 'CUST-999'"
    )
    row_cust = cur.fetchone()
    assert row_cust[0] == "Vila"
    assert row_cust[1] == "12345-678"

    cur.execute(
        "SELECT payment_type, installments FROM sales WHERE transaction_id = 'TXN-12345'"
    )
    row_sale = cur.fetchone()
    assert row_sale[0] == "PIX"
    assert row_sale[1] == 1


def test_insert_sale_negative_case(mock_db):
    """Testa se o SQLite respeita as constraints e impede de cadastrar sem os campos NOT NULL."""
    prod = Product(id="PROD-1", name="Curso de Violão")
    upsert_product(mock_db, prod)

    # Tentando forçar objeto pydantic ou mock vazio que passa no Python mas quebra as premissas do SQL (Falta o status obrigatório no BD)
    sale_invalid = Sale(
        transaction="TXN-INVALID",
        status=None,  # BD exige NOT NULL de acordo com schemas originais
        total_price=99.90,
        currency="BRL",
        customer_id="CUST-GHOST",  # Customer não existe
        product_id=prod.id,
    )

    with pytest.raises(sqlite3.IntegrityError):
        upsert_sale(mock_db, sale_invalid)
