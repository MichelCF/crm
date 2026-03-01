import pytest
from hypothesis import given, strategies as st
from pydantic import ValidationError
from src.models.schemas import Customer, Sale
from datetime import datetime

# =====================================================================
# Objetivo do Teste: Validar o schema de Customer (Self-Testing Pydantic)
# Valores esperados:
# 1. No Ponto: Cliente com todos os campos preenchidos corretamente.
# 2. Falha: Cliente sem ID ou sem Email (campos estritamente necessários se exigidos, ou tipos errados).
# 3. Intervalo Positivo/Property-Based: Fuzzing de strings vazias, nulas e longas para garantir que o modelo não quebra por causa de lixo na string.
# =====================================================================


def test_customer_happy_path():
    """Testa o instanciamento no ponto com dados perfeitamente formatados."""
    customer = Customer(
        id="12345",
        name="John Doe",
        email="john@example.com",
        phone="5511999999999",
        document="12345678900",
        zip_code="01000-000",
        address="Rua A",
        number="123",
        neighborhood="Centro",
        city="São Paulo",
        state="SP",
        country="BR",
        created_at=datetime.now(),
    )
    assert customer.id == "12345"
    assert customer.email == "john@example.com"
    assert customer.city == "São Paulo"


def test_customer_negative_case():
    """Testa falhas de validação quando o tipo está incorreto ou faltam campos requeridos."""
    with pytest.raises(ValidationError):
        # Falta o ID e created_at, que não tem valor default e não são opcionais
        Customer(name="John Doe")


@given(
    id_str=st.text(min_size=1, max_size=50),
    name_str=st.one_of(st.none(), st.text(max_size=100)),
    email_str=st.one_of(st.none(), st.emails()),
    zip_str=st.one_of(st.none(), st.text(max_size=20)),
)
def test_customer_property_boundaries(id_str, name_str, email_str, zip_str):
    """Garante que a Pydantic supporta bem sujeira de strings e None nos opcionais sem crashar."""
    customer = Customer(
        id=id_str,
        name=name_str,
        email=email_str,
        zip_code=zip_str,
        created_at=datetime.now(),
    )
    assert customer.id == id_str
    assert customer.zip_code == zip_str


# =====================================================================
# Objetivo do Teste: Validar o schema de Sale
# Valores esperados:
# 1. No Ponto: Venda completa com tipos de pagamentos variados.
# 2. Falha: Atribuir string num campo float (total_price) que não possa ser convertido.
# 3. Intervalo Positivo: Valores limitrofes numéricos iterados via Property-Based.
# =====================================================================


def test_sale_happy_path():
    """Testa se a Venda acomoda os novos campos de pagamento sem problemas."""
    sale = Sale(
        transaction="HP123",
        status="APPROVED",
        payment_method="CREDIT_CARD",
        payment_type="CREDIT_CARD",
        installments=12,
        total_price=1500.50,
        currency="BRL",
        product_id="PROD1",
        customer_id="CUST1",
    )
    assert sale.transaction == "HP123"
    assert sale.installments == 12


def test_sale_negative_case():
    """Garante que não aceitamos lixo em campos que são obrigatoriamente numéricos."""
    with pytest.raises(ValidationError):
        Sale(
            transaction="HP123",
            product_id="PROD1",
            customer_id="CUST1",
            total_price="NOT_A_NUMBER",  # Pydantic deve rejeitar
        )


@given(
    total_price=st.one_of(st.none(), st.floats(allow_nan=False, allow_infinity=False)),
    installments=st.one_of(st.none(), st.integers(min_value=1, max_value=24)),
)
def test_sale_property_boundaries(total_price, installments):
    """Injeta valores aleatórios limitrofes de preço e parcelas."""
    sale = Sale(
        transaction="TXN_RAND",
        total_price=total_price,
        installments=installments,
        product_id="P1",
        customer_id="C1",
    )
    assert sale.transaction == "TXN_RAND"
    assert sale.installments == installments
