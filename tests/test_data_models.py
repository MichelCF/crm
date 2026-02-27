import pytest
from pydantic import ValidationError
from datetime import datetime
from src.models.schemas import Customer, Product, Sale


def test_customer_model_valid():
    now = datetime.now()
    cust = Customer(
        id="123",
        email="test@example.com",
        name="Test User",
        phone="123456789",
        created_at=now,
    )
    assert cust.id == "123"
    assert cust.email == "test@example.com"
    assert cust.name == "Test User"
    assert cust.phone == "123456789"
    assert cust.created_at == now


def test_customer_model_invalid_email():
    now = datetime.now()
    with pytest.raises(ValidationError):
        Customer(id="123", email="not-an-email", name="Test User", created_at=now)


def test_product_model_valid():
    prod = Product(id=1, name="My Product")
    assert prod.id == 1
    assert prod.name == "My Product"


def test_sale_model_valid():
    now = datetime.now()
    sale = Sale(
        transaction_id="TX123",
        status="APPROVED",
        total_price=99.90,
        net_price=89.90,
        payment_method="CREDIT_CARD",
        purchased_at=now,
        customer_id="USER123",
        product_id=1,
    )
    assert sale.currency == "BRL"
    assert sale.transaction_id == "TX123"
    assert sale.total_price == 99.90
    assert sale.net_price == 89.90
    assert sale.payment_method == "CREDIT_CARD"
