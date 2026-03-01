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


# Removed invalid email test because V2 accepts string emails to bypass Hotmart bad data


def test_product_model_valid():
    prod = Product(id="1", name="My Product")
    assert prod.id == "1"
    assert prod.name == "My Product"


def test_sale_model_valid():
    sale = Sale(
        transaction="TX123",
        status="APPROVED",
        total_price=99.90,
        payment_method="CREDIT_CARD",
        customer_id="USER123",
        product_id="1",
        currency="BRL"
    )
    assert sale.currency == "BRL"
    assert sale.transaction == "TX123"
    assert sale.total_price == 99.90
    assert sale.payment_method == "CREDIT_CARD"
