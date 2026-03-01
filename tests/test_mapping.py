import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from hypothesis import given, strategies as st
from src.pipelines.hotmart_to_db import (
    _parse_hotmart_date,
    _resolve_buyer_id,
    _extract_sale_models,
)
from src.models.schemas import Customer, Product, Sale


# 1. Boundary Testing: _parse_hotmart_date
@pytest.mark.parametrize(
    "date_input, expected_type",
    [
        (None, datetime),
        ("", datetime),
        (0, datetime),
        (1704067200000, datetime),  # Valid MS
        ("1704067200000", datetime),  # Valid String MS
        (-1, datetime),  # Negative timestamp
        ("invalid", datetime),  # Malformed
    ],
)
def test_parse_hotmart_date_boundaries(date_input, expected_type):
    """
    Verifica se a conversão de data é resiliente a inputs extremos ou malformados.
    """
    result = _parse_hotmart_date(date_input)
    assert isinstance(result, expected_type)


# 2. MC/DC: _resolve_buyer_id
@pytest.mark.parametrize(
    "buyer_data, txn_id, expected",
    [
        ({"ucode": "U1", "id": "I1"}, "T1", "U1"),  # Condition A (ucode) is True
        ({"id": "I1"}, "T1", "I1"),  # Condition A is False, B (id) is True
        ({}, "T1", "T1"),  # Condition A and B are False
    ],
)
def test_resolve_buyer_id_mcdc(buyer_data, txn_id, expected):
    """
    Modified Condition/Decision Coverage:
    Garante que cada critério (ucode, id, txn) influencie o resultado final de forma independente.
    """
    assert _resolve_buyer_id(buyer_data, txn_id) == expected


# 3. Property-Based Testing: _extract_sale_models (No Crash)
@given(
    item=st.dictionaries(
        keys=st.text(),
        values=st.one_of(
            st.text(),
            st.integers(),
            st.floats(),
            st.dictionaries(keys=st.text(), values=st.text()),
        ),
    )
)
@patch("src.pipelines.hotmart_to_db.get_sale_price_details")
@patch("src.pipelines.hotmart_to_db.get_sale_users")
def test_extract_sale_models_no_crash_property(mock_get_users, mock_get_price, item):
    """
    Property-Based Testing:
    Garante que não importa o lixo que venha no JSON da Hotmart, a função nunca
    dispara uma exceção não tratada (No-Crash Property).
    """
    mock_get_users.return_value = {}
    mock_get_price.return_value = {}
    mock_client = MagicMock()

    try:
        customer, product, sale = _extract_sale_models(item, mock_client)
        assert isinstance(customer, Customer)
        assert isinstance(product, Product)
        assert isinstance(sale, Sale)
    except Exception as e:
        pytest.fail(f"Mapping crashed with item {item}: {e}")


# 4. Functional / Happy Path (Regressão)
@patch("src.pipelines.hotmart_to_db.get_sale_price_details")
@patch("src.pipelines.hotmart_to_db.get_sale_users")
def test_extract_sale_models_happy_path(mock_get_users, mock_get_price):
    """
    Regressão: Verifica se o mapeamento básico continua funcionando após a refatoração.
    """
    item = {
        "purchase": {
            "transaction": "TX123",
            "status": "APPROVED",
            "price": {"value": 100.0},
            "order_date": 1704067200000,
            "currency": "BRL",
        },
        "buyer": {"name": "Test User", "email": "test@test.com"},
        "product": {"id": "P1", "name": "Test Prod"},
    }
    mock_get_users.return_value = {}
    mock_get_price.return_value = {}

    customer, product, sale = _extract_sale_models(item, MagicMock())

    assert customer.name == "Test User"
    assert product.name == "Test Prod"
    assert sale.transaction == "TX123"
    assert sale.status == "APPROVED"
