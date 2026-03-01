import pytest
import os
import responses
from pydantic import ValidationError
from src.hotmart.sales import get_sales_history


@pytest.fixture
def mock_env():
    os.environ["HOTMART_CLIENT_ID"] = "test_client_id"
    os.environ["HOTMART_CLIENT_SECRET"] = "test_client_secret"
    yield
    del os.environ["HOTMART_CLIENT_ID"]
    del os.environ["HOTMART_CLIENT_SECRET"]


def test_sales_history_enforces_contract_mcdc(mock_env):
    """
    MC/DC Test: Validates API contract enforcement for get_sales_history.
    Decision: get_sales_history requires BOTH start_date (A) and end_date (B).
    Truth Table:
    A (start) | B (end) | Output
    ----------------------------
    False     | False   | Raises ValidationError (Missing both)
    True      | False   | Raises ValidationError (Missing end_date)
    False     | True    | Raises ValidationError (Missing start_date)
    True      | True    | Success (Implicit in `test_sales_history_success_contract`)
    """
    # Case 1: Both missing (False, False)
    with pytest.raises(ValidationError) as exc_info:
        get_sales_history()

    assert "start_date" in str(exc_info.value)

    # Case 2: start_date present, end_date missing (True, False)
    with pytest.raises(ValidationError) as exc_info:
        get_sales_history(start_date="1672531200000")

    assert "end_date" in str(exc_info.value)

    # Case 3: start_date missing, end_date present (False, True)
    with pytest.raises(ValidationError) as exc_info:
        get_sales_history(end_date="1704067199000")

    assert "start_date" in str(exc_info.value)


@responses.activate
def test_sales_history_success_contract(mock_env):
    """
    Ensures that valid contracts pass parsing and trigger the underlying API Call.
    """
    # 1. Auth Endpoint
    auth_url = "https://api-sec-vlc.hotmart.com/security/oauth/token?grant_type=client_credentials"
    responses.add(
        responses.POST,
        auth_url,
        json={
            "access_token": "valid_token",
            "token_type": "bearer",
            "expires_in": 3600,
        },
        status=200,
    )

    # 2. Sales Endpoint
    # With parameters appended via requests (they should match our Pydantic model dump)
    api_url = "https://developers.hotmart.com/payments/api/v1/sales/history"
    responses.add(
        responses.GET,
        api_url,
        json={
            "items": [{"transaction": "HP123", "status": "APPROVED"}],
            "page_info": {},
        },
        status=200,
    )

    # Should not raise exception
    response = get_sales_history(start_date="1672531200000", end_date="1704067199000")

    assert "items" in response
    assert len(response["items"]) == 1

    # Assert query string
    request = responses.calls[1].request
    assert "start_date=1672531200000" in request.url
    assert "end_date=1704067199000" in request.url
