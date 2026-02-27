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

def test_sales_history_enforces_contract(mock_env):
    """
    Ensures that empty requests or requests missing required date params 
    fail immediately without hitting the network.
    """
    with pytest.raises(ValidationError) as exc_info:
        get_sales_history() # Missing start_date and end_date
    
    # Check that both fields are missing
    assert "start_date" in str(exc_info.value)
    assert "end_date" in str(exc_info.value)

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
        json={"access_token": "valid_token", "token_type": "bearer", "expires_in": 3600}, 
        status=200
    )
    
    # 2. Sales Endpoint 
    # With parameters appended via requests (they should match our Pydantic model dump)
    api_url = "https://developers.hotmart.com/payments/api/v1/sales/history"
    responses.add(
        responses.GET, 
        api_url, 
        json={"items": [{"transaction": "HP123", "status": "APPROVED"}], "page_info": {}}, 
        status=200
    )
    
    # Should not raise exception
    response = get_sales_history(start_date="1672531200000", end_date="1704067199000")
    
    assert "items" in response
    assert len(response["items"]) == 1
    
    # Assert query string
    request = responses.calls[1].request
    assert "start_date=1672531200000" in request.url
    assert "end_date=1704067199000" in request.url
