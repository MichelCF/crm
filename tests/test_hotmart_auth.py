import pytest
import responses
import os
import base64
from src.hotmart.auth import HotmartAuth

@pytest.fixture
def mock_env():
    os.environ["HOTMART_CLIENT_ID"] = "test_client_id"
    os.environ["HOTMART_CLIENT_SECRET"] = "test_client_secret"
    yield
    del os.environ["HOTMART_CLIENT_ID"]
    del os.environ["HOTMART_CLIENT_SECRET"]

def test_hotmart_auth_initialization_without_credentials_raises_error():
    if "HOTMART_CLIENT_ID" in os.environ:
        del os.environ["HOTMART_CLIENT_ID"]
    if "HOTMART_CLIENT_SECRET" in os.environ:
        del os.environ["HOTMART_CLIENT_SECRET"]
        
    with pytest.raises(ValueError, match="HOTMART_CLIENT_ID and HOTMART_CLIENT_SECRET"):
        HotmartAuth()

def test_hotmart_auth_basic_header(mock_env):
    auth = HotmartAuth()
    expected_b64 = base64.b64encode(b"test_client_id:test_client_secret").decode()
    assert auth._get_basic_auth_header() == f"Basic {expected_b64}"

@responses.activate
def test_hotmart_auth_get_access_token(mock_env):
    auth = HotmartAuth()
    
    mock_response = {
        "access_token": "mocked_hotmart_access_token",
        "token_type": "bearer",
        "expires_in": 3600
    }
    
    url = f"{auth.AUTH_URL}?grant_type=client_credentials"
    responses.add(responses.POST, url, json=mock_response, status=200)
    
    token = auth.get_access_token()
    
    assert token == "mocked_hotmart_access_token"
    assert auth._access_token == "mocked_hotmart_access_token"
    
    # Second call should use cached token and not raise a responses.exceptions.ConnectionError
    token2 = auth.get_access_token()
    assert token2 == "mocked_hotmart_access_token"
    assert len(responses.calls) == 1  # Verify it only made one HTTP request
