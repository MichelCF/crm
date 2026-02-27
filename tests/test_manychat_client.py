import pytest
import responses
import os
from src.manychat.client import ManyChatClient

@pytest.fixture
def mock_env():
    os.environ["MANYCHAT_API_KEY"] = "fake_test_key_123"
    yield
    del os.environ["MANYCHAT_API_KEY"]

def test_manychat_client_initialization_without_key_raises_error():
    # Make sure env is clean
    if "MANYCHAT_API_KEY" in os.environ:
        del os.environ["MANYCHAT_API_KEY"]
        
    with pytest.raises(ValueError, match="MANYCHAT_API_KEY is not set"):
        ManyChatClient()

def test_manychat_client_initialization_with_env(mock_env):
    client = ManyChatClient()
    assert client.api_key == "fake_test_key_123"

def test_manychat_client_headers(mock_env):
    client = ManyChatClient()
    headers = client.get_headers()
    
    assert headers["Authorization"] == "Bearer fake_test_key_123"
    assert headers["Content-Type"] == "application/json"
    assert headers["Accept"] == "application/json"

@responses.activate
def test_manychat_client_request_success(mock_env):
    client = ManyChatClient()
    endpoint = "page/getTags"
    url = f"{client.BASE_URL}/{endpoint}"
    
    # Mocking based on standard ManyChat API documentation structure
    mock_response = {
        "status": "success",
        "data": [
            {"id": 1, "name": "Tag 1"},
            {"id": 2, "name": "Tag 2"}
        ]
    }
    
    responses.add(responses.GET, url, json=mock_response, status=200)
    
    result = client._request("GET", endpoint)
    
    assert result["status"] == "success"
    assert len(result["data"]) == 2
    assert result["data"][0]["name"] == "Tag 1"
