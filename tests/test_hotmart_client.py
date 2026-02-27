import pytest
import responses
import os
from unittest.mock import patch
from src.hotmart.client import HotmartClient


@pytest.fixture
def mock_env():
    os.environ["HOTMART_CLIENT_ID"] = "test_client_id"
    os.environ["HOTMART_CLIENT_SECRET"] = "test_client_secret"
    yield
    del os.environ["HOTMART_CLIENT_ID"]
    del os.environ["HOTMART_CLIENT_SECRET"]


@responses.activate
def test_hotmart_client_request_success(mock_env):
    # Mock the Auth request
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

    # Mock the API request
    client = HotmartClient()
    endpoint = "sales/history"
    api_url = f"{client.BASE_URL}/{endpoint}"

    mock_response = {
        "items": [{"transaction": "HP123456", "product": {"name": "Course 1"}}]
    }

    responses.add(
        responses.GET,
        api_url,
        match=[
            responses.matchers.header_matcher({"Authorization": "Bearer valid_token"})
        ],
        json=mock_response,
        status=200,
    )

    result = client._request("GET", endpoint)

    assert "items" in result
    assert result["items"][0]["transaction"] == "HP123456"


@patch.object(HotmartClient, "_request")
def test_hotmart_client_get_method(mock_request, mock_env):
    client = HotmartClient()
    mock_request.return_value = {"success": True}

    result = client.get("some/endpoint", param1="value")

    mock_request.assert_called_once_with("GET", "some/endpoint", param1="value")
    assert result == {"success": True}


@patch.object(HotmartClient, "_request")
def test_hotmart_client_post_method(mock_request, mock_env):
    client = HotmartClient()
    mock_request.return_value = {"created": True}

    result = client.post("another/endpoint", json={"data": 123})

    mock_request.assert_called_once_with("POST", "another/endpoint", json={"data": 123})
    assert result == {"created": True}
