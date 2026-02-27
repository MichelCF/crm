import os
import base64
import requests
from typing import Optional


class HotmartAuth:
    AUTH_URL = "https://api-sec-vlc.hotmart.com/security/oauth/token"

    def __init__(
        self, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        self.client_id = client_id or os.getenv("HOTMART_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("HOTMART_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "HOTMART_CLIENT_ID and HOTMART_CLIENT_SECRET map to be provided."
            )

        self._access_token: Optional[str] = None

    def _get_basic_auth_header(self) -> str:
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded_credentials}"

    def get_access_token(self, force_refresh: bool = False) -> str:
        if self._access_token and not force_refresh:
            return self._access_token

        headers = {
            "Authorization": self._get_basic_auth_header(),
            "Content-Type": "application/json",
        }

        url = f"{self.AUTH_URL}?grant_type=client_credentials"

        response = requests.post(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        self._access_token = data.get("access_token")

        if not self._access_token:
            raise ValueError(
                "Failed to retrieve access_token from Hotmart API response."
            )

        return self._access_token
