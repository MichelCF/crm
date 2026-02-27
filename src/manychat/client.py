import os
import requests
from typing import Dict, Any, Optional


class ManyChatClient:
    BASE_URL = "https://api.manychat.com/fb"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("MANYCHAT_API_KEY")
        if not self.api_key:
            raise ValueError(
                "MANYCHAT_API_KEY is not set. Please set it in the environment or .env file."
            )

    def get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self.get_headers()

        # Merge headers if provided in kwargs
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        response = requests.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()

    # The actual methods (get_tags, add_tag, etc.) will use self._request
