import requests
from typing import Dict, Any, Optional
from src.hotmart.auth import HotmartAuth

class HotmartClient:
    BASE_URL = "https://developers.hotmart.com/payments/api/v1"

    def __init__(self, auth: Optional[HotmartAuth] = None):
        self.auth = auth or HotmartAuth()

    def get_headers(self) -> Dict[str, str]:
        token = self.auth.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self.get_headers()
        
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
            
        response = requests.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        
        # Some Hotmart endpoints might return 204 No Content
        if response.status_code == 204:
            return {}
            
        return response.json()
