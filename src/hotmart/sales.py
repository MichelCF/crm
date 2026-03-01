from src.hotmart.client import HotmartClient
from src.models.schemas import HotmartSalesRequestParams
from typing import Dict, Any, Optional


def get_sales_history(
    client: Optional[HotmartClient] = None, **kwargs
) -> Dict[str, Any]:
    """Fetches the sales history from the Hotmart API."""
    # 1. Enforce the API Contract before touching the client
    contract = HotmartSalesRequestParams(**kwargs)

    if client is None:
        client = HotmartClient()
    # The endpoint for sales history is typically '/sales/history'
    return client.get("/sales/history", params=contract.model_dump())


def get_sale_users(
    transaction: str, client: Optional[HotmartClient] = None
) -> Dict[str, Any]:
    """Fetches user details for a specific transaction (returns address, phone, etc)."""
    if client is None:
        client = HotmartClient()
    return client.get("/sales/users", params={"transaction": transaction})


def get_sale_price_details(
    transaction: str, client: Optional[HotmartClient] = None
) -> Dict[str, Any]:
    """Fetches price details including payment method and installments."""
    if client is None:
        client = HotmartClient()
    return client.get("/sales/price/details", params={"transaction": transaction})
