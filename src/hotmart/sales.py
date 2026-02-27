from src.hotmart.client import HotmartClient
from src.models.schemas import HotmartSalesRequestParams
from typing import Dict, Any

def get_sales_history(**kwargs) -> Dict[str, Any]:
    """Fetches the sales history from the Hotmart API."""
    # 1. Enforce the API Contract before touching the client
    contract = HotmartSalesRequestParams(**kwargs)
    
    client = HotmartClient()
    # The endpoint for sales history is typically '/sales/history'
    return client.get("/sales/history", params=contract.model_dump())
