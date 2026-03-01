from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class Customer(BaseModel):
    id: str  # Note: Actually Hotmart has buyer id, but we usually use email or fallback
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    document: str | None = None
    # Endereço rico vindo do /sales/users
    zip_code: str | None = None
    address: str | None = None
    number: str | None = None
    neighborhood: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    created_at: datetime
    updated_at: Optional[datetime] = None


class Product(BaseModel):
    id: str
    name: str


class Sale(BaseModel):
    transaction: str
    status: str | None = None
    payment_method: str | None = None
    payment_type: str | None = None  # Pix, Cartão de Crédito, etc
    installments: int | None = None
    approved_date: int | None = None
    order_date: int | None = None
    total_price: float | None = None
    currency: str | None = None
    updated_at: Optional[datetime] = None

    # Foreign Keys (Logical representation)
    customer_id: str
    product_id: str


class HotmartSalesRequestParams(BaseModel):
    """
    Contract for requesting sales from Hotmart.
    Explicitly enforces that start_date and end_date (in ms) are provided
    to avoid accidental full-history/unfiltered API queries.
    """

    start_date: str
    end_date: str
    page_token: Optional[str] = None
