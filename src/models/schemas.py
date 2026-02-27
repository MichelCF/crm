from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


class Customer(BaseModel):
    id: str
    email: EmailStr
    name: str
    phone: Optional[str] = None
    document: Optional[str] = Field(
        None, description="CPF, CNPJ or other identification document"
    )
    created_at: datetime
    updated_at: Optional[datetime] = None


class Product(BaseModel):
    id: int
    name: str


class Sale(BaseModel):
    transaction_id: str
    status: str
    total_price: float
    net_price: float
    currency: str = "BRL"
    payment_method: str
    purchased_at: datetime
    updated_at: Optional[datetime] = None

    # Foreign Keys (Logical representation)
    customer_id: str
    product_id: int


class HotmartSalesRequestParams(BaseModel):
    """
    Contract for requesting sales from Hotmart.
    Explicitly enforces that start_date and end_date (in ms) are provided
    to avoid accidental full-history/unfiltered API queries.
    """

    start_date: str
    end_date: str
    page_token: Optional[str] = None
