import sys
import argparse
from datetime import datetime, timedelta
from src.hotmart.sales import get_sales_history
from src.models.schemas import Customer, Product, Sale
from src.db.database import get_connection, init_db, upsert_customer, upsert_product, upsert_sale, get_max_sale_date
from src.config import Config

def _date_str_to_ms(date_str: str) -> str:
    """Converts a YYYY-MM-DD string to unix time in milliseconds as required by Hotmart."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return str(int(dt.timestamp() * 1000))

def fetch_and_save_sales(conn, start_date_ms: str = None, end_date_ms: str = None):
    """Core function to fetch sales over a specific time period and save them to SQLite."""
    params = {}
    if start_date_ms:
        params['start_date'] = start_date_ms
    if end_date_ms:
        params['end_date'] = end_date_ms
        
    print(f"Fetching sales history with base params: {params}")
    success_count = 0
    page_count = 0
    has_next = True
    
    while has_next:
        page_count += 1
        try:
            print(f"Fetching page {page_count}...")
            response = get_sales_history(**params)
        except Exception as e:
            print(f"Failed to fetch data from Hotmart. Error: {e}")
            break

        items = response.get("items", [])
        page_info = response.get("page_info", {})
        print(f"Retrieved {len(items)} sales records in page {page_count}. Processing models...")
        
        # Determine if there's a next page and update params
        next_token = page_info.get("next_page_token")
        if next_token:
            params["page_token"] = next_token
        else:
            has_next = False
    
        # Process the page's items
        for item in items:
            try:
                # 1. Parse nested data
                txn_id = item.get("purchase", {}).get("transaction") or item.get("transaction")
                status = item.get("purchase", {}).get("status") or item.get("status", "UNKNOWN")
                
                # We want ALL statuses (STARTED, CANCELED, WAITING_PAYMENT, etc) for remarketing
                    
                payment_method = item.get("purchase", {}).get("payment", {}).get("type") or "UNKNOWN"
                
                # Hotmart provides original price and hotmart_fee (so net is often original - fee)
                # Or the user's specific commission. As a fallback we search common Hotmart keys
                purchase_data = item.get("purchase", {})
                total_price = purchase_data.get("price", {}).get("value") or getattr(purchase_data, "price", 0.0)
                
                # In some endpoints, commission/net is provided separately
                net_price = item.get("commission", {}).get("value") or total_price  # fallback if not available
                
                buyer_data = item.get("buyer", {})
                prod_data = item.get("product", {})
                
                # Map purchased_at to datetime
                purchased_at_raw = purchase_data.get("order_date")
                if purchased_at_raw:
                    # Hotmart date format: milliseconds or unix string
                    try:
                        purchased_at = datetime.fromtimestamp(int(purchased_at_raw)/1000.0)
                    except ValueError:
                        purchased_at = datetime.now() # Fallback
                else:
                    purchased_at = datetime.now()
                    
                updated_at_raw = purchase_data.get("approved_date")
                updated_at = None
                if updated_at_raw:
                    try:
                        updated_at = datetime.fromtimestamp(int(updated_at_raw)/1000.0)
                    except ValueError:
                        pass
                
                # Try to grab internal IDs and document/phone arrays
                buyer_id = str(buyer_data.get("ucode") or buyer_data.get("id") or txn_id)
                phone = buyer_data.get("phone") or purchase_data.get("phone")
                document = buyer_data.get("document") or purchase_data.get("document")
                
                # 2. Validate with Pydantic
                customer = Customer(
                    id=buyer_id,
                    email=buyer_data.get("email", f"unknown_{txn_id}@noemail.com"),
                    name=buyer_data.get("name", "Unknown Buyer"),
                    phone=str(phone) if phone else None,
                    document=str(document) if document else None,
                    created_at=purchased_at,
                    updated_at=updated_at
                )
                
                product = Product(
                    id=prod_data.get("id", 0),
                    name=prod_data.get("name", "Unknown Product")
                )
                
                sale = Sale(
                    transaction_id=txn_id,
                    status=status.upper(),
                    total_price=float(total_price or 0.0),
                    net_price=float(net_price or 0.0),
                    payment_method=payment_method,
                    purchased_at=purchased_at,
                    updated_at=updated_at,
                    customer_id=customer.id,
                    product_id=product.id
                )
                
                # 3. Save to database
                upsert_customer(conn, customer)
                upsert_product(conn, product)
                upsert_sale(conn, sale)
                
                success_count += 1
                
            except Exception as e:
                print(f"Skipping malformed or incomplete item {txn_id}: {e}")
                continue

            conn.commit()
    
    print(f"Successfully synced {success_count} total sales into the database over {page_count} pages.")

def get_date_chunks(start_dt: datetime, end_dt: datetime, max_days: int = 730) -> list[tuple[datetime, datetime]]:
    """Breaks a date range into smaller chunks of up to max_days to avoid API limits."""
    chunks = []
    current_start = start_dt
    while current_start <= end_dt:
        current_end = current_start + timedelta(days=max_days)
        if current_end > end_dt:
            current_end = end_dt
            
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    
    return chunks

def do_initial_sync(conn):
    """Scenario 1: The database is empty. Requires dates from .env config."""
    print("Scenario: Initial sync -> requiring dates from .env config.")
    
    if not Config.HOTMART_START_DATE or not Config.HOTMART_END_DATE:
        raise ValueError("HOTMART_START_DATE and HOTMART_END_DATE must be provided in .env for the initial sync.")
        
    start_dt = datetime.strptime(Config.HOTMART_START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(Config.HOTMART_END_DATE, "%Y-%m-%d")
    
    chunks = get_date_chunks(start_dt, end_dt, max_days=730)
    
    for current_start, current_end in chunks:
        print(f"Syncing chunk from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
        start_ms = str(int(current_start.timestamp() * 1000))
        end_ms = str(int(current_end.timestamp() * 1000))
        
        fetch_and_save_sales(conn, start_ms, end_ms)

def do_incremental_sync(conn, max_date_iso: str):
    """Scenario 2: The database has data. Sync from the last sale date up to yesterday."""
    max_date = datetime.fromisoformat(max_date_iso)
    yesterday = datetime.now() - timedelta(days=1)
    
    print(f"Scenario: Incremental sync -> fetching from last sale {max_date} up to yesterday {yesterday}.")
    start_ms = str(int(max_date.timestamp() * 1000))
    end_ms = str(int(yesterday.timestamp() * 1000))
    
    fetch_and_save_sales(conn, start_ms, end_ms)

def sync_sales_to_db():
    """
    Main orchestrator that determines the scenario and triggers the correct flow.
    """
    print("Starting Hotmart sync pipeline...")
    
    # Ensure database is ready
    conn = get_connection()
    init_db(conn)
    print("Database initialized.")

    max_date_iso = get_max_sale_date(conn)
    if not max_date_iso:
        do_initial_sync(conn)
    else:
        do_incremental_sync(conn, max_date_iso)
        
    conn.close()
    print("Finished pipeline.")

if __name__ == "__main__":
    sync_sales_to_db()
