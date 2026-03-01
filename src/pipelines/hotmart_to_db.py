from datetime import datetime, timedelta
from typing import Optional
from src.hotmart.sales import get_sales_history, get_sale_users, get_sale_price_details
from src.hotmart.client import HotmartClient
from src.models.schemas import Customer, Product, Sale
from src.db.database import (
    get_connection,
    init_db,
    upsert_customer,
    upsert_product,
    upsert_sale,
    get_max_sale_date,
    consolidate_all_to_master,
)
from src.logic.reporting import generate_delta_report
from src.config import Config


def _date_str_to_ms(date_str: str) -> str:
    """Converts a YYYY-MM-DD string to unix time in milliseconds as required by Hotmart."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return str(int(dt.timestamp() * 1000))


def fetch_and_save_sales(
    conn,
    start_date_ms: str = None,
    end_date_ms: str = None,
    page_token: str = None,
    client: Optional[HotmartClient] = None,
    imported_at: str = None,
) -> int:
    """Core function to fetch sales over a specific time period and save them to SQLite."""
    if client is None:
        client = HotmartClient()
    params = {}
    if start_date_ms:
        params["start_date"] = start_date_ms
    if end_date_ms:
        params["end_date"] = end_date_ms
    if page_token:
        params["page_token"] = page_token

    print(f"Fetching sales history with base params: {params}")
    success_count = 0
    page_count = 0
    has_next = True

    while has_next:
        page_count += 1
        try:
            print(f"Fetching page {page_count}...")
            response = get_sales_history(client=client, **params)
        except Exception as e:
            print(f"Failed to fetch data from Hotmart. Error: {e}")
            break

        items = response.get("items", [])
        page_info = response.get("page_info", {})
        print(
            f"Retrieved {len(items)} sales records in page {page_count}. Processing models..."
        )

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
                txn_id = item.get("purchase", {}).get("transaction") or item.get(
                    "transaction"
                )
                status = item.get("purchase", {}).get("status") or item.get(
                    "status", "UNKNOWN"
                )

                # We want ALL statuses (STARTED, CANCELED, WAITING_PAYMENT, etc) for remarketing

                payment_method = (
                    item.get("purchase", {}).get("payment", {}).get("type") or "UNKNOWN"
                )

                # Hotmart provides original price and hotmart_fee (so net is often original - fee)
                # Or the user's specific commission. As a fallback we search common Hotmart keys
                purchase_data = item.get("purchase", {})
                total_price = purchase_data.get("price", {}).get("value") or getattr(
                    purchase_data, "price", 0.0
                )

                buyer_data = item.get("buyer", {})
                prod_data = item.get("product", {})

                # Map purchased_at to datetime
                purchased_at_raw = purchase_data.get("order_date")
                if purchased_at_raw:
                    # Hotmart date format: milliseconds or unix string
                    try:
                        purchased_at = datetime.fromtimestamp(
                            int(purchased_at_raw) / 1000.0
                        )
                    except ValueError:
                        purchased_at = datetime.now()  # Fallback
                else:
                    purchased_at = datetime.now()

                updated_at_raw = purchase_data.get("approved_date")
                updated_at = None
                if updated_at_raw:
                    try:
                        updated_at = datetime.fromtimestamp(
                            int(updated_at_raw) / 1000.0
                        )
                    except ValueError:
                        pass

                # Try to grab internal IDs and document/phone arrays
                buyer_id = str(
                    buyer_data.get("ucode") or buyer_data.get("id") or txn_id
                )
                phone_fallback = buyer_data.get("phone") or purchase_data.get("phone")
                document = buyer_data.get("document") or purchase_data.get("document")

                # Enrichment with secondary APIs
                user_detail = {}
                try:
                    users_meta = get_sale_users(txn_id, client=client)
                    users_list = users_meta.get("users", [])
                    if users_list:
                        # Find the buyer
                        for user in users_list:
                            if (
                                user.get("role") == "BUYer"
                                or user.get("role") == "BUYER"
                            ):
                                user_detail = user.get("user", {})
                                break
                        if not user_detail:
                            user_detail = users_list[0].get("user", {})
                except Exception as e:
                    print(f"User enrichment failed for {txn_id}: {e}")

                price_detail = {}
                try:
                    price_detail = get_sale_price_details(txn_id, client=client)
                except Exception as e:
                    print(f"Price enrichment failed for {txn_id}: {e}")

                user_address = user_detail.get("address", {})
                phone_rich = user_detail.get("phone") or phone_fallback

                # 2. Validate with Pydantic
                customer = Customer(
                    id=buyer_id,
                    email=buyer_data.get("email")
                    or user_detail.get("email")
                    or f"unknown_{txn_id}@noemail.com",
                    name=buyer_data.get("name")
                    or user_detail.get("name")
                    or "Unknown Buyer",
                    phone=str(phone_rich) if phone_rich else None,
                    document=str(document) if document else None,
                    zip_code=user_address.get("zip_code"),
                    address=user_address.get("address"),
                    number=user_address.get("number"),
                    neighborhood=user_address.get("neighborhood"),
                    city=user_address.get("city"),
                    state=user_address.get("state"),
                    country=user_address.get("country"),
                    created_at=purchased_at,
                    updated_at=updated_at,
                )

                product = Product(
                    id=str(prod_data.get("id", "0")),
                    name=prod_data.get("name", "Unknown Product"),
                )

                # Payment Details Extraction
                payment_type = None
                installments = None

                payment_meta = price_detail.get("payment")
                if payment_meta:
                    payment_type = payment_meta.get("type") or payment_method
                    installments = payment_meta.get("installments_number")

                sale = Sale(
                    transaction=txn_id,
                    status=status.upper(),
                    total_price=float(total_price or 0.0),
                    currency=purchase_data.get("currency", "BRL"),
                    payment_method=payment_method,
                    payment_type=payment_type,
                    installments=installments,
                    approved_date=int(updated_at_raw) if updated_at_raw else None,
                    order_date=int(purchased_at_raw) if purchased_at_raw else None,
                    purchased_at=purchased_at,
                    updated_at=updated_at,
                    customer_id=customer.id,
                    product_id=str(product.id),
                )

                # 3. Save to database
                upsert_customer(conn, customer)
                upsert_product(conn, product)
                upsert_sale(conn, sale, imported_at=imported_at)

                success_count += 1

            except Exception as e:
                print(f"Skipping malformed or incomplete item {txn_id}: {e}")
                continue

            conn.commit()

    print(
        f"Successfully synced {success_count} total sales into the database over {page_count} pages."
    )
    return success_count


def get_date_chunks(
    start_dt: datetime, end_dt: datetime, max_days: int = 730
) -> list[tuple[datetime, datetime]]:
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


def do_initial_sync(conn, client: HotmartClient = None, imported_at: str = None):
    """Scenario 1: The database is empty. Requires dates from .env config."""
    print("Scenario: Initial sync -> requiring dates from .env config.")

    if not Config.HOTMART_START_DATE or not Config.HOTMART_END_DATE:
        raise ValueError(
            "HOTMART_START_DATE and HOTMART_END_DATE must be provided in .env for the initial sync."
        )

    if Config.is_dev():
        yesterday = datetime.now() - timedelta(days=1)
        start_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999)
        print(f"DEV MODE: Overriding dates to yesterday only: {start_dt} to {end_dt}")
    else:
        start_dt = datetime.strptime(Config.HOTMART_START_DATE, "%Y-%m-%d")
        end_dt = datetime.strptime(Config.HOTMART_END_DATE, "%Y-%m-%d")

    client = client or HotmartClient()
    chunks = get_date_chunks(start_dt, end_dt, max_days=730)

    for current_start, current_end in chunks:
        print(
            f"Syncing chunk from {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}"
        )
        start_ms = str(int(current_start.timestamp() * 1000))
        end_ms = str(int(current_end.timestamp() * 1000))

        fetch_and_save_sales(
            conn, start_ms, end_ms, client=client, imported_at=imported_at
        )


def do_incremental_sync(
    conn, max_date_iso: str, client: HotmartClient = None, imported_at: str = None
):
    """Scenario 2: The database has data. Sync from the last sale date up to yesterday."""
    yesterday = datetime.now() - timedelta(days=1)

    if Config.is_dev():
        max_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999)
        print(f"DEV MODE: Syncing only yesterday: {max_date} to {end_date}")
    else:
        max_date = datetime.fromisoformat(max_date_iso)
        end_date = yesterday

    print(f"Scenario: Incremental sync -> fetching from {max_date} up to {end_date}.")
    start_ms = str(int(max_date.timestamp() * 1000))
    end_ms = str(int(end_date.timestamp() * 1000))

    client = client or HotmartClient()
    fetch_and_save_sales(conn, start_ms, end_ms, client=client, imported_at=imported_at)


def sync_sales_to_db():
    """
    Main orchestrator that determines the scenario and triggers the correct flow.
    """
    print("Starting Hotmart sync pipeline...")

    # Ensure database is ready
    conn = get_connection()
    init_db(conn)
    print("Database initialized.")

    # Generate a unique timestamp for this run (for reporting deltas)
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    max_date = get_max_sale_date(conn)

    if not max_date:
        do_initial_sync(conn, imported_at=run_timestamp)
    else:
        do_incremental_sync(conn, max_date, imported_at=run_timestamp)

    consolidate_all_to_master(conn)

    print("\nGenerating status report...")
    generate_delta_report(conn)

    conn.close()
    print("Hotmart sync completed.")


if __name__ == "__main__":
    sync_sales_to_db()
