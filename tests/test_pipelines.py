import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from src.pipelines.hotmart_to_db import (
    _date_str_to_ms,
    fetch_and_save_sales,
    do_initial_sync,
    do_incremental_sync,
    sync_sales_to_db,
)
from src.config import Config


@patch("src.pipelines.hotmart_to_db.get_sales_history")
@patch("src.pipelines.hotmart_to_db.upsert_customer")
@patch("src.pipelines.hotmart_to_db.upsert_product")
@patch("src.pipelines.hotmart_to_db.upsert_sale")
def test_fetch_and_save_sales_pagination(
    mock_upsert_sale, mock_upsert_product, mock_upsert_customer, mock_get_sales
):
    """Verifies that the loop fetches multiple pages when next_page_token is present."""
    # Setup mock responses for 2 pages
    page_1 = {
        "items": [
            {
                "transaction": "TX1",
                "status": "APPROVED",
                "purchase": {"price": {"value": 100}, "order_date": 1704067200000},
            }
        ],
        "page_info": {"next_page_token": "token_abc123"},
    }
    page_2 = {
        "items": [
            {
                "transaction": "TX2",
                "status": "COMPLETE",
                "purchase": {"price": {"value": 200}, "order_date": 1704067300000},
            }
        ],
        "page_info": {},  # No next page token, loop should stop here
    }

    mock_get_sales.side_effect = [page_1, page_2]
    mock_conn = MagicMock()

    # Act
    fetch_and_save_sales(mock_conn, start_date_ms="1000", end_date_ms="2000")

    # Assert API was called twice with correct pagination parameters
    assert mock_get_sales.call_count == 2
    mock_get_sales.assert_any_call(start_date="1000", end_date="2000")
    mock_get_sales.assert_any_call(
        start_date="1000", end_date="2000", page_token="token_abc123"
    )

    # Assert that Both TX1 and TX2 items were successfully upserted to the DB
    assert mock_upsert_sale.call_count == 2

    # Verify commit was called
    assert mock_conn.commit.call_count == 2


def test_date_str_to_ms():
    date_str = "2024-01-01"
    # Unix timestamp for 2024-01-01 UTC is approx 1704067200, depending on timezone.
    # We just need to check if it parses correctly.
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    expected_ms = str(int(dt.timestamp() * 1000))
    assert _date_str_to_ms(date_str) == expected_ms


@patch("src.pipelines.hotmart_to_db.fetch_and_save_sales")
def test_do_initial_sync(mock_fetch):
    Config.HOTMART_START_DATE = "2020-01-01"
    Config.HOTMART_END_DATE = "2022-12-31"  # A 3 year span, should create 2 chunks

    mock_conn = MagicMock()
    do_initial_sync(mock_conn)

    # Needs to be called twice
    assert mock_fetch.call_count == 2

    # First chunk expected
    expected_start_1 = _date_str_to_ms("2020-01-01")
    # Leap year so 730 days is 2021-12-31
    expected_end_1 = _date_str_to_ms("2021-12-31")

    # Second chunk expected
    expected_start_2 = _date_str_to_ms("2022-01-01")
    expected_end_2 = _date_str_to_ms("2022-12-31")

    mock_fetch.assert_any_call(mock_conn, expected_start_1, expected_end_1)
    mock_fetch.assert_any_call(mock_conn, expected_start_2, expected_end_2)


@patch("src.pipelines.hotmart_to_db.fetch_and_save_sales")
def test_do_initial_sync_missing_dates(mock_fetch):
    Config.HOTMART_START_DATE = None
    Config.HOTMART_END_DATE = None

    mock_conn = MagicMock()
    with pytest.raises(ValueError, match="must be provided in .env"):
        do_initial_sync(mock_conn)

    mock_fetch.assert_not_called()


@patch("src.pipelines.hotmart_to_db.datetime")
@patch("src.pipelines.hotmart_to_db.fetch_and_save_sales")
def test_do_incremental_sync(mock_fetch, mock_datetime):
    # Mock datetime.now() to a fixed date
    fixed_now = datetime(2024, 2, 10, 12, 0, 0)
    mock_datetime.now.return_value = fixed_now
    mock_datetime.fromisoformat = datetime.fromisoformat

    mock_conn = MagicMock()
    max_date_iso = "2024-02-05T15:30:00"

    do_incremental_sync(mock_conn, max_date_iso)

    # Calculate expected
    expected_max_date = datetime.fromisoformat(max_date_iso)
    expected_yesterday = fixed_now - timedelta(days=1)

    expected_start = str(int(expected_max_date.timestamp() * 1000))
    expected_end = str(int(expected_yesterday.timestamp() * 1000))

    mock_fetch.assert_called_once_with(mock_conn, expected_start, expected_end)


@patch("src.pipelines.hotmart_to_db.get_max_sale_date")
@patch("src.pipelines.hotmart_to_db.do_initial_sync")
@patch("src.pipelines.hotmart_to_db.do_incremental_sync")
@patch("src.pipelines.hotmart_to_db.init_db")
@patch("src.pipelines.hotmart_to_db.get_connection")
def test_sync_sales_to_db_empty(
    mock_get_conn, mock_init, mock_inc, mock_init_sync, mock_get_max
):
    mock_get_max.return_value = None  # DB is empty
    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_conn

    sync_sales_to_db()

    mock_init_sync.assert_called_once_with(mock_conn)
    mock_inc.assert_not_called()
    mock_conn.close.assert_called_once()


@patch("src.pipelines.hotmart_to_db.get_max_sale_date")
@patch("src.pipelines.hotmart_to_db.do_initial_sync")
@patch("src.pipelines.hotmart_to_db.do_incremental_sync")
@patch("src.pipelines.hotmart_to_db.init_db")
@patch("src.pipelines.hotmart_to_db.get_connection")
def test_sync_sales_to_db_incremental(
    mock_get_conn, mock_init, mock_inc, mock_init_sync, mock_get_max
):
    mock_get_max.return_value = "2024-01-01T10:00:00"  # DB has data
    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_conn

    sync_sales_to_db()

    mock_inc.assert_called_once_with(mock_conn, "2024-01-01T10:00:00")
    mock_init_sync.assert_not_called()
    mock_conn.close.assert_called_once()
