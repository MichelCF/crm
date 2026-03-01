from unittest.mock import patch, MagicMock
from hypothesis import given, strategies as st
from src.pipelines.manychat_csv_importer import (
    excel_date_to_datetime,
    import_manychat_csv,
)

# =====================================================================
# BOUNDARY & PROPERTY-BASED TESTS
# =====================================================================


def test_excel_date_to_datetime_empty():
    """
    Boundary Test: Verifies that empty or None inputs return an empty string.
    """
    assert excel_date_to_datetime("") == ""
    assert excel_date_to_datetime(None) == ""


def test_excel_date_to_datetime_invalid():
    """
    Boundary Test: Verifies that non-numeric strings return an empty string.
    """
    assert excel_date_to_datetime("not a number") == ""


@given(st.floats(min_value=1.0, max_value=100000.0))
def test_excel_date_to_datetime_property(excel_val):
    """
    Property-Based Test: Verifies that any valid float (Excel date)
    converts to a string starting with an ISO date format (YYYY-MM-DD).
    """
    result = excel_date_to_datetime(str(excel_val))
    if result:  # Might be empty for edge cases if any
        # Check format YYYY-MM-DD...
        assert len(result) >= 10
        assert result[4] == "-"
        assert result[7] == "-"


def test_excel_date_to_datetime_valid_comma():
    """
    Boundary Test: Verifies support for Brazilian decimal separator (comma).
    46057,56185 should be equivalent to 46057.56185
    """
    result = excel_date_to_datetime("46057,56185")
    assert result.startswith("2026-02-04")


# =====================================================================
# INTEGRATION & MOCK TESTS (MC/DC logic)
# =====================================================================


@patch("src.pipelines.manychat_csv_importer.get_connection")
@patch("builtins.open")
@patch("src.pipelines.manychat_csv_importer.csv.DictReader")
def test_import_manychat_csv_skips_empty_contacts(
    mock_csv_reader, mock_open, mock_get_conn
):
    """
    Happy Path / Decision Test: Rows without both email AND whatsapp should be
    stored in raw tables but ignored for master customer records.

    Logic: (HasEmail OR HasWhatsapp) -> Process to Master.
    MC/DC:
    A (Email) | B (Whatsapp) | Master Processed?
    --------------------------------------------
    False     | False        | False (This test)
    True      | False        | True  (test_import_manychat_csv_creates_new_master)
    False     | True         | True  (test_import_manychat_csv_updates_existing_master)
    """

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_reader_instance = MagicMock()
    mock_reader_instance.fieldnames = ["nome", "email", "whatsapp", "instagram"]
    mock_reader_instance.__iter__.return_value = [
        {
            "nome": "No Contact Info",
            "email": "",
            "whatsapp": "",
            "instagram": "insta_ghost",
            "data_remarketing": "",
            "agendamento": "",
            "data_agendamento": "",
            "contactar": "",
            "data_contactar": "",
            "ultima_interacao": "",
            "data_registro": "",
        }
    ]
    mock_csv_reader.return_value = mock_reader_instance

    import_manychat_csv("dummy_path.csv")

    # Raw insert should happen regardless
    insert_raw_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "INSERT INTO manychat_contacts" in call[0][0]
    ]
    assert len(insert_raw_calls) == 1

    # Master table (customers) should NOT be touched
    query_master_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "customers" in call[0][0].lower()
    ]
    assert len(query_master_calls) == 0


@patch("src.pipelines.manychat_csv_importer.get_connection")
@patch("builtins.open")
@patch("src.pipelines.manychat_csv_importer.csv.DictReader")
def test_import_manychat_csv_creates_new_master(
    mock_csv_reader, mock_open, mock_get_conn
):
    """
    Happy Path Test: A valid row with a new email creates a new master customer.
    (Case A=True, B=False for MC/DC)
    """

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_cur.fetchone.return_value = None

    mock_reader_instance = MagicMock()
    mock_reader_instance.fieldnames = ["nome", "email", "whatsapp", "instagram"]
    mock_reader_instance.__iter__.return_value = [
        {
            "nome": "Valid User",
            "email": "test@test.com",
            "whatsapp": "",
            "instagram": "",
            "data_remarketing": "",
            "agendamento": "",
            "data_agendamento": "",
            "contactar": "",
            "data_contactar": "",
            "ultima_interacao": "",
            "data_registro": "",
        }
    ]
    mock_csv_reader.return_value = mock_reader_instance

    import_manychat_csv("dummy_path.csv")

    insert_master_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "INSERT INTO customers" in call[0][0]
    ]
    assert len(insert_master_calls) == 1


@patch("src.pipelines.manychat_csv_importer.get_connection")
@patch("builtins.open")
@patch("src.pipelines.manychat_csv_importer.csv.DictReader")
def test_import_manychat_csv_updates_existing_master(
    mock_csv_reader, mock_open, mock_get_conn
):
    """
    Happy Path Test: A valid row with an existing phone updates the master customer.
    (Case A=False, B=True for MC/DC)
    """

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_cur.fetchone.return_value = {"id": "123"}

    mock_reader_instance = MagicMock()
    mock_reader_instance.fieldnames = ["nome", "email", "whatsapp", "instagram"]
    mock_reader_instance.__iter__.return_value = [
        {
            "nome": "Existing User",
            "email": "",
            "whatsapp": "551199999",
            "instagram": "new_insta",
            "data_remarketing": "",
            "agendamento": "",
            "data_agendamento": "",
            "contactar": "",
            "data_contactar": "",
            "ultima_interacao": "",
            "data_registro": "",
        }
    ]
    mock_csv_reader.return_value = mock_reader_instance

    import_manychat_csv("dummy_path.csv")

    update_master_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "UPDATE customers" in call[0][0]
    ]
    assert len(update_master_calls) == 1
    assert update_master_calls[0][0][1][2] == "123"
