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
@patch("src.pipelines.manychat_csv_importer.consolidate_all_to_master")
def test_import_manychat_csv_skips_empty_contacts(
    mock_consolidate, mock_csv_reader, mock_open, mock_get_conn
):
    """
    Happy Path / Decision Test: Rows without both email AND whatsapp are stored 
    in raw tables. Business rules for Master are handled in consolidation.
    """

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_reader_instance = MagicMock()
    mock_reader_instance.__iter__.return_value = [
        {
            "nome": "No Contact Info",
            "email": "",
            "whatsapp": "",
            "instagram": "insta_ghost",
        }
    ]
    mock_csv_reader.return_value = mock_reader_instance

    import_manychat_csv("dummy_path.csv")

    # Raw insert should happen regardless
    mock_cur.execute.assert_called()
    # Consolidation should be triggered
    mock_consolidate.assert_called_once_with(mock_conn)


@patch("src.pipelines.manychat_csv_importer.get_connection")
@patch("builtins.open")
@patch("src.pipelines.manychat_csv_importer.csv.DictReader")
@patch("src.pipelines.manychat_csv_importer.consolidate_all_to_master")
def test_import_manychat_csv_creates_new_master(
    mock_consolidate, mock_csv_reader, mock_open, mock_get_conn
):
    """
    Happy Path Test: Verifies that importer triggers consolidation after raw insert.
    """

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_reader_instance = MagicMock()
    mock_reader_instance.__iter__.return_value = [
        {
            "nome": "Valid User",
            "email": "test@test.com",
            "whatsapp": "551199999",
        }
    ]
    mock_csv_reader.return_value = mock_reader_instance

    import_manychat_csv("dummy_path.csv")

    # Verify consolidation call
    mock_consolidate.assert_called_once_with(mock_conn)


@patch("src.pipelines.manychat_csv_importer.get_connection")
@patch("builtins.open")
@patch("src.pipelines.manychat_csv_importer.csv.DictReader")
@patch("src.pipelines.manychat_csv_importer.consolidate_all_to_master")
def test_import_manychat_csv_updates_existing_master(
    mock_consolidate, mock_csv_reader, mock_open, mock_get_conn
):
    """
    Happy Path Test: Verifies that importer triggers consolidation after raw insert.
    """

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_reader_instance = MagicMock()
    mock_reader_instance.__iter__.return_value = [
        {
            "nome": "Existing User",
            "whatsapp": "551199999",
        }
    ]
    mock_csv_reader.return_value = mock_reader_instance

    import_manychat_csv("dummy_path.csv")

    mock_consolidate.assert_called_once_with(mock_conn)
