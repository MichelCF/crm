from unittest.mock import patch, MagicMock
from src.pipelines.manychat_csv_importer import (
    excel_date_to_datetime,
    import_manychat_csv,
)


def test_excel_date_to_datetime_empty():
    assert excel_date_to_datetime("") == ""
    assert excel_date_to_datetime(None) == ""


def test_excel_date_to_datetime_invalid():
    assert excel_date_to_datetime("not a number") == ""


def test_excel_date_to_datetime_valid_dot():
    # 46057.56185 (Float with dot)
    result = excel_date_to_datetime("46057.56185")
    assert result.startswith("2026-02-04")


def test_excel_date_to_datetime_valid_comma():
    # 46057,56185 (Float with comma, common in pt-BR Excel exports)
    result = excel_date_to_datetime("46057,56185")
    assert result.startswith("2026-02-04")


@patch("src.pipelines.manychat_csv_importer.get_connection")
@patch("builtins.open")
@patch("src.pipelines.manychat_csv_importer.csv.DictReader")
def test_import_manychat_csv_skips_empty_contacts(
    mock_csv_reader, mock_open, mock_get_conn
):
    """Test that rows without both email and whatsapp are inserted into raw but skipped from master."""

    # Mocking DB Setup
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    # Mocking CSV with one invalid contact (no email, no phone)
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

    # Assertions
    # 1. It must insert into manychat_contacts (raw)
    insert_raw_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "INSERT INTO manychat_contacts" in call[0][0]
    ]
    assert len(insert_raw_calls) == 1

    # 2. It must NOT query or insert into customers (master)
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
    """Test that a valid row with an email that doesn't exist creates a new master customer."""

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    # Simulating that SELECT id FROM customers returns None (User does not exist)
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

    # It must select to check existence
    select_email_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "SELECT id FROM customers WHERE master_email" in call[0][0]
    ]
    assert len(select_email_calls) == 1

    # It must insert into new master
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
    """Test that a valid row with an existing email updates the master customer."""

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    # Simulating that user exists (Returns id '123')
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

    # It must select to check existence by phone (since email is empty)
    select_phone_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "SELECT id FROM customers WHERE master_phone" in call[0][0]
    ]
    assert len(select_phone_calls) == 1

    # It must UPDATE the master, not insert
    update_master_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "UPDATE customers" in call[0][0]
    ]
    assert len(update_master_calls) == 1

    # Ensure it updated the specific ID 123
    assert update_master_calls[0][0][1][2] == "123"
