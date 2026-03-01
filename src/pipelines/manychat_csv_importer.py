import csv
import argparse
from datetime import datetime, timedelta
from src.db.database import get_connection, consolidate_all_to_master
from src.config import Config


def excel_date_to_datetime(excel_date_str: str) -> str:
    """
    Converts an Excel serial date format (e.g. '46057,56185') to an ISO string.
    Excel's epoch is Dec 30, 1899.
    Returns empty string if invalid.
    """
    if not excel_date_str:
        return ""

    try:
        # ManyChat CSV exports in PT-BR might use comma for decimals
        clean_str = excel_date_str.replace(",", ".")
        serial = float(clean_str)

        # Excel date bug workaround: Excel thinks 1900 was a leap year.
        # Python datetime doesn't. So for dates > Mar 1 1900, we use Dec 30 1899 as epoch.
        epoch = datetime(1899, 12, 30)
        dt = epoch + timedelta(days=serial)
        return dt.isoformat()
    except Exception as e:
        print(f"Warning: Could not parse date '{excel_date_str}': {e}")
        return ""


def import_manychat_csv(file_path: str):
    """
    Reads a ManyChat CSV file (tab-separated) and imports it to the SQLite manychat_contacts table.
    Then, it triggers the engine to merge these into the master customers table.
    """
    conn = get_connection()
    cur = conn.cursor()

    print(f"Opening {file_path} for ManyChat import...")

    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            # Manychat exports often use tabs instead of commas
            reader = csv.DictReader(file, delimiter="\t")
            rows_imported = 0

            for row in reader:
                cur.execute(
                    """
                    INSERT INTO manychat_contacts (
                        nome, email, instagram, whatsapp, data_remarketing, 
                        agendamento, data_agendamento, contactar, data_contactar, 
                        ultima_interacao, data_registro
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        row.get("nome", "").strip(),
                        row.get("email", "").strip(),
                        row.get("instagram", "").strip(),
                        row.get("whatsapp", "").strip(),
                        excel_date_to_datetime(row.get("data_remarketing", "")),
                        row.get("agendamento", "").strip().upper(),
                        excel_date_to_datetime(row.get("data_agendamento", "")),
                        row.get("contactar", "").strip().upper(),
                        excel_date_to_datetime(row.get("data_contactar", "")),
                        excel_date_to_datetime(row.get("ultima_interacao", "")),
                        excel_date_to_datetime(row.get("data_registro", "")),
                    ),
                )
                rows_imported += 1

            conn.commit()
            print(f"Import complete! {rows_imported} rows added to manychat_contacts.")

            # Trigger consolidation
            print("Triggering Master consolidation...")
            consolidate_all_to_master(conn)
            print("Consolidation finished.")

    except FileNotFoundError:
        print(f"Error: Could not find the file '{file_path}'")
    except Exception as e:
        print(f"An error occurred during import: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import ManyChat CSV contacts to CRM.")
    parser.add_argument(
        "file_path",
        nargs="?",
        help="Path to the ManyChat CSV file (optional if set in .env)",
    )
    args = parser.parse_args()

    # Use CLI argument if provided, otherwise fallback to the environment variable (defaulting to data/ folder)
    target_file = args.file_path
    if not target_file:
        file_name = Config.MANYCHAT_CSV_OUTPUT
        # Ensure it looks in the data folder if no path is specified
        if not file_name.startswith("data/") and not file_name.startswith("/"):
            target_file = f"data/{file_name}"
        else:
            target_file = file_name

    print(f"Using ManyChat CSV file: {target_file}")
    import_manychat_csv(target_file)
