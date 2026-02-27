import csv
import argparse
from datetime import datetime, timedelta
from src.db.database import get_connection
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
        clean_str = excel_date_str.replace(',', '.')
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
    
    success_count = 0
    skip_count = 0
    
    print(f"Opening {file_path} for ManyChat import...")
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            # Manychat exports often use tabs instead of commas
            reader = csv.DictReader(file, delimiter='\t')
            
            # Print columns found for debug
            print(f"Detected columns: {reader.fieldnames}")
            
            for row in reader:
                # 1. Clean data and map columns safely
                nome = row.get("nome", "").strip()
                email = row.get("email", "").strip()
                instagram = row.get("instagram", "").strip()
                whatsapp = row.get("whatsapp", "").strip()
                data_remarketing = excel_date_to_datetime(row.get("data_remarketing", ""))
                agendamento = row.get("agendamento", "").strip().upper()
                data_agendamento = excel_date_to_datetime(row.get("data_agendamento", ""))
                contactar = row.get("contactar", "").strip().upper()
                data_contactar = excel_date_to_datetime(row.get("data_contactar", ""))
                ultima_interacao = excel_date_to_datetime(row.get("ultima_interacao", ""))
                data_registro = excel_date_to_datetime(row.get("data_registro", ""))
                
                # 2. Insert into RAW ManyChat table (we insert ALL rows here, as requested)
                # We use a simple insert. If you need to avoid duplicates in the raw table,
                # you might need a unique constraint on some combination of fields.
                # Since CSVs might be re-uploaded, let's keep it as an append-only log for now,
                # or you can write UPSERT logic if you have ManyChat IDs.
                cur.execute('''
                    INSERT INTO manychat_contacts (
                        nome, email, instagram, whatsapp, data_remarketing, 
                        agendamento, data_agendamento, contactar, data_contactar, 
                        ultima_interacao, data_registro
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    nome, email, instagram, whatsapp, data_remarketing,
                    agendamento, data_agendamento, contactar, data_contactar,
                    ultima_interacao, data_registro
                ))
                
                # 3. Apply the critical business rule for the MASTER merge
                # Only process if it has either Email or WhatsApp
                if not email and not whatsapp:
                    skip_count += 1
                    continue
                    
                # 4. Upsert to Master Customers Table
                # First, check if a master record exists with this email or whatsapp
                master_id = None
                
                if email:
                    cur.execute("SELECT id FROM customers WHERE master_email = ?", (email,))
                    res = cur.fetchone()
                    if res:
                        master_id = res['id']
                
                if not master_id and whatsapp:
                    cur.execute("SELECT id FROM customers WHERE master_phone = ?", (whatsapp,))
                    res = cur.fetchone()
                    if res:
                        master_id = res['id']
                
                if master_id:
                    # Update existing master record with ManyChat data
                    cur.execute('''
                        UPDATE customers 
                        SET name = COALESCE(name, ?),
                            instagram = COALESCE(instagram, ?)
                        WHERE id = ?
                    ''', (nome if nome else None, instagram if instagram else None, master_id))
                else:
                    # Create new master record originating from Manychat
                    cur.execute('''
                        INSERT INTO customers (master_email, master_phone, name, instagram)
                        VALUES (?, ?, ?, ?)
                    ''', (email if email else None, whatsapp if whatsapp else None, nome, instagram))
                
                success_count += 1
                
            conn.commit()
            print(f"Import complete! Successfully merged {success_count} valid contacts into the Master DB.")
            print(f"Skipped {skip_count} contacts that lacked both Email and WhatsApp.")
        
    except FileNotFoundError:
        print(f"Error: Could not find the file '{file_path}'")
    except Exception as e:
        print(f"An error occurred during import: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import ManyChat CSV contacts to CRM.")
    parser.add_argument("file_path", nargs="?", help="Path to the ManyChat CSV file (optional if set in .env)")
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
