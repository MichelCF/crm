import schedule
import time
import sys
import os
from datetime import datetime

# Garante que o diretório raiz está no path para importar src
sys.path.append(os.getcwd())

from src.pipelines.hotmart_to_db import sync_sales_to_db
from src.pipelines.manychat_csv_importer import process_manychat_input_dir
from src.logic.audiences import (
    refresh_audiences,
    export_audiences_to_csv,
    generate_audience_report,
)
from src.logic.remarketing import (
    generate_remarketing_batch,
    generate_remarketing_report,
)
from src.db.database import get_connection
from src.config import Config


def run_daily_job():
    """
    Orchestrates the daily pipeline execution:
    1. Sync Hotmart sales
    2. Process all ManyChat CSVs in the input directory
    """
    print(f"[{datetime.now().isoformat()}] Starting daily scheduled job...")

    try:
        # 1. Hotmart Sync
        # Note: sync_sales_to_db handles its own connection and incremental logic
        print("--- Step 1: Hotmart Sync ---")
        sync_sales_to_db()

        # 2. ManyChat Import
        # Note: process_manychat_input_dir handles reading from data/input/manychat and cleanup
        print("\n--- Step 2: ManyChat Import ---")
        process_manychat_input_dir()

        # 3. Gold Audience Refresh
        print("\n--- Step 3: Refreshing Gold Audiences ---")
        with get_connection() as conn:
            refresh_audiences(conn)
            generate_audience_report(conn)
            export_audiences_to_csv(conn)

        # 4. Remarketing Generation (Gold)
        print("\n--- Step 4: Generating Remarketing Batch ---")
        with get_connection() as conn:
            generate_remarketing_batch(conn, limit=50)
            generate_remarketing_report(conn)

        print(f"\n[{datetime.now().isoformat()}] Daily job completed successfully.")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] CRITICAL: Daily job failed: {e}")


def main():
    print("--- CRM ORCHESTRATOR SERVER ---")
    print(f"Environment: {Config.ENVIRONMENT.upper()}")

    run_time = Config.get_schedule_time()
    print(f"Scheduled execution time: {run_time}")

    # Schedule the job
    schedule.every().day.at(run_time).do(run_daily_job)

    # Run once at startup for validation (optional)
    # run_daily_job()

    print(f"Server is running. Waiting for {run_time}...")

    while True:
        schedule.run_pending()
        time.sleep(1)  # Check every second for higher precision


if __name__ == "__main__":
    delay = Config.get_startup_delay()
    if delay > 0:
        print(
            f"Aguardando {delay} segundos para iniciar (Ambiente: {Config.ENVIRONMENT.upper()})..."
        )
        time.sleep(delay)

    # If passed '--now' arg, run immediately
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        run_daily_job()
    else:
        main()
