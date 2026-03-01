import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()

    # Database Paths (Dynamic by environment)
    @classmethod
    def get_db_path(cls) -> str:
        base_dir = f"data/db/{cls.ENVIRONMENT}"
        os.makedirs(base_dir, exist_ok=True)
        return os.path.join(base_dir, f"crm_{cls.ENVIRONMENT}.sqlite")

    @property
    def DB_NAME(self) -> str:
        # Compatibility for instances using Config().DB_NAME
        return self.get_db_path()

    @classmethod
    def recompute_db_path(cls):
        cls.DB_NAME = cls.get_db_path()

    # Hotmart Sync Parameters
    HOTMART_START_DATE = os.getenv("HOTMART_START_DATE")
    HOTMART_END_DATE = os.getenv("HOTMART_END_DATE")

    # ManyChat Import Parameters
    MANYCHAT_INPUT_DIR = "data/input/manychat"
    MANYCHAT_CSV_OUTPUT = os.getenv("MANYCHAT_CSV_OUTPUT", "manychat_output.csv")

    # Output Paths
    OUTPUT_PUBLICO = "data/output/publico"
    OUTPUT_REMARKETING = "data/output/remarketing"
    REPORTS_DIR = "data/reports"

    @classmethod
    def check_and_create_dirs(cls):
        """Ensures all environment-specific and output directories exist."""
        cls.recompute_db_path()
        dirs = [
            os.path.dirname(cls.DB_NAME),
            cls.MANYCHAT_INPUT_DIR,
            cls.OUTPUT_PUBLICO,
            cls.OUTPUT_REMARKETING,
            cls.REPORTS_DIR,
        ]
        for d in dirs:
            os.makedirs(d, exist_ok=True)

    @classmethod
    def get_schedule_time(cls) -> str:
        """Returns the time to run the daily job (HH:MM:SS format)."""
        env_time = os.getenv("SCHEDULE_TIME")
        if env_time:
            return env_time

        if cls.is_prd():
            return "00:00:00"

        # For non-prd (dev/hml), default to now + startup delay for testing
        from datetime import datetime, timedelta

        target_time = datetime.now() + timedelta(seconds=cls.get_startup_delay())
        return target_time.strftime("%H:%M:%S")

    @classmethod
    def get_startup_delay(cls) -> int:
        """Returns delay in seconds before starting the job."""
        return 10 if not cls.is_prd() else 0

    @classmethod
    def get_hotmart_date_range(cls):
        """
        Returns (start_dt, end_dt) based on environment:
        - dev: Yesterday
        - hml: Last Month
        - prd: None (Incremental)
        """
        from datetime import datetime, timedelta

        if cls.is_dev():
            yesterday = datetime.now() - timedelta(days=1)
            start_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            end_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999)
            return start_dt, end_dt

        if cls.ENVIRONMENT == "hml":
            today = datetime.now()
            first_day_this_month = today.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )
            last_day_last_month = first_day_this_month - timedelta(microseconds=1)
            first_day_last_month = last_day_last_month.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )
            return first_day_last_month, last_day_last_month

        # PRD: Default to None for incremental logic
        if cls.HOTMART_START_DATE and cls.HOTMART_END_DATE:
            return datetime.strptime(
                cls.HOTMART_START_DATE, "%Y-%m-%d"
            ), datetime.strptime(cls.HOTMART_END_DATE, "%Y-%m-%d")

        return None, None

    @classmethod
    def is_prd(cls) -> bool:
        return cls.ENVIRONMENT == "prd"

    @classmethod
    def is_dev(cls) -> bool:
        return cls.ENVIRONMENT == "dev"


# Initialize paths on module load
Config.recompute_db_path()
Config.check_and_create_dirs()
