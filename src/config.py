import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()

    # Ensure data directory paths are used
    DB_NAME = "data/crm_prd.sqlite" if ENVIRONMENT == "prd" else "data/crm_dev.sqlite"

    # Hotmart Sync Parameters
    HOTMART_START_DATE = os.getenv("HOTMART_START_DATE")
    HOTMART_END_DATE = os.getenv("HOTMART_END_DATE")

    # ManyChat Import Parameters
    MANYCHAT_CSV_OUTPUT = os.getenv("MANYCHAT_CSV_OUTPUT", "manychat_output.csv")

    @classmethod
    def is_prd(cls) -> bool:
        return cls.ENVIRONMENT == "prd"

    @classmethod
    def is_dev(cls) -> bool:
        return cls.ENVIRONMENT == "dev"
