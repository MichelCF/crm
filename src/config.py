import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()

    # Database Paths
    DB_PRD = "data/db/prd/crm_prd.sqlite"
    DB_DEV = "data/db/dev/crm_dev.sqlite"
    DB_NAME = DB_PRD if ENVIRONMENT == "prd" else DB_DEV

    # Hotmart Sync Parameters
    HOTMART_START_DATE = os.getenv("HOTMART_START_DATE")
    HOTMART_END_DATE = os.getenv("HOTMART_END_DATE")

    # ManyChat Import Parameters
    MANYCHAT_INPUT_DIR = "data/input/manychat"
    MANYCHAT_CSV_OUTPUT = os.getenv("MANYCHAT_CSV_OUTPUT", "manychat_output.csv")

    # Output Paths
    OUTPUT_PUBLICO = "data/output/publico"
    OUTPUT_REMARKETING = "data/output/remarketing"

    @classmethod
    def is_prd(cls) -> bool:
        return cls.ENVIRONMENT == "prd"

    @classmethod
    def is_dev(cls) -> bool:
        return cls.ENVIRONMENT == "dev"
