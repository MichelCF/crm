import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()
    
    DB_NAME = "crm_prd.sqlite" if ENVIRONMENT == "prd" else "crm_dev.sqlite"
    
    # Hotmart Sync Parameters
    HOTMART_START_DATE = os.getenv("HOTMART_START_DATE")
    HOTMART_END_DATE = os.getenv("HOTMART_END_DATE")
    
    @classmethod
    def is_prd(cls) -> bool:
        return cls.ENVIRONMENT == "prd"
        
    @classmethod
    def is_dev(cls) -> bool:
        return cls.ENVIRONMENT == "dev"
