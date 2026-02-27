import os
from dotenv import load_dotenv

# Ensure we load the .env file variables into the system environment
load_dotenv()

class Config:
    """
    Central configuration class for the CRM.
    Reads from environment variables and sets up paths and keys accordingly.
    """
    # Defaults to 'dev' if not specified in .env
    ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").lower()
    
    # Example: database path changes based on environment
    DB_NAME = "crm_prd.sqlite" if ENVIRONMENT == "prd" else "crm_dev.sqlite"
    
    @classmethod
    def is_prd(cls) -> bool:
        return cls.ENVIRONMENT == "prd"
        
    @classmethod
    def is_dev(cls) -> bool:
        return cls.ENVIRONMENT == "dev"
