import os
import pytest
from importlib import reload
import src.config
from unittest.mock import patch

@pytest.fixture(autouse=True)
def mock_load_dotenv():
    with patch("dotenv.load_dotenv") as mock:
        yield mock

@pytest.fixture
def mock_env():
    # Save the original environment variable if it exists
    original_env = os.environ.get("ENVIRONMENT")
    yield
    # Restore it after the test
    if original_env is not None:
        os.environ["ENVIRONMENT"] = original_env
    else:
        os.environ.pop("ENVIRONMENT", None)
        
def test_config_defaults_to_dev_when_missing(mock_env):
    os.environ.pop("ENVIRONMENT", None)
    reload(src.config)
    assert src.config.Config.ENVIRONMENT == "dev"
    assert src.config.Config.is_dev() is True
    assert src.config.Config.is_prd() is False
    assert src.config.Config.DB_NAME == "crm_dev.sqlite"
        
def test_config_dev_environment(mock_env):
    os.environ["ENVIRONMENT"] = "dev"
    # Reload the module to force it to re-evaluate module level variables
    reload(src.config)
    assert src.config.Config.ENVIRONMENT == "dev"
    assert src.config.Config.is_dev() is True
    assert src.config.Config.is_prd() is False
    assert src.config.Config.DB_NAME == "crm_dev.sqlite"

def test_config_prd_environment(mock_env):
    os.environ["ENVIRONMENT"] = "prd"
    reload(src.config)
    assert src.config.Config.ENVIRONMENT == "prd"
    assert src.config.Config.is_dev() is False
    assert src.config.Config.is_prd() is True
    assert src.config.Config.DB_NAME == "crm_prd.sqlite"
