import pytest
from datetime import datetime
from src.pipelines.hotmart_to_db import get_date_chunks

def test_get_date_chunks_under_max_days():
    """Test when the date range is smaller than the max_days."""
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 1, 10)
    
    chunks = get_date_chunks(start_dt, end_dt, max_days=30)
    
    assert len(chunks) == 1
    assert chunks[0][0] == datetime(2023, 1, 1)
    assert chunks[0][1] == datetime(2023, 1, 10)

def test_get_date_chunks_exact_max_days():
    """Test when the date range is exactly max_days."""
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 1, 31)
    
    chunks = get_date_chunks(start_dt, end_dt, max_days=30)
    
    assert len(chunks) == 1
    assert chunks[0][0] == datetime(2023, 1, 1)
    assert chunks[0][1] == datetime(2023, 1, 31)

def test_get_date_chunks_over_max_days():
    """Test when the date range requires multiple chunks."""
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 3, 15) # Roughly 73 days
    
    chunks = get_date_chunks(start_dt, end_dt, max_days=30)
    
    assert len(chunks) == 3
    
    # First chunk: Jan 1 + 30 days = Jan 31
    assert chunks[0][0] == datetime(2023, 1, 1)
    assert chunks[0][1] == datetime(2023, 1, 31)
    
    # Second chunk: Feb 1 + 30 days = Mar 3
    assert chunks[1][0] == datetime(2023, 2, 1)
    assert chunks[1][1] == datetime(2023, 3, 3)
    
    # Third chunk: Mar 4 up to Mar 15 (shortened)
    assert chunks[2][0] == datetime(2023, 3, 4)
    assert chunks[2][1] == datetime(2023, 3, 15)

def test_get_date_chunks_two_years():
    """Test with the default 730 days for a 3 year span."""
    start_dt = datetime(2020, 1, 1)
    end_dt = datetime(2022, 12, 31) 
    
    chunks = get_date_chunks(start_dt, end_dt) # Default is 730
    
    assert len(chunks) == 2
    assert chunks[0][0] == datetime(2020, 1, 1)
    # 2020 is leap year, so 2020-01-01 + 730 days is 2021-12-31
    assert chunks[0][1] == datetime(2021, 12, 31)
    
    # Next starts 2022-01-01
    assert chunks[1][0] == datetime(2022, 1, 1)
    assert chunks[1][1] == datetime(2022, 12, 31)
