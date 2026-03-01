from datetime import datetime, timedelta
from hypothesis import given, strategies as st
from src.pipelines.hotmart_to_db import get_date_chunks

# =====================================================================
# BOUNDARY TESTS
# =====================================================================


def test_get_date_chunks_under_max_days():
    """
    Boundary Test: Date range strictly smaller than the max_days.
    Should return exactly 1 chunk spanning the requested interval.
    """
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 1, 10)

    chunks = get_date_chunks(start_dt, end_dt, max_days=30)

    assert len(chunks) == 1
    assert chunks[0][0] == datetime(2023, 1, 1)
    assert chunks[0][1] == datetime(2023, 1, 10)


def test_get_date_chunks_exact_max_days():
    """
    Boundary Test: Date range exactly matches max_days.
    Should return exactly 1 chunk without overflowing into a second one.
    """
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 1, 31)

    chunks = get_date_chunks(start_dt, end_dt, max_days=30)

    assert len(chunks) == 1
    assert chunks[0][0] == datetime(2023, 1, 1)
    assert chunks[0][1] == datetime(2023, 1, 31)


def test_get_date_chunks_over_max_days():
    """
    Boundary Test: Date range slightly exceeds max_days.
    Should predictably slice the dates into consecutive chunks.
    """
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 3, 15)  # Roughly 73 days

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


def test_get_date_chunks_same_day():
    """
    Boundary Test: Start and end date are exactly the same day.
    """
    start_dt = datetime(2020, 1, 1)
    end_dt = datetime(2020, 1, 1)

    chunks = get_date_chunks(start_dt, end_dt)

    assert len(chunks) == 1
    assert chunks[0][0] == datetime(2020, 1, 1)
    assert chunks[0][1] == datetime(2020, 1, 1)


# =====================================================================
# PROPERTY-BASED TESTS
# =====================================================================


# Strategy: Start date between 2000 and 2050, duration up to 2000 days, max_days up to 100
@given(
    start_dt=st.datetimes(
        min_value=datetime(2000, 1, 1), max_value=datetime(2050, 1, 1)
    ),
    delta_days=st.integers(min_value=0, max_value=2000),
    max_days=st.integers(min_value=1, max_value=100),
)
def test_date_chunks_properties(start_dt, delta_days, max_days):
    """
    Property-Based Test: Ensures properties scale mathematically across thousands of combinations.
    - Total chunks strictly adhere to max_days limit.
    - Dates within chunks do not overlap.
    - Contiguous end-to-start sequence preserves the exact overall duration.
    """
    end_dt = start_dt + timedelta(days=delta_days)
    chunks = get_date_chunks(start_dt, end_dt, max_days=max_days)

    # Property 1: Must yield at least 1 chunk
    assert len(chunks) >= 1

    # Property 2: First chunk starts at start_dt
    assert chunks[0][0] == start_dt

    # Property 3: Last chunk ends at end_dt
    assert chunks[-1][1] == end_dt

    # Property 4: No chunk spans more than max_days
    for c_start, c_end in chunks:
        assert (c_end - c_start).days <= max_days
        assert c_start <= c_end

    # Property 5: Chunks are contiguous without gaps (c2_start = c1_end + 1 day)
    for i in range(1, len(chunks)):
        prev_end = chunks[i - 1][1]
        curr_start = chunks[i][0]
        assert curr_start == prev_end + timedelta(days=1)
