from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_each_timeframe_is_in_fixed_ten_year_source_plan() -> None:
    ranges=FullHistoryBackfillManager._simple_source_ranges(datetime(2026,7,27,22,0,tzinfo=UTC))
    assert {r.timeframe for r in ranges}=={'1m','1h','1d'}
