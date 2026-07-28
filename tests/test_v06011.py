from datetime import UTC, datetime
from axetos_market_data.full_history import FullHistoryBackfillManager

def test_fixed_ten_year_plan_replaces_discovery_tree():
    ranges=FullHistoryBackfillManager._simple_source_ranges(datetime(2026,7,27,tzinfo=UTC)); assert ranges[0].timeframe=='1m'; assert ranges[-1].timeframe=='1d'
