from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from axetos_market_data.domain import Candle, Tick
from axetos_market_data.storage import MarketDataStore


POSTGRES_URL = os.getenv("AXETOS_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="AXETOS_TEST_POSTGRES_URL is required for PostgreSQL integration tests",
)


def test_postgres_round_trip_and_restart_initialization() -> None:
    assert POSTGRES_URL is not None
    store = MarketDataStore(POSTGRES_URL)
    store.initialize()

    now = datetime(2026, 7, 25, 0, 0, tzinfo=timezone.utc)
    tick = Tick(
        provider="Postgres.Integration",
        instrument="EUR/USD",
        timestamp=now,
        bid=Decimal("1.10000"),
        ask=Decimal("1.10002"),
    )
    candle = Candle(
        provider="Postgres.Integration",
        instrument="EUR/USD",
        timeframe="1m",
        open_time=now,
        open=Decimal("1.10001"),
        high=Decimal("1.10003"),
        low=Decimal("1.10000"),
        close=Decimal("1.10002"),
        tick_count=1,
    )

    assert store.insert_ticks([tick]) in {0, 1}
    store.upsert_candle(candle)
    event_id = store.record_operational_event(
        "info",
        "postgres.integration",
        "Postgres.Integration",
        "EUR/USD",
        "PostgreSQL integration round trip",
        now,
        '{"verified":true}',
    )
    assert event_id > 0

    with store.connect() as connection:
        tick_row = connection.execute(
            "SELECT provider,instrument,bid,ask FROM ticks WHERE provider=? AND instrument=?",
            ("Postgres.Integration", "EUR/USD"),
        ).fetchone()
        candle_row = connection.execute(
            "SELECT open,high,low,close,tick_count FROM candles "
            "WHERE provider=? AND instrument=? AND timeframe=? AND open_time_utc=?",
            ("Postgres.Integration", "EUR/USD", "1m", now.isoformat(timespec="microseconds")),
        ).fetchone()
        event_row = connection.execute(
            "SELECT category,message FROM operational_events WHERE id=?",
            (event_id,),
        ).fetchone()

    assert tick_row is not None
    assert tick_row[0] == "Postgres.Integration"
    assert tick_row[1] == "EUR/USD"
    assert candle_row is not None
    assert int(candle_row[4]) == 1
    assert event_row is not None
    assert event_row[0] == "postgres.integration"

    # Schema initialization must remain safe across service restarts.
    MarketDataStore(POSTGRES_URL).initialize()
