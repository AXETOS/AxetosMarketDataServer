from __future__ import annotations

import argparse
import sqlite3


parser = argparse.ArgumentParser()
parser.add_argument("database")
args = parser.parse_args()

connection = sqlite3.connect(args.database)
for table in ("ticks", "candles"):
    count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count}")

print("\nLatest candles:")
for row in connection.execute(
    """
    SELECT provider, instrument, timeframe, open_time_utc, open, high, low, close, tick_count, complete
    FROM candles
    ORDER BY open_time_utc DESC
    LIMIT 20
    """
):
    print(row)
