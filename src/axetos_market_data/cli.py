from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .aggregation import CandleAggregator
from .providers.mock import MockTickProvider
from .providers.mt5 import MetaTrader5TickProvider
from .service import MarketDataService
from .storage import MarketDataStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect market ticks and store OHLC candles.")
    parser.add_argument("--database", default="data/market_data.sqlite")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    mock = subparsers.add_parser("mock", help="Run the deterministic local demonstration provider.")
    mock.add_argument("--instrument", default="EUR/USD")
    mock.add_argument("--interval", type=float, default=1.0)

    mt5 = subparsers.add_parser("mt5", help="Collect live ticks from a local MetaTrader 5 terminal.")
    mt5.add_argument("--symbol", action="append", required=True)
    mt5.add_argument("--terminal-path")

    aggregate = subparsers.add_parser("aggregate", help="Build larger candles from stored 1m candles.")
    aggregate.add_argument("--provider", required=True)
    aggregate.add_argument("--instrument", required=True)
    aggregate.add_argument("--timeframe", required=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    store = MarketDataStore(Path(args.database))
    store.initialize()

    if args.command == "aggregate":
        written = CandleAggregator(store).aggregate(args.instrument, args.timeframe, args.provider)
        print(f"Wrote {written} {args.timeframe} candle(s).")
        return

    if args.command == "mock":
        provider = MockTickProvider(args.instrument, args.interval)
    elif args.command == "mt5":
        provider = MetaTrader5TickProvider(args.symbol, args.terminal_path)
    else:
        raise AssertionError(f"unhandled command: {args.command}")

    service = MarketDataService(store)
    try:
        service.run(provider.stream())
    except KeyboardInterrupt:
        service.builder.flush(complete=False)
        print("Stopped. Active candles were saved as incomplete.")


if __name__ == "__main__":
    main()
