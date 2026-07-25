from __future__ import annotations

import argparse
import logging
from .aggregation import CandleAggregator
from .providers.mock import MockTickProvider
from .providers.mt5 import MetaTrader5TickProvider
from .service import MarketDataService
from .storage import MarketDataStore
from .backups import BackupError, BackupService
from .benchmarks import IngestionBenchmark, write_benchmark_result
from .endurance import EnduranceRunner, write_endurance_result


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

    backup = subparsers.add_parser("backup", help="Create and verify a portable SQLite backup archive.")
    backup.add_argument("--output")
    backup.add_argument("--configuration", default="data/providers.json")
    backup.add_argument("--backup-directory", default="data/backups")

    verify = subparsers.add_parser("verify-backup", help="Verify checksums and SQLite integrity.")
    verify.add_argument("archive")

    restore = subparsers.add_parser("restore", help="Restore a verified SQLite backup archive.")
    restore.add_argument("archive")
    restore.add_argument("--configuration", default="data/providers.json")
    restore.add_argument("--overwrite", action="store_true")

    benchmark = subparsers.add_parser("benchmark", help="Run a deterministic ingestion benchmark.")
    benchmark.add_argument("--ticks", type=int, default=100_000)
    benchmark.add_argument("--instruments", type=int, default=10)
    benchmark.add_argument("--batch-size", type=int, default=1_000)
    benchmark.add_argument("--output")
    benchmark.add_argument("--keep-database", action="store_true")

    endurance = subparsers.add_parser("endurance", help="Run repeated benchmark cycles for sustained-performance checks.")
    endurance.add_argument("--duration-seconds", type=float, default=300.0)
    endurance.add_argument("--ticks-per-cycle", type=int, default=100_000)
    endurance.add_argument("--instruments", type=int, default=10)
    endurance.add_argument("--batch-size", type=int, default=5_000)
    endurance.add_argument("--regression-threshold-percent", type=float, default=20.0)
    endurance.add_argument("--output")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.command == "endurance":
        result = EnduranceRunner(
            duration_seconds=args.duration_seconds,
            ticks_per_cycle=args.ticks_per_cycle,
            instruments=args.instruments,
            batch_size=args.batch_size,
            regression_threshold_percent=args.regression_threshold_percent,
        ).run()
        write_endurance_result(result, args.output)
        if result.regression_detected:
            raise SystemExit(2)
        return

    if args.command == "benchmark":
        database = args.database if args.keep_database else None
        result = IngestionBenchmark(
            ticks=args.ticks,
            instruments=args.instruments,
            batch_size=args.batch_size,
            database=database,
        ).run()
        write_benchmark_result(result, args.output)
        return

    if args.command in {"backup", "verify-backup", "restore"}:
        service = BackupService(
            args.database,
            getattr(args, "configuration", "data/providers.json"),
            getattr(args, "backup_directory", "data/backups"),
        )
        try:
            if args.command == "backup":
                result = service.create(args.output)
            elif args.command == "verify-backup":
                result = service.verify(args.archive)
            else:
                result = service.restore(args.archive, overwrite=args.overwrite)
        except BackupError as exc:
            raise SystemExit(str(exc)) from exc
        import json
        print(json.dumps(result, indent=2))
        return

    store = MarketDataStore(args.database)
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
