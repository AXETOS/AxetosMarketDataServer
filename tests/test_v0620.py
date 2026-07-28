from __future__ import annotations

import os

from axetos_market_data.repair_worker import CandleRepairProcess


def test_candle_repair_runs_in_dedicated_process(tmp_path) -> None:
    worker = CandleRepairProcess(tmp_path / "repair.sqlite")
    try:
        result = worker.run("ICMarkets.MT5", ["EUR/USD"], timeout_seconds=30)
        assert result["worker_pid"] != os.getpid()
        assert result["summary"]["instruments"] == 1
        assert result["summary"]["stages"] >= 4
        assert worker.view()["completed_jobs"] == 1
    finally:
        worker.shutdown()
