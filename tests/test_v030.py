from datetime import UTC, datetime

from fastapi.testclient import TestClient

from axetos_market_data.benchmark_jobs import BenchmarkJobManager
from axetos_market_data.benchmarks import IngestionBenchmark
from axetos_market_data.web import create_app


def test_benchmark_generation_is_minute_aligned_and_repeatable(tmp_path):
    first = IngestionBenchmark(ticks=1000, instruments=5, batch_size=100, database=str(tmp_path / "a.sqlite")).run()
    second = IngestionBenchmark(ticks=1000, instruments=5, batch_size=250, database=str(tmp_path / "b.sqlite")).run()
    assert first.candles_written == second.candles_written
    generated = next(IngestionBenchmark(ticks=1, instruments=1, batch_size=1)._generate_ticks())
    assert generated.timestamp == datetime(2020, 1, 1, tzinfo=UTC)


def test_benchmark_job_manager_runs_comparison():
    manager = BenchmarkJobManager()
    manager.start(1000, 5, [100, 250])
    status = manager.wait_for_completion(timeout=60)
    job = status["job"]
    assert job["status"] == "completed"
    assert len(job["results"]) == 2
    assert job["best_batch_size"] in {100, 250}


def test_management_ui_and_api_expose_benchmark(tmp_path):
    app = create_app(tmp_path / "market.sqlite", tmp_path / "providers.json")
    with TestClient(app) as client:
        html = client.get("/").text
        assert "Run benchmark" in html
        assert "Performance benchmark" in html
        response = client.post("/api/benchmarks/run", json={"ticks": 1000, "instruments": 5, "batch_sizes": [100]})
        assert response.status_code == 200
        assert response.json()["status"] in {"queued", "running"}
        assert client.get("/api/benchmarks/status").status_code == 200
        assert app.state.benchmark_jobs.wait_for_completion(timeout=60)["status"] == "completed"
