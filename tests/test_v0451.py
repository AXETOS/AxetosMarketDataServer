from axetos_market_data.benchmark_jobs import BenchmarkJobManager


def test_wait_for_completion_returns_terminal_job():
    manager = BenchmarkJobManager()
    manager.start(100, 2, [25])
    status = manager.wait_for_completion(timeout=30)
    assert status["status"] == "completed"
    assert status["job"]["completed_utc"] is not None


def test_wait_for_completion_is_safe_when_idle():
    assert BenchmarkJobManager().wait_for_completion(timeout=0) == {"status": "idle", "job": None}
