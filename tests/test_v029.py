import json

from axetos_market_data import __version__
from axetos_market_data.benchmarks import IngestionBenchmark, write_benchmark_result


def test_ingestion_benchmark_produces_repeatable_report(tmp_path):
    result = IngestionBenchmark(
        ticks=240,
        instruments=3,
        batch_size=40,
        database=str(tmp_path / "benchmark.sqlite"),
    ).run()
    assert result.ticks_requested == 240
    assert result.ticks_written == 240
    assert result.instruments == 3
    assert result.candles_written >= 3
    assert result.ticks_per_second > 0
    assert result.database_bytes > 0


def test_benchmark_result_can_be_written_as_json(tmp_path):
    result = IngestionBenchmark(ticks=30, instruments=1, batch_size=10).run()
    output = tmp_path / "result.json"
    write_benchmark_result(result, str(output))
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["ticks_written"] == 30
    assert payload["backend"] == "sqlite"


def test_release_029_metadata_readme_and_status_wording():
    assert __version__ == "0.41.0"
    readme = open("README.md", encoding="utf-8").read()
    web = open("src/axetos_market_data/web.py", encoding="utf-8").read()
    assert "## Version 0.41.0" in readme
    assert "ingestion benchmark" in readme.lower()
    assert "Connected providers" in web
