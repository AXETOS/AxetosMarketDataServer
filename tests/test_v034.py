from pathlib import Path

from axetos_market_data import __version__


def test_v034_release_metadata_and_ci_workflow() -> None:
    root = Path(__file__).resolve().parents[1]
    assert __version__ == "0.68.7"
    assert 'version = "0.68.7"' in (root / "pyproject.toml").read_text()
    readme = (root / "README.md").read_text()
    assert "## Version 0.68.7" in readme
    assert "ephemeral PostgreSQL 16 service" in readme

    workflow = (root / ".github" / "workflows" / "ci.yml").read_text()
    assert "postgres:16" in workflow
    assert "AXETOS_TEST_POSTGRES_URL" in workflow
    assert 'python-version: ["3.11", "3.12", "3.13"]' in workflow
    assert "tests/test_postgres_integration.py" in workflow
