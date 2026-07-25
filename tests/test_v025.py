from pathlib import Path

from axetos_market_data import __version__


ROOT = Path(__file__).resolve().parents[1]


def test_release_version_and_github_readme() -> None:
    assert __version__ == "0.33.1"
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "## Version 0.33.1" in readme
    assert "Docker deployment" in readme
    assert "excluded from release ZIPs and Git" not in readme


def test_docker_packaging_files_exist() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert "USER axetos" in dockerfile
    assert "HEALTHCHECK" in dockerfile
    assert "postgres:16-alpine" in compose
    assert "condition: service_healthy" in compose
    assert "AXETOS_DATABASE_URL" in compose
    assert "data" in dockerignore
    assert "POSTGRES_PASSWORD=" in env_example


def test_runtime_data_remains_ignored() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "data/" in gitignore
