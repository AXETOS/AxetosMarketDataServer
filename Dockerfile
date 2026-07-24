FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    AXETOS_DATABASE_URL=postgresql://axetos:axetos@postgres:5432/axetos_market_data

WORKDIR /app

RUN groupadd --system axetos && useradd --system --gid axetos --home-dir /app axetos

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m pip install --upgrade pip && \
    python -m pip install ".[postgres]"

RUN mkdir -p /app/data /app/backups && chown -R axetos:axetos /app
USER axetos

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3)" || exit 1

CMD ["axetos-market-data-server", "--host", "0.0.0.0", "--port", "8000"]
