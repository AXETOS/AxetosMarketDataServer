from __future__ import annotations

import argparse
import logging
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Axetos Market Data Server web service.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--database-url", default=os.getenv("AXETOS_DATABASE_URL", "data/market_data.sqlite"))
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper())
    os.environ["AXETOS_DATABASE_URL"] = args.database_url
    uvicorn.run("axetos_market_data.web:app", host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
