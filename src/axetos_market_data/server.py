from __future__ import annotations

import argparse
import ipaddress
import logging
import os

import uvicorn

from .security import SecuritySettings


def is_loopback_host(host: str) -> bool:
    """Return whether a bind host is restricted to the local machine."""

    normalized = host.strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def validate_network_startup(
    host: str,
    settings: SecuritySettings,
    *,
    allow_insecure_public_bind: bool = False,
) -> None:
    """Prevent accidental unauthenticated exposure on non-loopback interfaces."""

    settings.validate()
    if settings.enabled or is_loopback_host(host) or allow_insecure_public_bind:
        return
    raise RuntimeError(
        "Refusing to bind an unauthenticated server to a non-loopback interface. "
        "Enable authentication with AXETOS_AUTH_ENABLED=true and configure the required "
        "tokens, bind to 127.0.0.1/::1, or explicitly pass --allow-insecure-public-bind."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Axetos Market Data Server web service.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--database-url", default=os.getenv("AXETOS_DATABASE_URL", "data/market_data.sqlite"))
    parser.add_argument(
        "--allow-insecure-public-bind",
        action="store_true",
        help=(
            "Explicitly allow authentication-disabled binding to a non-loopback interface. "
            "Use only behind another trusted access-control boundary."
        ),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=args.log_level.upper())
    settings = SecuritySettings.from_environment()
    validate_network_startup(
        args.host,
        settings,
        allow_insecure_public_bind=args.allow_insecure_public_bind,
    )
    os.environ["AXETOS_DATABASE_URL"] = args.database_url
    uvicorn.run("axetos_market_data.web:app", host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
