from __future__ import annotations

import base64
import os
import secrets
from dataclasses import dataclass
from enum import IntEnum

from fastapi import Request
from fastapi.responses import JSONResponse


class Role(IntEnum):
    VIEWER = 1
    OPERATOR = 2
    ADMINISTRATOR = 3


@dataclass(frozen=True, slots=True)
class SecuritySettings:
    enabled: bool
    viewer_token: str | None
    operator_token: str | None
    administrator_token: str | None
    bridge_token: str | None

    @classmethod
    def from_environment(cls) -> "SecuritySettings":
        enabled = os.getenv("AXETOS_AUTH_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
        return cls(
            enabled=enabled,
            viewer_token=_clean(os.getenv("AXETOS_VIEWER_TOKEN")),
            operator_token=_clean(os.getenv("AXETOS_OPERATOR_TOKEN")),
            administrator_token=_clean(os.getenv("AXETOS_ADMIN_TOKEN")),
            bridge_token=_clean(os.getenv("AXETOS_BRIDGE_TOKEN")),
        )

    def validate(self) -> None:
        if not self.enabled:
            return
        if not self.administrator_token:
            raise RuntimeError("AXETOS_ADMIN_TOKEN is required when authentication is enabled")
        if not self.bridge_token:
            raise RuntimeError("AXETOS_BRIDGE_TOKEN is required when authentication is enabled")

    def role_for_token(self, token: str | None) -> Role | None:
        if not token:
            return None
        candidates = (
            (Role.ADMINISTRATOR, self.administrator_token),
            (Role.OPERATOR, self.operator_token),
            (Role.VIEWER, self.viewer_token),
        )
        for role, expected in candidates:
            if expected and secrets.compare_digest(token, expected):
                return role
        return None

    def bridge_token_is_valid(self, token: str | None) -> bool:
        return bool(token and self.bridge_token and secrets.compare_digest(token, self.bridge_token))


def _clean(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


def _credential_token(request: Request) -> str | None:
    api_key = request.headers.get("x-api-key")
    if api_key:
        return api_key.strip()
    authorization = request.headers.get("authorization", "")
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() == "bearer":
        return value.strip()
    if scheme.lower() == "basic":
        try:
            decoded = base64.b64decode(value).decode("utf-8")
            _, _, password = decoded.partition(":")
            return password.strip()
        except (ValueError, UnicodeDecodeError):
            return None
    return None


def required_role(path: str, method: str) -> Role:
    if method in {"GET", "HEAD", "OPTIONS"}:
        return Role.VIEWER
    admin_prefixes = (
        "/api/database/retention/run",
        "/api/maintenance/schedules",
        "/api/symbol-policies",
    )
    if method == "DELETE" or path.startswith(admin_prefixes):
        return Role.ADMINISTRATOR
    if method == "PUT" and path.startswith("/api/providers/"):
        return Role.ADMINISTRATOR
    return Role.OPERATOR


def install_security_middleware(app, settings: SecuritySettings) -> None:
    settings.validate()

    @app.middleware("http")
    async def authorize(request: Request, call_next):
        if not settings.enabled:
            return await call_next(request)

        path = request.url.path
        if path in {"/api/health", "/metrics", "/openapi.json"} or path.startswith("/docs") or path.startswith("/redoc"):
            return await call_next(request)

        token = _credential_token(request)
        if path.startswith("/api/market-data/ingest/mt5/"):
            if settings.bridge_token_is_valid(token):
                return await call_next(request)
            return JSONResponse(
                status_code=401,
                content={"detail": "Valid MT5 bridge API token required"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        actual_role = settings.role_for_token(token)
        expected_role = required_role(path, request.method.upper())
        if actual_role is None:
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication required"},
                headers={"WWW-Authenticate": 'Basic realm="Axetos Market Data Server"'},
            )
        if actual_role < expected_role:
            return JSONResponse(
                status_code=403,
                content={"detail": f"{expected_role.name.lower()} role required"},
            )
        request.state.role = actual_role.name.lower()
        return await call_next(request)
