from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import json
import os
import sys
from ctypes import wintypes
from pathlib import Path


class SecretStoreError(RuntimeError):
    pass


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


def _blob(data: bytes) -> tuple[_DATA_BLOB, ctypes.Array]:
    buffer = ctypes.create_string_buffer(data)
    return _DATA_BLOB(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte))), buffer


class SecretStore:
    """Machine-local MT5 secret storage.

    Windows uses DPAPI. Other platforms require AXETOS_SECRET_KEY and use an
    authenticated, machine-local encrypted file. Provider configuration stores
    only whether a secret exists, never the secret value.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _read(self) -> dict[str, dict[str, str]]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            return dict(payload.get("secrets", {}))
        except (OSError, ValueError, TypeError) as exc:
            raise SecretStoreError(f"Could not read secret store: {exc}") from exc

    def _write(self, values: dict[str, dict[str, str]]) -> None:
        self.path.write_text(json.dumps({"version": 1, "secrets": values}, indent=2), encoding="utf-8")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _dpapi_encrypt(value: bytes) -> bytes:
        source, source_buffer = _blob(value)
        target = _DATA_BLOB()
        crypt32 = ctypes.windll.crypt32
        kernel32 = ctypes.windll.kernel32
        if not crypt32.CryptProtectData(ctypes.byref(source), None, None, None, None, 0, ctypes.byref(target)):
            raise SecretStoreError("Windows DPAPI could not protect the MT5 password")
        try:
            return ctypes.string_at(target.pbData, target.cbData)
        finally:
            kernel32.LocalFree(target.pbData)
            del source_buffer

    @staticmethod
    def _dpapi_decrypt(value: bytes) -> bytes:
        source, source_buffer = _blob(value)
        target = _DATA_BLOB()
        crypt32 = ctypes.windll.crypt32
        kernel32 = ctypes.windll.kernel32
        if not crypt32.CryptUnprotectData(ctypes.byref(source), None, None, None, None, 0, ctypes.byref(target)):
            raise SecretStoreError("Windows DPAPI could not decrypt the MT5 password")
        try:
            return ctypes.string_at(target.pbData, target.cbData)
        finally:
            kernel32.LocalFree(target.pbData)
            del source_buffer

    @staticmethod
    def _portable_key() -> bytes:
        raw = os.getenv("AXETOS_SECRET_KEY")
        if not raw:
            raise SecretStoreError("AXETOS_SECRET_KEY is required to save MT5 passwords outside Windows")
        return hashlib.sha256(raw.encode("utf-8")).digest()

    @staticmethod
    def _xor_stream(data: bytes, key: bytes, nonce: bytes) -> bytes:
        output = bytearray()
        counter = 0
        while len(output) < len(data):
            block = hmac.new(key, nonce + counter.to_bytes(8, "big"), hashlib.sha256).digest()
            output.extend(block)
            counter += 1
        return bytes(left ^ right for left, right in zip(data, output, strict=False))

    def _encrypt(self, value: str) -> dict[str, str]:
        encoded = value.encode("utf-8")
        if sys.platform == "win32":
            return {"scheme": "dpapi", "payload": base64.b64encode(self._dpapi_encrypt(encoded)).decode("ascii")}
        key = self._portable_key()
        nonce = os.urandom(16)
        ciphertext = self._xor_stream(encoded, key, nonce)
        tag = hmac.new(key, b"tag" + nonce + ciphertext, hashlib.sha256).digest()
        return {
            "scheme": "portable-v1",
            "payload": base64.b64encode(nonce + tag + ciphertext).decode("ascii"),
        }

    def _decrypt(self, item: dict[str, str]) -> str:
        scheme = item.get("scheme")
        payload = base64.b64decode(item.get("payload", ""))
        if scheme == "dpapi":
            if sys.platform != "win32":
                raise SecretStoreError("This MT5 password was protected with Windows DPAPI")
            return self._dpapi_decrypt(payload).decode("utf-8")
        if scheme == "portable-v1":
            if len(payload) < 48:
                raise SecretStoreError("Encrypted MT5 password is invalid")
            nonce, tag, ciphertext = payload[:16], payload[16:48], payload[48:]
            key = self._portable_key()
            expected = hmac.new(key, b"tag" + nonce + ciphertext, hashlib.sha256).digest()
            if not hmac.compare_digest(tag, expected):
                raise SecretStoreError("Encrypted MT5 password failed authentication")
            return self._xor_stream(ciphertext, key, nonce).decode("utf-8")
        raise SecretStoreError("Unknown MT5 secret-store format")

    def set(self, provider_key: str, password: str) -> None:
        if not password:
            raise SecretStoreError("MT5 password cannot be empty")
        values = self._read()
        values[provider_key.casefold()] = self._encrypt(password)
        self._write(values)

    def get(self, provider_key: str) -> str | None:
        item = self._read().get(provider_key.casefold())
        return self._decrypt(item) if item else None

    def configured(self, provider_key: str) -> bool:
        return provider_key.casefold() in self._read()

    def delete(self, provider_key: str) -> bool:
        values = self._read()
        removed = values.pop(provider_key.casefold(), None) is not None
        if removed:
            self._write(values)
        return removed
