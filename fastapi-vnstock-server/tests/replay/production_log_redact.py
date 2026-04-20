"""
Redact sensitive fields from captured production-style JSON before replay or storage.

Matching is by dict key substring (case-insensitive), so nested broker payloads and
HTTP-ish envelopes are covered without hard-coding every vendor field name.
"""

from __future__ import annotations

import copy
from typing import Any

_SENSITIVE_KEY_MARKERS: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "apikey",
        "api_key",
        "authorization",
        "auth_header",
        "cookie",
        "cookies",
        "credential",
        "private_key",
        "client_secret",
        "access_token",
        "refresh_token",
        "trading_token",
        "id_token",
        "bearer",
    }
)


def _key_is_sensitive(key: str) -> bool:
    lower = key.lower().replace("-", "_")
    return any(m in lower for m in _SENSITIVE_KEY_MARKERS)


def redact_sensitive_fields(value: Any, *, placeholder: str = "[REDACTED]") -> Any:
    """
    Return a structure of the same shape with sensitive dict keys scrubbed.

    - dict: recurse; if a key matches, replace its value with ``placeholder`` (no recursion into old secret).
    - list/tuple: recurse element-wise (tuple becomes list).
    - scalars: returned as-is.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if _key_is_sensitive(str(k)):
                out[k] = placeholder
            else:
                out[k] = redact_sensitive_fields(v, placeholder=placeholder)
        return out
    if isinstance(value, list):
        return [redact_sensitive_fields(v, placeholder=placeholder) for v in value]
    if isinstance(value, tuple):
        return [redact_sensitive_fields(v, placeholder=placeholder) for v in value]
    return copy.deepcopy(value)
