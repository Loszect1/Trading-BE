from __future__ import annotations

import pytest

from app.services.execution.dnse_token import (
    normalize_dnse_error,
    validate_dnse_access_token_for_live,
    validate_dnse_tokens_for_live,
)


@pytest.mark.parametrize(
    "access,trading,ok,reason_substr",
    [
        ("", "", False, "missing"),
        ("a" * 20, "", False, "trading"),
        ("", "abcd", False, "access"),
        ("short", "abcd", False, "too_short"),
        ("a" * 20, "abc", False, "trading"),
        ("a" * 20, "abcd", True, ""),
    ],
)
def test_validate_dnse_tokens_for_live(
    access: str, trading: str, ok: bool, reason_substr: str
) -> None:
    good, reason = validate_dnse_tokens_for_live(access, trading)
    assert good is ok
    if not ok:
        assert reason is not None
        assert reason_substr in reason


def test_validate_dnse_access_token_for_live() -> None:
    ok, _ = validate_dnse_access_token_for_live("a" * 20)
    assert ok is True
    ok2, r2 = validate_dnse_access_token_for_live("")
    assert ok2 is False
    assert r2 == "dnse_access_token_missing"


def test_validate_jwt_shape() -> None:
    bad = ".".join(["", "x", "y"]) + "z" * 14
    ok, reason = validate_dnse_tokens_for_live(bad, "token1")
    assert ok is False
    assert reason == "dnse_access_token_malformed_jwt"


def test_normalize_dnse_error() -> None:
    assert normalize_dnse_error(TimeoutError("x")) == "dnse_timeout"
    assert normalize_dnse_error(ConnectionError("refused")) == "dnse_network_error"
    assert normalize_dnse_error("timeout waiting") == "dnse_timeout"
