from __future__ import annotations

from tests.replay.production_log_redact import redact_sensitive_fields


def test_redact_scrubs_common_secret_keys():
    src = {
        "authorization": "Bearer x",
        "nested": {"api_key": "k", "safe": 1},
        "list": [{"access_token": "t"}, {"ok": True}],
    }
    out = redact_sensitive_fields(src)
    assert out["authorization"] == "[REDACTED]"
    assert out["nested"]["api_key"] == "[REDACTED]"
    assert out["nested"]["safe"] == 1
    assert out["list"][0]["access_token"] == "[REDACTED]"
    assert out["list"][1]["ok"] is True


def test_redact_matches_substrings_case_insensitive():
    src = {"Dnse_Trading_Token": "abc", "x_authorization_y": "z", "symbol": "VCI"}
    out = redact_sensitive_fields(src)
    assert out["Dnse_Trading_Token"] == "[REDACTED]"
    assert out["x_authorization_y"] == "[REDACTED]"
    assert out["symbol"] == "VCI"


def test_redact_custom_placeholder():
    out = redact_sensitive_fields({"password": "x"}, placeholder="***")
    assert out["password"] == "***"
