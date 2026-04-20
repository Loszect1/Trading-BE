"""
Shared pytest configuration.

PostgreSQL: integration and replay tests patch ``app.core.config.settings.database_url``
to ``TEST_DATABASE_URL`` if set, otherwise ``DATABASE_URL``, otherwise the default from
``AppSettings`` (localhost). If no server is reachable, those tests are skipped.

Test data uses symbols starting with ``TST`` (both DEMO and REAL); fixtures delete those rows between tests.
"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from psycopg import connect


def _candidate_db_urls() -> list[str]:
    out: list[str] = []
    for key in ("TEST_DATABASE_URL", "DATABASE_URL"):
        v = os.environ.get(key, "").strip()
        if v:
            out.append(v)
    if not out:
        out.append("postgresql://postgres:postgres@127.0.0.1:5432/trading")
    return out


def _first_live_url() -> str | None:
    for url in _candidate_db_urls():
        try:
            with connect(url, connect_timeout=3) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return url
        except Exception:
            continue
    return None


@pytest.fixture(scope="session")
def postgres_url() -> str:
    url = _first_live_url()
    if not url:
        pytest.skip(
            "PostgreSQL not reachable for integration tests "
            "(set TEST_DATABASE_URL or DATABASE_URL, or start local Postgres)."
        )
    return url


@pytest.fixture
def trading_db(postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> Generator[str, None, None]:
    """Point settings at live Postgres and remove prior TST* rows (DEMO and REAL test symbols)."""
    import app.core.config as config_mod

    monkeypatch.setattr(config_mod.settings, "database_url", postgres_url)
    _cleanup_demo_tst(postgres_url)
    yield postgres_url
    _cleanup_demo_tst(postgres_url)


def _cleanup_demo_tst(database_url: str) -> None:
    with connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM order_events
                WHERE order_id IN (
                    SELECT id FROM orders_core
                    WHERE symbol LIKE 'TST%%'
                );
                """
            )
            cur.execute("DELETE FROM position_lots WHERE symbol LIKE 'TST%%';")
            cur.execute("DELETE FROM orders_core WHERE symbol LIKE 'TST%%';")
            cur.execute("SELECT to_regclass('public.experience') AS reg_exp")
            row_exp = cur.fetchone()
            if row_exp and row_exp[0]:
                cur.execute("DELETE FROM experience WHERE symbol LIKE 'TST%%';")
            cur.execute("SELECT to_regclass('public.signals') AS reg")
            row = cur.fetchone()
            if row and row[0]:
                cur.execute("DELETE FROM signals WHERE symbol LIKE 'TST%%';")
        conn.commit()


@pytest.fixture
def trading_api_client(trading_db: str):
    """FastAPI app with trading routes only (avoids scheduler startup from main)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.routers.trading_core import router as trading_core_router

    app = FastAPI()
    app.include_router(trading_core_router)
    return TestClient(app)
