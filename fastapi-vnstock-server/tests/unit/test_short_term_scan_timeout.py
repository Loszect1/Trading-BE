from __future__ import annotations

import queue

import pytest

import app.services.short_term_automation_service as auto_svc


def test_scan_timeout_wrapper_fails_fast_when_worker_exits_without_payload(monkeypatch) -> None:
    class DeadProcess:
        def start(self) -> None:
            return None

        def is_alive(self) -> bool:
            return False

        def join(self, timeout=None) -> None:
            return None

    class EmptyQueue:
        cancelled = False
        closed = False

        def get(self, timeout=None):
            raise queue.Empty

        def get_nowait(self):
            raise queue.Empty

        def cancel_join_thread(self) -> None:
            self.cancelled = True

        def close(self) -> None:
            self.closed = True

        def join_thread(self) -> None:
            raise AssertionError("join_thread should be skipped for failed worker cleanup")

    empty_queue = EmptyQueue()

    class FakeContext:
        def Queue(self, maxsize=1):
            return empty_queue

        def Process(self, target, args):
            return DeadProcess()

    monkeypatch.setattr(auto_svc.mp, "get_context", lambda _name: FakeContext())

    with pytest.raises(RuntimeError, match="worker exited without result payload"):
        auto_svc._run_short_term_scan_batch_with_timeout(
            limit_symbols=1,
            timeout_seconds=20,
            exchange_scope="HOSE",
        )

    assert empty_queue.cancelled is True
    assert empty_queue.closed is True
