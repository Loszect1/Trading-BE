from __future__ import annotations

import json
from typing import Any, Optional

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import settings


class RedisCacheService:
    def __init__(self) -> None:
        self._client: Optional[Redis] = None
        self._enabled = bool((settings.redis_url or "").strip())

    def _get_client(self) -> Optional[Redis]:
        if not self._enabled:
            return None
        if self._client is None:
            self._client = Redis.from_url(
                settings.redis_url.strip(),
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        return self._client

    def get_json(self, key: str) -> Optional[dict[str, Any]]:
        client = self._get_client()
        if client is None:
            return None
        try:
            raw = client.get(key)
            if not raw:
                return None
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else None
        except (RedisError, json.JSONDecodeError):
            return None

    def set_json(self, key: str, value: dict[str, Any], ttl_seconds: int) -> None:
        client = self._get_client()
        if client is None:
            return
        try:
            payload = json.dumps(value, ensure_ascii=False, default=self._json_default)
            client.setex(key, ttl_seconds, payload)
        except (RedisError, TypeError, ValueError):
            return

    def scan_keys(self, pattern: str, *, limit: int = 10_000) -> list[str]:
        client = self._get_client()
        if client is None:
            return []
        safe_limit = max(1, min(int(limit), 200_000))
        out: list[str] = []
        try:
            for key in client.scan_iter(match=pattern, count=1000):
                out.append(str(key))
                if len(out) >= safe_limit:
                    break
            return out
        except RedisError:
            return []

    def delete_keys_by_pattern(self, pattern: str, *, limit: int = 200_000, chunk_size: int = 1000) -> int:
        client = self._get_client()
        if client is None:
            return 0
        safe_limit = max(1, min(int(limit), 500_000))
        safe_chunk_size = max(1, min(int(chunk_size), 5000))
        deleted = 0
        pending: list[str] = []
        try:
            for key in client.scan_iter(match=pattern, count=1000):
                pending.append(str(key))
                if len(pending) >= safe_chunk_size:
                    deleted += int(client.delete(*pending) or 0)
                    pending = []
                if deleted >= safe_limit:
                    break
            if pending and deleted < safe_limit:
                deleted += int(client.delete(*pending) or 0)
            return min(deleted, safe_limit)
        except RedisError:
            return deleted

    @staticmethod
    def _json_default(value: Any) -> Any:
        # Handle numpy/pandas scalar types and uncommon objects from dataframes.
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        if isinstance(value, set):
            return list(value)
        return str(value)
