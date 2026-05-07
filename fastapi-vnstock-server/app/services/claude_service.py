from __future__ import annotations

import hashlib
import json
import random
import time
from typing import Optional

from anthropic import Anthropic
from anthropic import APIError
from anthropic import APIStatusError

from app.core.config import settings
from app.services.redis_cache import RedisCacheService


class ClaudeService:
    def __init__(self) -> None:
        self._client = Anthropic(api_key=settings.claude_token or None)
        self._cache = RedisCacheService()
        self._failure_count = 0
        self._blocked_until_epoch = 0.0
        self._metrics: dict[str, int] = {
            "cache_hit": 0,
            "cache_miss": 0,
            "request_success": 0,
            "request_failure": 0,
            "cooldown_trigger": 0,
            "cooldown_reject": 0,
        }

    def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: float = 0.2,
    ) -> str:
        if not settings.use_claude:
            raise RuntimeError("claude_disabled")

        token = (settings.claude_token or "").strip()
        if not token:
            raise ValueError("Missing CLAUDE_TOKEN in environment.")

        if not prompt.strip():
            raise ValueError("Prompt must not be empty.")

        requested_model = (model or "").strip()
        invalid_model_values = {"", "string", "none", "null", "undefined"}
        selected_model = (
            settings.claude_model.strip()
            if requested_model.lower() in invalid_model_values
            else requested_model
        )
        if not selected_model:
            raise ValueError("Missing Claude model. Set CLAUDE_MODEL or pass a valid model.")
        fallback_model = (settings.claude_fallback_model or "").strip()
        selected_max_tokens = max_tokens or settings.claude_max_tokens
        selected_temperature = max(0.0, min(1.0, temperature))
        max_retries = max(0, int(settings.claude_max_retries))

        model_candidates = [selected_model]
        if fallback_model and fallback_model != selected_model:
            model_candidates.append(fallback_model)

        last_error: Exception | None = None
        primary_error: Exception | None = None
        response = None
        for model_index, model_name in enumerate(model_candidates):
            request_kwargs = {
                "model": model_name,
                "max_tokens": selected_max_tokens,
                "temperature": selected_temperature,
                "messages": [{"role": "user", "content": prompt}],
            }
            if system_prompt and system_prompt.strip():
                request_kwargs["system"] = [
                    {"type": "text", "text": system_prompt.strip()}
                ]

            for attempt in range(max_retries + 1):
                try:
                    response = self._client.messages.create(**request_kwargs)
                    break
                except APIStatusError as exc:
                    last_error = exc
                    if model_index == 0:
                        primary_error = exc
                    # If fallback model does not exist for this account/project, skip it.
                    if exc.status_code == 404 and model_index > 0:
                        break
                    if exc.status_code in (429, 529) and attempt < max_retries:
                        # Exponential backoff + jitter to reduce synchronized retry spikes.
                        delay = (1.1 * (2 ** attempt)) + random.uniform(0.0, 0.45)
                        time.sleep(delay)
                        continue
                    # If primary model overloaded, allow switching to fallback model.
                    if exc.status_code in (429, 529):
                        break
                    raise RuntimeError(
                        f"Claude API status error ({exc.status_code}): {exc.message}"
                    ) from exc
                except APIError as exc:
                    last_error = exc
                    if attempt < max_retries:
                        delay = (1.1 * (2 ** attempt)) + random.uniform(0.0, 0.45)
                        time.sleep(delay)
                        continue
                    raise RuntimeError(f"Claude API error: {str(exc)}") from exc
                except Exception as exc:
                    last_error = exc
                    raise RuntimeError(f"Unexpected Claude service error: {str(exc)}") from exc
            if response is not None:
                break

        if response is None:
            if isinstance(last_error, APIStatusError) and last_error.status_code == 404 and primary_error is not None:
                last_error = primary_error
            if isinstance(last_error, APIStatusError):
                raise RuntimeError(
                    f"Claude API status error ({last_error.status_code}): {last_error.message}"
                ) from last_error
            if isinstance(last_error, APIError):
                raise RuntimeError(f"Claude API error: {str(last_error)}") from last_error
            raise RuntimeError(f"Claude API failed after retries: {last_error}")

        text_parts = []
        for item in response.content:
            block_text = getattr(item, "text", None)
            if block_text:
                text_parts.append(block_text)

        answer = "\n".join(text_parts).strip()
        if not answer:
            raise RuntimeError("Claude returned an empty response.")
        return answer

    def generate_text_with_resilience(
        self,
        *,
        prompt: str,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: float = 0.2,
        cache_namespace: str,
        cache_ttl_seconds: int,
    ) -> str:
        now = time.time()
        if now < self._blocked_until_epoch:
            self._metrics["cooldown_reject"] += 1
            raise RuntimeError("Claude service temporarily blocked by failure cooldown.")

        cache_key = self._build_cache_key(
            namespace=cache_namespace,
            prompt=prompt,
            system_prompt=system_prompt,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        cached = self._cache.get_json(cache_key)
        if cached and isinstance(cached.get("text"), str) and cached["text"].strip():
            self._metrics["cache_hit"] += 1
            return str(cached["text"])
        self._metrics["cache_miss"] += 1

        try:
            text = self.generate_text(
                prompt=prompt,
                system_prompt=system_prompt,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            self._failure_count = 0
            self._blocked_until_epoch = 0.0
            self._metrics["request_success"] += 1
            self._cache.set_json(cache_key, {"text": text}, ttl_seconds=max(1, int(cache_ttl_seconds)))
            return text
        except Exception:
            self._failure_count += 1
            self._metrics["request_failure"] += 1
            if self._failure_count >= 2:
                self._blocked_until_epoch = time.time() + float(settings.ai_claude_failure_cooldown_seconds)
                self._metrics["cooldown_trigger"] += 1
            raise

    @staticmethod
    def _build_cache_key(
        *,
        namespace: str,
        prompt: str,
        system_prompt: Optional[str],
        model: Optional[str],
        max_tokens: Optional[int],
        temperature: float,
    ) -> str:
        payload = {
            "prompt": prompt,
            "system_prompt": system_prompt or "",
            "model": model or "",
            "max_tokens": max_tokens or settings.claude_max_tokens,
            "temperature": round(float(temperature), 4),
            "pipeline": "claude_resilience_v1",
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        return f"ai:claude:{namespace}:{digest}"

    def get_runtime_metrics(self) -> dict[str, int | float]:
        now = time.time()
        remain = max(0.0, self._blocked_until_epoch - now)
        return {
            **self._metrics,
            "failure_count": int(self._failure_count),
            "cooldown_remaining_seconds": round(remain, 3),
        }
