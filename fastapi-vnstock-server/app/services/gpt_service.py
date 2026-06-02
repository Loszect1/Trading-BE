from __future__ import annotations

import hashlib
import json
import random
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

from app.core.config import settings
from app.services.redis_cache import RedisCacheService


class GptService:
    def __init__(self) -> None:
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
        output_schema: dict[str, Any] | None = None,
    ) -> str:
        if not settings.use_gpt:
            raise RuntimeError("gpt_disabled")

        if not prompt.strip():
            raise ValueError("Prompt must not be empty.")

        selected_model = self._select_model(model)
        full_prompt = self._build_prompt(
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return self._run_codex_cli(
            prompt=full_prompt,
            model=selected_model,
            output_schema=output_schema,
        )

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
        output_schema: dict[str, Any] | None = None,
    ) -> str:
        now = time.time()
        if now < self._blocked_until_epoch:
            self._metrics["cooldown_reject"] += 1
            raise RuntimeError("GPT service temporarily blocked by failure cooldown.")

        cache_key = self._build_cache_key(
            namespace=cache_namespace,
            prompt=prompt,
            system_prompt=system_prompt,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            output_schema=output_schema,
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
                output_schema=output_schema,
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
                self._blocked_until_epoch = time.time() + float(settings.ai_gpt_failure_cooldown_seconds)
                self._metrics["cooldown_trigger"] += 1
            raise

    def get_runtime_metrics(self) -> dict[str, int | float]:
        now = time.time()
        remain = max(0.0, self._blocked_until_epoch - now)
        return {
            **self._metrics,
            "failure_count": int(self._failure_count),
            "cooldown_remaining_seconds": round(remain, 3),
        }

    @staticmethod
    def _select_model(model: Optional[str]) -> str:
        requested_model = (model or "").strip()
        invalid_model_values = {"", "string", "none", "null", "undefined"}
        if requested_model.lower() in invalid_model_values or requested_model.lower().startswith("claude-"):
            return settings.gpt_model.strip()
        return requested_model

    @staticmethod
    def _build_prompt(
        *,
        prompt: str,
        system_prompt: Optional[str],
        max_tokens: Optional[int],
        temperature: float,
    ) -> str:
        parts: list[str] = []
        if system_prompt and system_prompt.strip():
            parts.append(f"System instructions:\n{system_prompt.strip()}")
        if max_tokens:
            parts.append(f"Output budget: keep the final answer within about {int(max_tokens)} tokens.")
        selected_temperature = max(0.0, min(1.0, float(temperature)))
        parts.append(f"Requested sampling temperature: {selected_temperature:.2f}.")
        parts.append(f"User prompt:\n{prompt.strip()}")
        return "\n\n".join(parts)

    def _run_codex_cli(
        self,
        *,
        prompt: str,
        model: str,
        output_schema: dict[str, Any] | None,
    ) -> str:
        with tempfile.TemporaryDirectory(prefix="gpt-codex-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            output_path = tmp_path / "codex-output.txt"
            cmd = [
                "codex",
                "exec",
                "--cd",
                self._resolve_workdir(),
                "--skip-git-repo-check",
                "--ephemeral",
                "--sandbox",
                "read-only",
            ]
            if model:
                cmd += ["--model", model]
            if output_schema:
                schema_path = tmp_path / "codex-output.schema.json"
                schema_path.write_text(json.dumps(output_schema), encoding="utf-8")
                cmd += ["--output-schema", str(schema_path)]
            cmd += ["--output-last-message", str(output_path), "--json", "-"]

            max_retries = max(0, int(settings.gpt_max_retries))
            last_error = ""
            for attempt in range(max_retries + 1):
                try:
                    completed = subprocess.run(
                        cmd,
                        input=prompt,
                        text=True,
                        capture_output=True,
                        timeout=float(settings.gpt_codex_timeout_seconds),
                        check=False,
                    )
                except FileNotFoundError as exc:
                    raise RuntimeError("codex_cli_not_found") from exc
                except subprocess.TimeoutExpired as exc:
                    raise RuntimeError("codex_cli_timeout") from exc

                if completed.returncode == 0 and output_path.exists():
                    text = output_path.read_text(encoding="utf-8").strip()
                    if text:
                        return text
                    last_error = "codex_cli_empty_output"
                else:
                    last_error = self._compact_text(completed.stderr or completed.stdout or "", 1200)

                if attempt < max_retries:
                    delay = (0.8 * (2**attempt)) + random.uniform(0.0, 0.35)
                    time.sleep(delay)

            raise RuntimeError(f"codex_cli_failed: {last_error}")

    @staticmethod
    def _resolve_workdir() -> str:
        configured = (settings.gpt_codex_workdir or "").strip()
        if configured and Path(configured).exists():
            return configured
        if Path("/app").exists():
            return "/app"
        return str(Path.cwd())

    @staticmethod
    def _compact_text(raw: str, max_chars: int) -> str:
        text = " ".join(str(raw or "").split())
        if len(text) <= max_chars:
            return text
        return text[: max(0, max_chars - 3)] + "..."

    @staticmethod
    def _build_cache_key(
        *,
        namespace: str,
        prompt: str,
        system_prompt: Optional[str],
        model: Optional[str],
        max_tokens: Optional[int],
        temperature: float,
        output_schema: dict[str, Any] | None,
    ) -> str:
        payload = {
            "prompt": prompt,
            "system_prompt": system_prompt or "",
            "model": model or "",
            "max_tokens": max_tokens or settings.gpt_max_tokens,
            "temperature": round(float(temperature), 4),
            "output_schema": output_schema or {},
            "pipeline": "gpt_codex_resilience_v1",
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        return f"ai:gpt:{namespace}:{digest}"
