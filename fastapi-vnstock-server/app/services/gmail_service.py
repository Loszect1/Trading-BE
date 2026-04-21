from __future__ import annotations

import base64
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.core.config import settings

_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def _safe_name(raw: str, fallback: str) -> str:
    text = (raw or "").strip()
    if not text:
        text = fallback
    cleaned = re.sub(r"[<>:\"/\\|?*\x00-\x1F]", "_", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:140] or fallback


def _decode_urlsafe_base64(raw: str) -> bytes:
    payload = raw.encode("utf-8")
    padding = b"=" * (-len(payload) % 4)
    return base64.urlsafe_b64decode(payload + padding)


def _extract_header(headers: list[dict[str, str]], key: str) -> str:
    for row in headers:
        if str(row.get("name", "")).lower() == key.lower():
            return str(row.get("value", "")).strip()
    return ""


class GmailFetchService:
    def __init__(self) -> None:
        self._client_secret_file = Path(settings.gmail_oauth_client_secret_file).resolve()
        self._token_file = Path(settings.gmail_oauth_token_file).resolve()

    def _build_creds(self) -> Credentials:
        creds: Credentials | None = None
        if self._token_file.exists():
            creds = Credentials.from_authorized_user_file(str(self._token_file), _SCOPES)

        if creds and creds.valid:
            return creds

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not self._client_secret_file.exists():
                raise RuntimeError(
                    f"Gmail client secret file not found: {self._client_secret_file}"
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(self._client_secret_file),
                _SCOPES,
            )
            creds = flow.run_local_server(port=0)

        self._token_file.parent.mkdir(parents=True, exist_ok=True)
        self._token_file.write_text(creds.to_json(), encoding="utf-8")
        return creds

    def fetch_and_download(
        self,
        *,
        query: str,
        max_results: int,
        download_dir: str | None,
        include_eml: bool,
        include_attachments: bool,
    ) -> dict[str, Any]:
        target_dir = Path(download_dir or settings.gmail_download_dir).resolve()
        target_dir.mkdir(parents=True, exist_ok=True)

        try:
            creds = self._build_creds()
            service = build("gmail", "v1", credentials=creds)
            scoped_query = self._build_today_query(query.strip())
            response = (
                service.users()
                .messages()
                .list(userId="me", q=scoped_query, maxResults=max(1, min(max_results, 100)))
                .execute()
            )
            messages = response.get("messages", []) or []

            downloaded: list[dict[str, str]] = []
            folder_counters: dict[str, int] = {}
            for index, item in enumerate(messages, start=1):
                gmail_message_id = str(item.get("id") or "").strip()
                if not gmail_message_id:
                    continue

                full = (
                    service.users()
                    .messages()
                    .get(userId="me", id=gmail_message_id, format="full")
                    .execute()
                )
                raw = (
                    service.users()
                    .messages()
                    .get(userId="me", id=gmail_message_id, format="raw")
                    .execute()
                )
                payload = full.get("payload", {}) if isinstance(full, dict) else {}
                headers = payload.get("headers", []) if isinstance(payload, dict) else []
                subject = _extract_header(headers, "Subject") or f"message_{index}"
                safe_subject = _safe_name(subject, f"message_{index}")
                day_key = self._message_day_key(full)
                sequence = int(folder_counters.get(day_key, 0)) + 1
                folder_counters[day_key] = sequence
                day_folder = target_dir / day_key
                day_folder.mkdir(parents=True, exist_ok=True)

                if include_eml:
                    raw_data = str(raw.get("raw") or "")
                    if raw_data:
                        eml_bytes = _decode_urlsafe_base64(raw_data)
                        eml_name = f"{index:03d}_{safe_subject}.eml"
                        eml_path = day_folder / eml_name
                        eml_path.write_bytes(eml_bytes)
                        downloaded.append(
                            {
                                "message_id": str(full.get("threadId") or ""),
                                "gmail_message_id": gmail_message_id,
                                "subject": subject,
                                "file_path": str(eml_path),
                                "file_type": "eml",
                            }
                        )

                if include_attachments:
                    self._download_attachments(
                        service=service,
                        user_id="me",
                        gmail_message_id=gmail_message_id,
                        payload=payload,
                        subject=subject,
                        thread_id=str(full.get("threadId") or ""),
                        output_dir=day_folder,
                        file_prefix=f"{sequence:03d}_{safe_subject}",
                        output_rows=downloaded,
                    )

            return {
                "success": True,
                "query": scoped_query,
                "matched_messages": len(messages),
                "downloaded_files": len(downloaded),
                "download_dir": str(target_dir),
                "files": downloaded,
            }
        except HttpError as exc:
            raise RuntimeError(f"Gmail API error: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Gmail fetch/download failed: {exc}") from exc

    def fetch_today_message_texts(self, *, query: str, max_results: int) -> list[dict[str, str]]:
        """
        Fetch today's matched emails and extract textual content for AI analysis.
        """
        try:
            creds = self._build_creds()
            service = build("gmail", "v1", credentials=creds)
            scoped_query = self._build_today_query(query.strip())
            response = (
                service.users()
                .messages()
                .list(userId="me", q=scoped_query, maxResults=max(1, min(max_results, 100)))
                .execute()
            )
            messages = response.get("messages", []) or []
            out: list[dict[str, str]] = []
            for item in messages:
                gmail_message_id = str(item.get("id") or "").strip()
                if not gmail_message_id:
                    continue
                full = (
                    service.users()
                    .messages()
                    .get(userId="me", id=gmail_message_id, format="full")
                    .execute()
                )
                payload = full.get("payload", {}) if isinstance(full, dict) else {}
                headers = payload.get("headers", []) if isinstance(payload, dict) else []
                subject = _extract_header(headers, "Subject") or ""
                body_text = self._collect_message_text(service, "me", gmail_message_id, payload)
                if not body_text:
                    body_text = "(empty)"
                out.append(
                    {
                        "gmail_message_id": gmail_message_id,
                        "subject": subject,
                        "text": body_text[:12000],
                    }
                )
            return out
        except HttpError as exc:
            raise RuntimeError(f"Gmail API error: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Gmail text fetch failed: {exc}") from exc

    def _download_attachments(
        self,
        *,
        service: Any,
        user_id: str,
        gmail_message_id: str,
        payload: dict[str, Any],
        subject: str,
        thread_id: str,
        output_dir: Path,
        file_prefix: str,
        output_rows: list[dict[str, str]],
    ) -> None:
        for part in self._walk_parts(payload):
            body = part.get("body") if isinstance(part, dict) else {}
            filename = str(part.get("filename") or "").strip()
            attachment_id = str((body or {}).get("attachmentId") or "").strip()
            if not filename or not attachment_id:
                continue

            response = (
                service.users()
                .messages()
                .attachments()
                .get(userId=user_id, messageId=gmail_message_id, id=attachment_id)
                .execute()
            )
            data = str(response.get("data") or "")
            if not data:
                continue
            content = _decode_urlsafe_base64(data)
            safe_filename = _safe_name(filename, "attachment.bin")
            safe_filename = f"{file_prefix}__{safe_filename}"
            out_path = output_dir / safe_filename
            out_path.write_bytes(content)
            output_rows.append(
                {
                    "message_id": thread_id,
                    "gmail_message_id": gmail_message_id,
                    "subject": subject,
                    "file_path": str(out_path),
                    "file_type": "attachment",
                }
            )

    def _walk_parts(self, node: dict[str, Any]) -> list[dict[str, Any]]:
        parts = node.get("parts")
        if not isinstance(parts, list) or not parts:
            return [node]
        out: list[dict[str, Any]] = []
        for part in parts:
            if isinstance(part, dict):
                out.extend(self._walk_parts(part))
        return out

    def _collect_message_text(
        self,
        service: Any,
        user_id: str,
        gmail_message_id: str,
        payload: dict[str, Any],
    ) -> str:
        chunks: list[str] = []
        for part in self._walk_parts(payload):
            mime = str(part.get("mimeType") or "").lower()
            body = part.get("body") if isinstance(part, dict) else {}
            data = str((body or {}).get("data") or "")
            filename = str(part.get("filename") or "").strip().lower()
            attachment_id = str((body or {}).get("attachmentId") or "").strip()

            if data:
                decoded = self._decode_text_safely(data)
                if decoded and ("text/plain" in mime or "text/markdown" in mime or mime == ""):
                    chunks.append(decoded)
                continue

            if not attachment_id:
                continue
            if filename.endswith(".txt") or filename.endswith(".md") or "text/" in mime:
                response = (
                    service.users()
                    .messages()
                    .attachments()
                    .get(userId=user_id, messageId=gmail_message_id, id=attachment_id)
                    .execute()
                )
                attached_data = str(response.get("data") or "")
                decoded = self._decode_text_safely(attached_data)
                if decoded:
                    chunks.append(decoded)

        # Deduplicate repeated chunks that can appear in multipart alternatives.
        unique_chunks: list[str] = []
        seen: set[str] = set()
        for chunk in chunks:
            norm = chunk.strip()
            if not norm or norm in seen:
                continue
            seen.add(norm)
            unique_chunks.append(norm)
        return "\n\n".join(unique_chunks)

    def _decode_text_safely(self, encoded: str) -> str:
        try:
            raw = _decode_urlsafe_base64(encoded)
        except Exception:
            return ""
        for codec in ("utf-8", "utf-16", "latin-1"):
            try:
                return raw.decode(codec, errors="ignore").strip()
            except Exception:
                continue
        return ""

    def _build_today_query(self, base_query: str) -> str:
        tz = ZoneInfo("Asia/Ho_Chi_Minh")
        now_local = datetime.now(tz=tz)
        start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        after_text = start.strftime("%Y/%m/%d")
        before_text = end.strftime("%Y/%m/%d")
        safe_base = base_query or "in:inbox"
        return f"({safe_base}) after:{after_text} before:{before_text}"

    def _message_day_key(self, message: dict[str, Any]) -> str:
        raw_ms = str(message.get("internalDate") or "").strip()
        try:
            ts_seconds = int(raw_ms) / 1000.0
            dt_utc = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        except Exception:
            return datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d")
        return dt_utc.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d")
