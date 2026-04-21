from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from app.schemas.gmail import GmailFetchDownloadRequest, GmailFetchDownloadResponse
from app.services.gmail_service import GmailFetchService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gmail", tags=["gmail"])
_gmail_service = GmailFetchService()
_DEFAULT_GMAIL_QUERY = "Tin hieu Can Cung Vol Dot Bien"


@router.post("/fetch-download", response_model=GmailFetchDownloadResponse)
def post_fetch_and_download(body: GmailFetchDownloadRequest) -> GmailFetchDownloadResponse:
    """
    Fetch emails by Gmail query, then download .eml and attachments to local disk.
    First call may open browser consent flow if OAuth token is not cached.
    """
    try:
        query_text = (body.query or "").strip() or _DEFAULT_GMAIL_QUERY
        raw = _gmail_service.fetch_and_download(
            query=query_text,
            max_results=body.max_results,
            download_dir=body.download_dir,
            include_eml=body.include_eml,
            include_attachments=body.include_attachments,
        )
        return GmailFetchDownloadResponse.model_validate(raw)
    except Exception as exc:
        logger.exception("gmail.fetch_download_failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
