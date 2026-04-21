from __future__ import annotations

from pydantic import BaseModel, Field


class GmailFetchDownloadRequest(BaseModel):
    query: str = Field(
        default="Tin hieu Can Cung Vol Dot Bien",
        min_length=1,
        max_length=500,
        description="Gmail search query",
    )
    max_results: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of matched emails to fetch for today-filtered query.",
    )
    download_dir: str | None = Field(
        default=None,
        description="Override download directory; defaults to GMAIL_DOWNLOAD_DIR",
    )
    include_eml: bool = Field(
        default=True,
        description="Save full raw email (.eml) for each matched message.",
    )
    include_attachments: bool = Field(
        default=True,
        description="Download all attachments from matched messages.",
    )


class GmailDownloadedFile(BaseModel):
    message_id: str
    gmail_message_id: str
    subject: str
    file_path: str
    file_type: str = Field(description="eml or attachment")


class GmailFetchDownloadResponse(BaseModel):
    success: bool
    query: str
    matched_messages: int
    downloaded_files: int
    download_dir: str
    files: list[GmailDownloadedFile] = Field(default_factory=list)
