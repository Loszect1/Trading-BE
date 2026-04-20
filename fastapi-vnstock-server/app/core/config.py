from datetime import date

from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.services.vn_market_holiday_calendar import (
    parse_vn_market_holidays_csv,
    resolve_vn_market_holiday_dates,
)


class AppSettings(BaseSettings):
    app_name: str = "VNStock Backend Service"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_reload: bool = True
    vnstock_api_key: str = ""
    claude_token: str = ""
    claude_model: str = "claude-sonnet-4-6"
    claude_fallback_model: str = "claude-3-5-haiku-latest"
    claude_max_tokens: int = 1024
    claude_max_retries: int = 3
    #: Enable Claude-based enrichment for signal scoring metadata.
    ai_claude_signal_scoring_enabled: bool = False
    #: Max tokens for Claude scoring enrichment responses.
    ai_claude_signal_scoring_max_tokens: int = Field(default=500, ge=128, le=2048)
    #: Redis cache TTL (seconds) for Claude scoring enrichment responses.
    ai_claude_signal_scoring_cache_ttl_seconds: int = Field(default=1800, ge=30, le=86_400)
    #: Enable Claude-based analysis for experience root cause/action.
    ai_claude_experience_enabled: bool = False
    #: Max tokens for Claude experience analysis responses.
    ai_claude_experience_max_tokens: int = Field(default=400, ge=128, le=2048)
    #: Redis cache TTL (seconds) for Claude experience analysis responses.
    ai_claude_experience_cache_ttl_seconds: int = Field(default=3600, ge=30, le=86_400)
    #: Enable Claude refinement of BUY levels (entry/tp/sl) before automation execution.
    ai_claude_automation_levels_enabled: bool = False
    #: Max tokens for Claude automation level refinement responses.
    ai_claude_automation_levels_max_tokens: int = Field(default=350, ge=128, le=2048)
    #: Redis cache TTL (seconds) for Claude automation level refinement.
    ai_claude_automation_levels_cache_ttl_seconds: int = Field(default=900, ge=30, le=86_400)
    #: Global cool-down seconds after repeated Claude API failures.
    ai_claude_failure_cooldown_seconds: int = Field(default=45, ge=5, le=600)
    dnse_username: str = ""
    dnse_password: str = ""
    #: Gợi ý sub-account mặc định (ví dụ tiền tố TK); FE đọc qua GET /dnse/defaults.
    dnse_default_sub_account: str = ""
    #: Sub-account used by server-side REAL execution when ``REAL_EXECUTION_ADAPTER`` is DNSE (overrides default hint).
    dnse_sub_account: str = ""
    #: JWT from DNSE login (``POST /dnse/auth/login``). Required for automated REAL execution without per-request login.
    dnse_access_token: str = ""
    #: Trading token from ``POST /dnse/auth/trading-token``. Short-lived; refresh out of band for automation.
    dnse_trading_token: str = ""
    #: Default stock order type for automated DNSE placement (vnstock ``order_type``, e.g. LO).
    dnse_order_type: str = "LO"
    dnse_asset_type: Literal["stock", "derivative"] = "stock"
    #: Optional loan package id forwarded to vnstock ``place_order``.
    dnse_loan_package_id: int | None = None
    #: REAL execution backend: ``demo`` keeps internal simulated fills; ``dnse_live`` calls DNSE (reject on outage, no silent fills).
    real_execution_adapter: Literal["demo", "dnse_live"] = "demo"
    dnse_execution_timeout_seconds: float = Field(default=20.0, ge=2.0, le=120.0)
    dnse_execution_place_retries: int = Field(default=2, ge=0, le=8)
    dnse_execution_poll_attempts: int = Field(default=6, ge=1, le=40)
    dnse_execution_poll_interval_seconds: float = Field(default=0.45, ge=0.05, le=5.0)
    #: Background refresh of trading token for ``dnse_live`` (off by default). Requires valid ``DNSE_ACCESS_TOKEN``.
    dnse_trading_token_refresh_enabled: bool = False
    #: Seconds between refresh attempts when enabled (minimum enforced for broker rate safety).
    dnse_trading_token_refresh_interval_seconds: int = Field(default=300, ge=60, le=86_400)
    #: Per-attempt timeout for broker ``get_trading_token`` (thread-pool bounded).
    dnse_trading_token_refresh_timeout_seconds: float = Field(default=30.0, ge=5.0, le=120.0)
    #: Forwarded to vnstock ``get_trading_token`` when ``DNSE_REFRESH_OTP`` is empty.
    dnse_trading_token_refresh_smart_otp: bool = True
    #: OTP for scheduled refresh when ``smart_otp`` is false or broker requires explicit OTP.
    dnse_refresh_otp: str = ""
    database_url: str = "postgresql://postgres:postgres@127.0.0.1:5432/trading"
    redis_url: str = "redis://127.0.0.1:6379/0"
    ai_cache_ttl_seconds: int = 86400
    #: Redis TTL for cached vnstock financial responses (default: 30 days).
    financial_cache_ttl_seconds: int = Field(default=2_592_000, ge=300, le=31_536_000)
    #: TTL Redis cho danh sách mã theo sàn / theo ngành (listing), mặc định ~1 năm.
    listing_exchange_industry_redis_ttl_seconds: int = 31_536_000
    #: TTL cache cho GET /news (tổng hợp RSS).
    news_cache_ttl_seconds: int = 300
    #: Khóa API Firecrawl (Bearer) cho GET /news/firecrawl/* — đặt FIRECRAWL_API_KEY trong .env.
    firecrawl_api_key: str = ""
    #: Timeout gửi xuống Firecrawl Search (ms), trong khoảng API cho phép.
    firecrawl_search_timeout_ms: int = 90_000
    #: Số tin tối đa lấy từ Firecrawl (tin trong ngày) khi gọi GET /news.
    news_firecrawl_today_max: int = 20
    #: Mỗi feed RSS lỗi: tối đa số bài Firecrawl Search thay thế (site:domain, tbs=qdr:d).
    news_firecrawl_fallback_per_feed: int = 5
    #: Tổng giới hạn bài Firecrawl fallback mỗi request /news (tránh tốn credit).
    news_firecrawl_fallback_max_total: int = 30
    #: Số worker song song khi gọi Firecrawl cho nhiều feed lỗi.
    news_firecrawl_fallback_max_workers: int = 4
    #: Short-term scanner: minutes between runs during VN regular sessions (09:00–11:30, 13:00–14:45 local).
    short_term_scan_interval_minutes: int = Field(default=15, ge=1, le=120)
    #: IANA timezone for session windows and schedule API (VN market local time).
    short_term_scan_timezone: str = Field(default="Asia/Ho_Chi_Minh", min_length=3, max_length=64)
    #: Drawdown proxy (from snapshots + positions) alert threshold, percent.
    monitoring_drawdown_alert_pct: float = Field(default=15.0, ge=0.1, le=99.0)
    #: Rolling window for counting rejected orders (execution stress proxy).
    monitoring_error_window_minutes: int = Field(default=60, ge=5, le=1440)
    #: How many rejected orders in the window triggers an alert.
    monitoring_error_count_threshold: int = Field(default=5, ge=1, le=500)
    #: If the latest signal is older than this many minutes, emit stale-data alert.
    monitoring_signal_stale_minutes: int = Field(default=240, ge=1, le=10080)
    #: Suppress duplicate same-rule alerts within this many minutes.
    monitoring_alert_cooldown_minutes: int = Field(default=30, ge=1, le=1440)
    #: Max open-position symbols to fetch external mark prices for (dashboard MTM only).
    monitoring_dashboard_mtm_max_symbols: int = Field(default=40, ge=1, le=200)
    #: Telegram bot token for production alert push (optional).
    monitoring_telegram_bot_token: str = ""
    #: Telegram chat id for production alert push (optional).
    monitoring_telegram_chat_id: str = ""
    #: Slack incoming webhook URL for production alert push (optional).
    monitoring_slack_webhook_url: str = ""
    #: Outbound timeout when dispatching alert to external channels.
    monitoring_alert_dispatch_timeout_seconds: float = Field(default=5.0, ge=1.0, le=30.0)
    #: Enable internal short-term scheduler loop (disabled by default for safety).
    automation_short_term_scheduler_enabled: bool = False
    #: Poll interval (seconds) for internal scheduler loop.
    automation_short_term_scheduler_poll_seconds: int = Field(default=30, ge=5, le=300)
    #: Hard timeout for one short-term scan batch to avoid lock starvation.
    #: Default 15 minutes for full-universe scans across HOSE/HNX/UPCOM.
    automation_short_term_scan_timeout_seconds: int = Field(default=900, ge=20, le=1800)
    #: Global throttle for vnstock calls per process to reduce upstream rate-limit bursts.
    vnstock_max_requests_per_minute: int = Field(default=48, ge=10, le=600)
    #: Cooldown in seconds between exchange phases for ALL-scope manual scan.
    automation_short_term_scan_exchange_cooldown_seconds: float = Field(default=5.0, ge=0.0, le=120.0)
    #: Hard timeout budget per exchange phase when scope=ALL (HOSE/HNX/UPCOM).
    automation_short_term_scan_all_phase_timeout_seconds: int = Field(default=120, ge=30, le=900)
    #: Retry attempts for full scan when upstream rate-limit is detected before falling back to light mode.
    automation_short_term_scan_rate_limit_retry_attempts: int = Field(default=1, ge=0, le=6)
    #: Base backoff delay (seconds) between full-scan retries after rate-limit.
    automation_short_term_scan_rate_limit_retry_backoff_seconds: float = Field(default=8.0, ge=1.0, le=120.0)
    #: Hard timeout for one BUY handling step (risk + place_order) to prevent post-scan hangs.
    automation_short_term_buy_step_timeout_seconds: int = Field(default=25, ge=5, le=180)
    #: Timeout for fallback light scan after full scan hits the hard timeout.
    #: Increased default for unlimited fallback scans.
    automation_short_term_scan_fallback_light_timeout_seconds: int = Field(default=900, ge=30, le=1800)
    #: Max symbols processed in fallback light scan (4 per exchange x 3 = 12).
    automation_short_term_scan_fallback_light_max_symbols: int = Field(default=12, ge=4, le=20)
    #: Liquidity floor (average 20 sessions volume) to exclude illiquid/penny-like symbols from short-term analysis.
    short_term_scan_min_avg_daily_volume: float = Field(default=20_000.0, ge=0.0, le=100_000_000.0)
    #: Minimum latest-session volume to consider a symbol actively traded for short-term analysis.
    short_term_scan_min_latest_volume: float = Field(default=10_000.0, ge=0.0, le=100_000_000.0)
    #: Minimum volume spike ratio (latest / 30-session baseline) required before running short-term analysis.
    short_term_scan_min_volume_spike_ratio: float = Field(default=1.5, ge=1.0, le=20.0)
    #: Redis TTL for per-symbol liquidity gate cache (symbol + exchange).
    short_term_symbol_liquidity_cache_ttl_seconds: int = Field(default=86_400, ge=300, le=604_800)
    #: Enable pre-market warmup job to populate short-term liquidity cache each weekday.
    automation_short_term_cache_warm_enabled: bool = False
    #: Warmup schedule (VN local time), default 07:00 Monday-Friday.
    automation_short_term_cache_warm_hour: int = Field(default=7, ge=0, le=23)
    automation_short_term_cache_warm_minute: int = Field(default=0, ge=0, le=59)
    #: Enable post-close refresh: update latest daily volume + rebuild short-term liquidity cache from DB.
    automation_short_term_post_close_refresh_enabled: bool = True
    #: Post-close refresh schedule (VN local time), default 16:00 Monday-Friday.
    automation_short_term_post_close_refresh_hour: int = Field(default=16, ge=0, le=23)
    automation_short_term_post_close_refresh_minute: int = Field(default=0, ge=0, le=59)
    #: Retention window (days) for market_symbol_daily_volume writes.
    market_symbol_daily_volume_retention_days: int = Field(default=30, ge=5, le=120)
    #: Optional CSV list of VN market holidays: YYYY-MM-DD,YYYY-MM-DD
    vn_market_holidays_csv: str = ""
    #: When true, merge packaged `app/data/vn_market_holidays_builtin.json` into the holiday set.
    vn_market_holidays_builtin_enabled: bool = True
    #: Optional path to local JSON file: array of YYYY-MM-DD or {"dates": [...]} — no network I/O.
    vn_market_holidays_json_path: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    @field_validator("short_term_scan_timezone")
    @classmethod
    def _timezone_must_resolve(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError as e:
            raise ValueError(f"Invalid IANA timezone: {v!r}") from e
        return v

    @model_validator(mode="after")
    def _short_term_scan_interval_aligns_sessions(self) -> "AppSettings":
        from app.services.short_term_scan_schedule import validate_interval_for_default_sessions

        try:
            validate_interval_for_default_sessions(self.short_term_scan_interval_minutes)
        except ValueError as e:
            raise ValueError(
                f"short_term_scan_interval_minutes={self.short_term_scan_interval_minutes!r} "
                f"does not align with VN session windows: {e}"
            ) from e
        return self


settings = AppSettings()


def parse_vn_market_holidays(raw_csv: str) -> set[date]:
    """Backward-compatible CSV-only parser (same as legacy `parse_vn_market_holidays_csv`)."""
    return parse_vn_market_holidays_csv(raw_csv)


def get_vn_market_holiday_dates() -> frozenset[date]:
    """Union of builtin JSON (optional), optional local JSON file, and CSV env."""
    return resolve_vn_market_holiday_dates(
        builtin_enabled=settings.vn_market_holidays_builtin_enabled,
        json_path=settings.vn_market_holidays_json_path,
        csv=settings.vn_market_holidays_csv,
    )
