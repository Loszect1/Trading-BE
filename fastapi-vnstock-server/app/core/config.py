from pydantic_settings import BaseSettings, SettingsConfigDict


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
    dnse_username: str = ""
    dnse_password: str = ""
    #: Gợi ý sub-account mặc định (ví dụ tiền tố TK); FE đọc qua GET /dnse/defaults.
    dnse_default_sub_account: str = ""
    redis_url: str = "redis://127.0.0.1:6379/0"
    ai_cache_ttl_seconds: int = 86400
    #: TTL Redis cho danh sách mã theo sàn / theo ngành (listing), mặc định ~1 năm.
    listing_exchange_industry_redis_ttl_seconds: int = 31_536_000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = AppSettings()
