from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    app_name: str = "VNStock Backend Service"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_reload: bool = True
    vnstock_api_key: str = ""
    dnse_username: str = ""
    dnse_password: str = ""
    #: Gợi ý sub-account mặc định (ví dụ tiền tố TK); FE đọc qua GET /dnse/defaults.
    dnse_default_sub_account: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = AppSettings()
