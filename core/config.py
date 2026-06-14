# core/config.py
from functools import lru_cache
from typing import List
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # ── Application ───────────────────────────────────────────
    app_env: str = "development"
    app_name: str = "avito_agents"
    log_level: str = "INFO"
    tz: str = "Europe/Moscow"

    # ── PostgreSQL ────────────────────────────────────────────
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "crm_db"
    db_user: str = "postgres"
    db_password: str
    db_pool_size: int = 10
    db_max_overflow: int = 20

    @computed_field
    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # ── Anthropic ─────────────────────────────────────────────
    anthropic_api_key: str
    claude_sonnet_model: str = "claude-sonnet-4-5"
    claude_haiku_model: str = "claude-haiku-4-5-20251001"
    anthropic_monthly_budget_usd: float = 280.0
    anthropic_max_tokens_sonnet: int = 2048
    anthropic_max_tokens_haiku: int = 512

    # ── Telegram ──────────────────────────────────────────────
    telegram_bot_token: str
    telegram_owner_chat_id: str
    telegram_alert_chat_id: str

    # ── Avito ─────────────────────────────────────────────────
    avito_client_id: str
    avito_client_secret: str
    avito_user_id: str
    avito_api_base_url: str = "https://api.avito.ru"
    avito_refresh_token: str = ""  # Set after authorization_code flow

    # ── Nano Banana ───────────────────────────────────────────
    nano_banana_api_key: str
    nano_banana_api_url: str = "https://api.nanobanana.io/v1"
    nano_banana_max_retries: int = 2

    # ── NewsAPI ───────────────────────────────────────────────
    news_api_key: str
    news_api_base_url: str = "https://newsapi.org/v2"

    # ── VKontakte ─────────────────────────────────────────────
    vk_service_token: str
    vk_api_version: str = "5.199"

    # ── Wildberries ───────────────────────────────────────────
    wb_api_base_url: str = "https://search.wb.ru"

    # ── FastAPI ───────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = False

    # ── Scheduler ─────────────────────────────────────────────
    scheduler_timezone: str = "Europe/Moscow"
    avito_peak_hours: str = "9,12,18,20"

    @computed_field
    @property
    def peak_hours_list(self) -> List[int]:
        return [int(h.strip()) for h in self.avito_peak_hours.split(",")]

    # ── Business rules ────────────────────────────────────────
    min_margin_percent: float = 40.0
    expense_alert_threshold: float = 0.60
    conversion_drop_alert: float = 0.30
    client_response_sla_minutes: int = 5
    trend_min_sources: int = 2
    trend_score_threshold: int = 30
    inventory_reorder_days: int = 14

    # ── Security ──────────────────────────────────────────────
    secret_key: str

    @computed_field
    @property
    def is_production(self) -> bool:
        return self.app_env == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
