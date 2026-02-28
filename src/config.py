from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Ollama
    ollama_host: str = "ollama"
    ollama_port: int = 11434
    ollama_filter_model: str = "phi3:mini"
    ollama_analysis_model: str = "mistral:7b"
    ollama_num_parallel: int = 1

    # Notification backend selection
    notification_backend: str = "telegram"
    notification_backends: list[str] = ["telegram"]

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # ntfy
    ntfy_topic: str = ""
    ntfy_server: str = "https://ntfy.sh"
    ntfy_token: str = ""

    # Email (SMTP)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    email_to: str = ""

    # WhatsApp (Twilio)
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_whatsapp_from: str = "whatsapp:+14155238886"
    my_whatsapp_number: str = ""

    # Data
    sqlite_db_path: str = "/data/investments.db"

    # Logging
    log_level: str = "INFO"

    # RSS feed URLs
    rss_feed_urls: list[str] = [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
        "https://www.marketwatch.com/rss/topstories",
        "https://www.investing.com/rss/news.rss",
        "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    ]

    # Yahoo Finance tickers
    yahoo_tickers: list[str] = [
        "^IBEX",
        "^GSPC",
        "ACWI",
        "VWCE.DE",
        "IUSN.DE",
        "AGGH.DE",
        "SPY",
        "QQQ",
    ]

    # Web scraper targets (Investing.com blocks scrapers with 403; disabled)
    web_scraper_targets: list[dict[str, str]] = []

    # Pipeline settings
    max_article_tokens: int = 2000
    dedup_similarity_threshold: float = 0.9
    prefilter_relevance_threshold: int = 5

    # LLM settings
    ollama_timeout: float = 600.0
    ollama_filter_temperature: float = 0.2
    ollama_analysis_temperature: float = 0.4
    ollama_max_retries: int = 3

    # Recommendation history
    recommendation_history_months: int = 3

    @property
    def ollama_base_url(self) -> str:
        return f"http://{self.ollama_host}:{self.ollama_port}"



def get_settings() -> Settings:
    return Settings()
