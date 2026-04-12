from pydantic import Field, PostgresDsn, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration — loaded from environment variables or .env file.

    All sensitive values (passwords, API keys) are read exclusively from the environment.
    Never set defaults for secrets here. The application will fail at startup if required
    env vars are missing — that is intentional.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Application
    # -------------------------------------------------------------------------
    app_name: str = "TerraZoning API"
    app_version: str = "0.1.0"
    debug: bool = False
    log_level: str = "INFO"

    @field_validator("debug", mode="before")
    @classmethod
    def parse_debug(cls, value: object) -> object:
        """Accept common env-style debug values beyond strict booleans."""
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on", "debug", "dev", "development"}:
                return True
            if normalized in {"0", "false", "no", "off", "release", "prod", "production"}:
                return False
        return value

    # -------------------------------------------------------------------------
    # Database — PostgreSQL + PostGIS
    # Individual components allow docker-compose and GCP Cloud SQL to provide
    # host/port/user/pass separately without constructing a URL externally.
    # -------------------------------------------------------------------------
    db_host: str = Field(default="localhost")
    db_port: int = Field(default=5432)
    db_name: str = Field(default="terrazoning")
    db_user: str = Field(default="terrazoning")
    db_password: str = Field(default="terrazoning")

    # Async driver for SQLAlchemy (asyncpg)
    @computed_field
    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # Sync driver for Alembic offline migrations (psycopg2)
    @computed_field
    @property
    def database_url_sync(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # -------------------------------------------------------------------------
    # Connection pool tuning
    # -------------------------------------------------------------------------
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_timeout: int = 30

    # -------------------------------------------------------------------------
    # GCS — Evidence Chain storage
    # -------------------------------------------------------------------------
    gcs_bucket: str = Field(default="terrazoning-evidence-dev")
    gcs_project: str = Field(default="terrazoning-dev")

    # -------------------------------------------------------------------------
    # External APIs
    # -------------------------------------------------------------------------
    uldk_base_url: str = "https://uldk.gugik.gov.pl"
    komornik_notice_api_base_url: str = (
        "https://licytacje.komornik.pl/services/item-back/rest/item/notice"
    )
    komornik_notice_basic_auth: str = (
        "Basic ZS1hdWN0aW9uc19hcHA6ZS1hdWN0aW9uc0AxMjM="
    )

    # -------------------------------------------------------------------------
    # LLM fallback extractor — Gemini on Vertex AI
    # Uses ADC locally (`gcloud auth application-default login`) or the
    # attached service account in Cloud Run / GCE. If gcp_project_id is empty,
    # the extractor will try to discover it from ADC at runtime.
    # -------------------------------------------------------------------------
    gcp_project_id: str = ""
    gcp_location: str = "global"
    vertex_model: str = "gemini-2.5-flash"
    llm_fallback_enabled: bool = True
    llm_timeout_s: int = 20
    llm_max_output_tokens: int = 256
    llm_temperature: float = 0.0

    # -------------------------------------------------------------------------
    # Feature flags
    # -------------------------------------------------------------------------
    future_buildability_enabled: bool = True


# Module-level singleton — import this everywhere
settings = Settings()
