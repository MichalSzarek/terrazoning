from pydantic import Field, PostgresDsn, computed_field
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


# Module-level singleton — import this everywhere
settings = Settings()
