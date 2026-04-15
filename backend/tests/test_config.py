from app.core.config import Settings


def test_database_url_defaults_to_component_config() -> None:
    settings = Settings(
        db_host="db.internal",
        db_port=5433,
        db_name="terrazoning_test",
        db_user="tz_user",
        db_password="pass+with-special",
    )

    assert (
        settings.database_url
        == "postgresql+asyncpg://tz_user:pass+with-special@db.internal:5433/terrazoning_test"
    )
    assert (
        settings.database_url_sync
        == "postgresql+psycopg2://tz_user:pass+with-special@db.internal:5433/terrazoning_test"
    )


def test_database_url_override_normalizes_async_driver() -> None:
    settings = Settings(
        DATABASE_URL="postgresql://admin:secret@127.0.0.1:6543/terrazoning"
    )

    assert (
        settings.database_url
        == "postgresql+asyncpg://admin:secret@127.0.0.1:6543/terrazoning"
    )
    assert (
        settings.database_url_sync
        == "postgresql+psycopg2://admin:secret@127.0.0.1:6543/terrazoning"
    )


def test_database_url_override_rewrites_existing_driver() -> None:
    settings = Settings(
        DATABASE_URL="postgresql+psycopg2://admin:secret@localhost/terrazoning"
    )

    assert (
        settings.database_url
        == "postgresql+asyncpg://admin:secret@localhost/terrazoning"
    )
    assert (
        settings.database_url_sync
        == "postgresql+psycopg2://admin:secret@localhost/terrazoning"
    )


def test_cors_allowed_origins_parses_csv() -> None:
    settings = Settings(
        cors_allowed_origins="http://localhost:5173, https://terrazoning.example.com  ,"
    )

    assert settings.parsed_cors_allowed_origins() == [
        "http://localhost:5173",
        "https://terrazoning.example.com",
    ]
