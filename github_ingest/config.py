from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import (
    BaseSettings,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

_COMMON_SETTINGS = Path(__file__).parent.parent / "common" / "settings.json"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    github_token: SecretStr | None = None
    github_base_url: str = "https://api.github.com"
    page_size: int = 100
    request_timeout: int = 30
    max_retries: int = 3
    duckdb_path: Path = Path("warehouse.duckdb")
    log_level: str = "INFO"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        json_source = JsonConfigSettingsSource(settings_cls, json_file=_COMMON_SETTINGS)
        return (init_settings, env_settings, dotenv_settings, json_source, file_secret_settings)
