import logging
from pathlib import Path

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR: Path = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"

LOG_FORMAT: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
LOG_FILE = "govscape_pipeline.log"
LOG_FILE_PATH: Path = LOG_DIR / "govscape_pipeline.log"

# 2. Asegurar que la carpeta 'logs/' exista en la raíz
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO, format=LOG_FORMAT, handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler()]
)

logger = logging.getLogger("govscape_config")


class Settings(BaseSettings):
    congress_api_key: SecretStr = Field(alias="CONGRESS_API_KEY")
    critical_min_records: int = Field(default=5, alias="CRITICAL_MIN_RECORDS")
    expected_min_states: int = Field(default=5, alias="EXPECTED_MIN_STATES")

    base_data_path: Path = BASE_DIR / "data"

    @property
    def bronze_path(self) -> Path:
        return self.base_data_path / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.base_data_path / "silver"

    @property
    def gold_path(self) -> Path:
        return self.base_data_path / "gold"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


try:
    config = Settings()
    logger.info("Config loaded successfully")
except Exception as e:
    logger.critical(f"Config could not be loaded: {e}")
