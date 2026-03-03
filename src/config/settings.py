"""Settings and configuration management using Pydantic."""

from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from .env file."""

    # Database
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "ycm"
    db_user: str = "postgres"
    db_password: str = ""
    
    # API
    youtube_api_key: str
    youtube_api_base_url: str = "https://www.googleapis.com/youtube/v3"
    
    # ETL Settings
    batch_size: int = 100
    max_retries: int = 3
    request_timeout: int = 30
    
    # Paths
    data_dir: Path = Path("data")
    logs_dir: Path = Path("data/logs")
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Environment
    environment: str = "development"  # development, staging, production
    debug: bool = False
    
    class Config:
        """Pydantic config."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @property
    def db_url(self) -> str:
        """Build database URL."""
        return (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )


# Global settings instance
settings = Settings()
