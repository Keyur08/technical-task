import os
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    # Database Configuration
    database_url: str = Field(
        default="postgresql://wind_solar_user:test%40123@localhost:5432/wind_solar_db",
        env="DATABASE_URL",
        description="Database connection URL"
    )
    
    # API Configuration
    api_base_url: str = Field(
        default="https://data.elexon.co.uk/bmrs/api/v1/generation/actual/per-type/wind-and-solar",
        env="API_BASE_URL",
        description="Elexon API base URL"
    )
    
    request_timeout: int = Field(
        default=60,
        env="REQUEST_TIMEOUT",
        description="HTTP request timeout in seconds"
    )
    
    rate_limit_delay: float = Field(
        default=1.0,
        env="RATE_LIMIT_DELAY",
        description="Delay between API requests in seconds"
    )
    
    max_chunk_days: int = Field(
        default=6,
        env="MAX_CHUNK_DAYS",
        description="Maximum days per API request chunk"
    )
    
    # FastAPI Configuration
    app_title: str = Field(
        default="Wind & Solar Data Pipeline API",
        env="APP_TITLE"
    )
    
    app_version: str = Field(
        default="1.0.0",
        env="APP_VERSION"
    )
    
    host: str = Field(
        default="0.0.0.0",
        env="HOST"
    )
    
    port: int = Field(
        default=8000,
        env="PORT"
    )
    
    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        env="LOG_LEVEL",
        description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    
    log_file: str = Field(
        default="wind_solar_pipeline.log",
        env="LOG_FILE",
        description="Log file path"
    )
       
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

settings = Settings()

def validate_settings():
    """Validate critical settings."""
    required_settings = [
        'database_url',
        'api_base_url'
    ]
    
    for setting in required_settings:
        if not getattr(settings, setting):
            raise ValueError(f"Required setting '{setting}' is not configured")
    
validate_settings()