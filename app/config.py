import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    REDIS_URL: str
    POSTGRES_SOURCE_URL: str
    NEON_DB_URL: str
    APP_ENV: str = "development"
    LOG_LEVEL: str = "info"

    class Config:
        env_file = ".env"

settings = Settings()
