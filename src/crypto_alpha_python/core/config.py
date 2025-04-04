"""
Application configuration settings.
"""
from typing import List
from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Crypto Market Alpha Engine"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/api/v1"
    
    # CORS Configuration
    CORS_ORIGINS: List[AnyHttpUrl] = []
    
    @field_validator("CORS_ORIGINS", mode='before')
    @classmethod
    def assemble_cors_origins(cls, v: str | List[str]) -> List[str] | str:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)
    
    # Database Configuration
    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    SQLALCHEMY_DATABASE_URI: str | None = None
    
    @field_validator("SQLALCHEMY_DATABASE_URI", mode='before')
    @classmethod
    def assemble_db_connection(cls, v: str | None, info) -> str:
        if isinstance(v, str):
            return v
        values = info.data
        return f"postgresql+asyncpg://{values.get('POSTGRES_USER')}:{values.get('POSTGRES_PASSWORD')}@{values.get('POSTGRES_SERVER')}/{values.get('POSTGRES_DB')}"
    
    # Redis Configuration
    REDIS_HOST: str
    REDIS_PORT: int
    
    # Exchange API Keys
    BINANCE_API_KEY: str | None = None
    BINANCE_API_SECRET: str | None = None
    COINBASE_API_KEY: str | None = None
    COINBASE_API_SECRET: str | None = None
    
    @field_validator("COINBASE_API_KEY", "COINBASE_API_SECRET")
    @classmethod
    def validate_coinbase_keys(cls, v: str | None) -> str | None:
        if v is not None:
            # Remove any whitespace and newlines
            v = v.strip()
            # Check if the key is empty after stripping
            if not v:
                return None
        return v
    
    # JWT Configuration
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env",
        extra='allow'
    )

settings = Settings() 