from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://oauth:oauth@localhost:5432/oauth"
    rsa_key_bits: int = 2048
    access_token_expire_seconds: int = 3600
    issuer: str = "http://oauth-service:8081"
    jwks_kid: str = "catalog-sync-key-1"
    admin_token: str = "admin-secret-change-me"  # for management endpoints

    class Config:
        env_file = ".env"


settings = Settings()
