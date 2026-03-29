"""SQLAlchemy models for OAuth service."""
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Boolean, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class OAuthClient(Base):
    __tablename__ = "oauth_clients"

    client_id = Column(String(128), primary_key=True)
    client_secret_hash = Column(String(256), nullable=False)
    name = Column(String(256), nullable=False)
    description = Column(Text, nullable=True)
    scopes = Column(Text, nullable=False, default="catalog:read")  # space-separated
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class KeyStore(Base):
    __tablename__ = "key_store"

    key_id = Column(String(128), primary_key=True)
    private_pem = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
