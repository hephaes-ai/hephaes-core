"""SQLAlchemy models for the backend application."""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for backend database models."""
