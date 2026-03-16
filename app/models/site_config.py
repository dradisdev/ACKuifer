"""Editable site configuration stored in database."""

from sqlalchemy import Column, DateTime, String, Text, func

from app.database import Base


class SiteConfig(Base):
    """Key-value store for operator-editable content.

    Used for PFAS info page links, municipal water warning text,
    and any other content that should be editable from the admin panel
    without a code deploy.
    """
    __tablename__ = "site_config"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=False, default="")
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
