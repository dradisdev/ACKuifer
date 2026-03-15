"""Scraper tracking models — document dedup and run history."""

import uuid

from sqlalchemy import Column, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID

from app.database import Base


class SeenDocument(Base):
    """Dedup registry for both scrapers.

    For Laserfiche: keyed by doc ID (stored as string).
    For Source Discovery: keyed by EEA PDF URL.
    """
    __tablename__ = "seen_documents"

    doc_key = Column(String, primary_key=True)  # Laserfiche doc ID or EEA PDF URL
    source = Column(String, nullable=False)  # 'laserfiche' | 'massdep'
    discovered_at = Column(DateTime(timezone=True), server_default=func.now())
    parse_status = Column(String, default="pending")  # 'success' / 'error' / 'pending'
    error_message = Column(Text, nullable=True)


class ScrapeRun(Base):
    """Scraper run history and status."""
    __tablename__ = "scrape_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String, nullable=False)  # 'laserfiche' | 'massdep'
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String, default="running")  # 'running' / 'success' / 'error'
    new_docs_found = Column(Integer, default=0)
    new_docs_parsed = Column(Integer, default=0)
    parse_errors = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
