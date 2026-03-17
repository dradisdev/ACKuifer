"""PFAS result models for both data sources."""

import uuid

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import UUID

from app.database import Base


class PfasResult(Base):
    """Board of Health / Laserfiche residential well test results."""
    __tablename__ = "pfas_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    laserfiche_doc_id = Column(Integer, unique=True, nullable=False)
    map_number = Column(String)
    parcel_number = Column(String)
    neighborhood = Column(String)
    street_name = Column(String)
    sample_date = Column(Date)
    pfos = Column(Numeric, nullable=True)
    pfoa = Column(Numeric, nullable=True)
    pfhxs = Column(Numeric, nullable=True)
    pfna = Column(Numeric, nullable=True)
    pfhpa = Column(Numeric, nullable=True)
    pfda = Column(Numeric, nullable=True)
    pfas6_sum = Column(Numeric, nullable=True)
    j_qualifier_present = Column(Boolean, default=False)
    pass_fail = Column(String)  # 'PASS' / 'FAIL' / 'UNKNOWN'
    result_status = Column(String)  # 'NON-DETECT' / 'DETECT' / 'HIGH-DETECT' / 'HAZARD'
    discovered_at = Column(DateTime(timezone=True), server_default=func.now())
    notified_at = Column(DateTime(timezone=True), nullable=True)


class SourceDiscoveryResult(Base):
    """MassDEP Source Discovery investigation results (RTN 4-0029612)."""
    __tablename__ = "source_discovery_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_doc_url = Column(Text)
    sample_location = Column(Text)
    sample_date = Column(Date)
    pfos = Column(Numeric(8, 2), nullable=True)
    pfoa = Column(Numeric(8, 2), nullable=True)
    pfhxs = Column(Numeric(8, 2), nullable=True)
    pfna = Column(Numeric(8, 2), nullable=True)
    pfhpa = Column(Numeric(8, 2), nullable=True)
    pfda = Column(Numeric(8, 2), nullable=True)
    pfas6_sum = Column(Numeric(8, 2), nullable=True)
    result_status = Column(String)  # 'NON-DETECT' / 'DETECT' / 'HIGH-DETECT' / 'HAZARD'
    neighborhood = Column(String, nullable=True)
    latitude = Column(Numeric(9, 6), nullable=True)
    longitude = Column(Numeric(9, 6), nullable=True)
    depth = Column(Text, nullable=True)
    medium = Column(Text, nullable=True)  # 'groundwater', 'drinking_water', 'soil'
    geocode_review_needed = Column(Boolean, default=False)
    notified_at = Column(DateTime(timezone=True), nullable=True)
