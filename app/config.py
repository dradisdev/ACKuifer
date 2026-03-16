"""Application configuration — all named constants and env var loading."""

import logging
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str

    # Security
    admin_password: str = "changeme"
    secret_key: str = "changeme"

    # Email (Resend)
    resend_api_key: str = ""
    email_from: str = "alerts@ackuifer.org"

    # SMS (Twilio)
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""

    # Base URL for links in emails (no trailing slash)
    base_url: str = "http://localhost:8080"

    # Mapbox
    mapbox_public_token: str = ""

    # Operator
    operator_email: str = ""
    deadmans_window_days: int = 10

    # Scraper schedules
    laserfiche_cron_schedule: str = "0 8 * * *"
    massdep_cron_schedule: str = "0 9 * * *"

    # Laserfiche portal constants
    laserfiche_base_url: str = "https://portal.laserfiche.com"
    laserfiche_repo_id: str = "r-ec7bdbfe"
    laserfiche_root_folder_id: str = "145009"


# --- Named constants (regulatory / business logic) ---

# Massachusetts PFAS6 maximum contaminant level (ppt)
MCL: float = 20.0

# 80% of MCL; triggers standalone SMS alert
SMS_THRESHOLD: float = 16.0

# MassDEP Imminent Hazard threshold; triggers HAZARD status
IH_THRESHOLD: float = 90.0

# Months of inactivity before re-confirmation email
INACTIVITY_MONTHS: int = 12

# Days to retain record after unsubscribe before deletion
RETENTION_DAYS_AFTER_UNSUBSCRIBE: int = 30

# Days within which same-street results are treated as retests in digest
RETEST_WINDOW_DAYS: int = 45

# Fallback neighborhood for unresolved parcels
FALLBACK_NEIGHBORHOOD: str = "Nantucket (Island-wide)"

# Expected neighborhoods from OSM
NEIGHBORHOODS: list[str] = [
    "Nantucket",
    "Madaket",
    "Cisco",
    "Surfside",
    "Tom Nevers",
    "Sconset",
    "Quidnet",
    "Wauwinet",
    "Polpis",
]


def classify_result_status(pfas6_sum: Optional[float]) -> str:
    """Four-tier result status classification applied at parse time."""
    if pfas6_sum is None or pfas6_sum == 0:
        return "NON-DETECT"
    if pfas6_sum <= MCL:
        return "DETECT"
    if pfas6_sum < IH_THRESHOLD:
        return "HIGH-DETECT"
    return "HAZARD"


def check_municipal_water(lat: float, lng: float) -> bool:
    """Check if coordinates fall within municipal water service area.

    STUBBED: Always returns False. Needs a real service area polygon
    from Nantucket Water Works or Town GIS before this can work.
    """
    logger.warning(
        "Municipal water check is stubbed — no service area polygon available. "
        "All addresses will be treated as private well. "
        "Obtain Nantucket Water Works service area GeoJSON to enable this feature."
    )
    return False


# Singleton settings instance — import this
settings = Settings()
