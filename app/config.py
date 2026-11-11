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
    deadmans_window_days: int = 20
    # Master switch for dead man's switch alerting. Default True (fail-safe: a
    # forgotten env var still alerts). Set DEADMANS_ALERTING_ENABLED=false per
    # environment (e.g. staging, whose scrape cron is off) to silence alerts
    # there without a code change.
    deadmans_alerting_enabled: bool = True

    # Scraper schedules
    laserfiche_cron_schedule: str = "0 8 1,15 * *"
    massdep_cron_schedule: str = "0 9 1,15 * *"

    # Laserfiche portal constants
    laserfiche_base_url: str = "https://portal.laserfiche.com"
    laserfiche_repo_id: str = "r-ec7bdbfe"
    laserfiche_root_folder_id: str = "145009"

    # Laserfiche crawler behaviour (D-1). The folder list is a virtual scroller
    # that only renders rows on screen, so the crawler scrolls it to the bottom.
    # The tall viewport is a second line of defence: more rows render per screen.
    laserfiche_viewport_width: int = 1280
    laserfiche_viewport_height: int = 4000
    # Hard cap on scroll passes per folder. Map 66 (477 entries) needs ~40 at 720px.
    laserfiche_max_scroll_passes: int = 600
    # Stop once at the bottom and this many passes in a row found no new links.
    laserfiche_stale_scroll_passes: int = 2
    laserfiche_scroll_settle_ms: int = 300
    # Longest wait for the list to redraw after a scroll (normally two frames).
    laserfiche_scroll_redraw_timeout_ms: int = 500
    # Extra attempts after a folder load fails or times out.
    laserfiche_nav_retries: int = 2
    # Pause before every folder load, to stay under the portal's rate limit
    # (Cloudflare bot check, first hit 2026-11-10). Each 1000 ms adds roughly
    # 7 hours to a full run (~26k folder loads), so keep it small.
    laserfiche_folder_delay_ms: int = 500
    # Wait after a bot-check page before retrying the same folder.
    laserfiche_block_backoff_seconds: int = 120
    # Stop the run (status "error") after this many folders in a row stay blocked.
    laserfiche_max_consecutive_blocks: int = 3


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
RETEST_WINDOW_DAYS: int = 60

# Admin PFAS6 discrepancy review: flag records where
# abs(stored pfas6_sum - raw sum of the six components) exceeds this (ppt).
# 0.05 suppresses floating-point dust and trivial rounding; certified-total
# vs raw-sum gaps above this are listed for review against the source PDF.
DISCREPANCY_TOLERANCE_PPT: float = 0.05

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
