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

    # Security. No usable default (S-1): on Railway, a service that needs one of
    # these refuses to start when it's unset or a placeholder (app/secret_guard.py).
    admin_password: str = ""
    secret_key: str = ""

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
    # Coverage guard (D-2). A full run is suspect if it took under this share
    # of the median duration of the successful runs before it.
    deadmans_short_run_ratio: float = 0.5
    deadmans_duration_baseline_runs: int = 5
    # Sources whose run time is steady enough for the duration check.
    deadmans_duration_check_sources: tuple[str, ...] = ("laserfiche",)

    # Scraper schedules (UTC). laserfiche_cron_schedule must match the
    # production laserfiche-cron schedule in the Railway dashboard: the N-11
    # start guard only lets a crawl run within the window after these times.
    laserfiche_cron_schedule: str = "0 8 1,15 * *"
    laserfiche_schedule_window_minutes: int = 60
    # N-11: LASERFICHE_FORCE_RUN=1 runs a full crawl on start, whatever the time
    # (e.g. a staging crawl). Remove it afterwards, or every deploy will crawl.
    laserfiche_force_run: bool = False
    # D-5 (operator 2026-11-18): MassDEP runs DAILY by design. A run takes
    # ~10s, and daily picks up new DEP filings sooner. Railway reads the
    # schedule from cronSchedule in railway.massdep.toml; keep the two equal.
    # Unlike Laserfiche there is no start guard: a deploy start also runs it.
    massdep_cron_schedule: str = "0 9 * * *"
    # D-5: the EEA document list is paged (50 per page; 40 items on
    # 2026-11-18). The scraper clicks "Next" until it is disabled. A run that
    # can't finish paging is recorded "degraded" so the dead man's switch alerts.
    massdep_max_pages: int = 20
    # Longest wait for the list to change after clicking "Next".
    massdep_page_change_timeout_ms: int = 10000
    massdep_page_poll_ms: int = 250

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
    # Folder names listed in a degraded run's error message (the rest are counted).
    laserfiche_degraded_folder_names_max: int = 50
    # CATCHUP_RUN=1 starts a full crawl on deploy, outside the schedule (N-11), and
    # records the run as a catch-up. It does not label results (see
    # OLDER_REPORT_MIN_AGE_DAYS).
    catchup_run: bool = False


# --- DB identity guard (--expect-env) ---
# Public host of each environment's Postgres (Railway TCP proxy), the host the
# connection actually used. Guarded scripts (app/db_identity.py) refuse to run
# unless it matches the environment named with --expect-env. Unlike the server
# address, the host survives a Postgres restart (S-3b). It changes only if the
# TCP proxy is removed or regenerated in Railway; update here if it does.
# Connections over Railway's private network use another host, so they refuse.
DB_ENV_FINGERPRINTS: dict = {
    "staging": "zephyr.proxy.rlwy.net",
    "prod": "caboose.proxy.rlwy.net",
}
DB_IDENTITY_NAME: str = "railway"
# Sanity range for pfas_results, not the identity check: it only catches an empty
# or freshly restored database. Kept wide so catch-up growth never trips it.
DB_ENV_ROW_RANGES: dict = {
    "staging": (300, 10000),
    "prod": (300, 10000),
}

# --- S-1: fail closed on missing secrets ---
# Values that count as unset for ADMIN_PASSWORD / SECRET_KEY (compared after
# stripping whitespace, case-insensitive): empty, the old default, and the
# .env.example placeholders.
PLACEHOLDER_SECRET_VALUES: tuple = (
    "",
    "changeme",
    "your_admin_password_here",
    "your_secret_key_here",
)
# Railway sets this on every service; when present the guard enforces (refuses
# to start), otherwise (local development) it only warns.
SECRET_GUARD_ENFORCE_ENV_VAR: str = "RAILWAY_ENVIRONMENT_NAME"
# Settings each service must have. web checks admin logins and link tokens; the
# scraper crons sign digest manage/unsubscribe links; healthcheck-cron reads neither.
SECRET_GUARD_REQUIRED: dict = {
    "web": ("admin_password", "secret_key"),
    "laserfiche-cron": ("secret_key",),
    "massdep-cron": ("secret_key",),
    "healthcheck-cron": (),
}

# D-8: two PFAS values (ppt) count as equal in the seeded-row re-parse check when
# they differ by no more than this (stored Numeric vs parsed float rounding).
REPARSE_MATCH_TOLERANCE_PPT: float = 0.005

# --- Named constants (regulatory / business logic) ---

# Scrape run statuses whose parsed results are valid to notify subscribers about.
# "degraded" missed some folders and "partial" covered one map, but what they
# parsed is real.
NOTIFIABLE_RUN_STATUSES: tuple = ("success", "degraded", "partial")

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

# F-3: privacy page. Linked from every public page footer and every email footer.
PRIVACY_PATH: str = "/privacy"
# Public contact address (Squarespace forwards it to the operator).
CONTACT_EMAIL: str = "hello@ackuifer.org"
# Shown on the privacy page; change it whenever the page text changes.
PRIVACY_LAST_UPDATED: str = "November 28, 2026"

# Days within which same-street results are treated as retests in digest
RETEST_WINDOW_DAYS: int = 60

# Admin PFAS6 discrepancy review: flag records where
# abs(stored pfas6_sum - raw sum of the six components) exceeds this (ppt).
# 0.05 suppresses floating-point dust and trivial rounding; certified-total
# vs raw-sum gaps above this are listed for review against the source PDF.
DISCREPANCY_TOLERANCE_PPT: float = 0.05

# D-3b: a Laserfiche result is labelled "older report, newly added" in digest
# emails and SMS (never on the map) when its sample date is more than this many
# days before ACKuifer found it, whichever run found it. Production 2026-05..11:
# 3 of 37 normally-found reports were over 90 days (median 24-63 days).
OLDER_REPORT_MIN_AGE_DAYS: int = 90
# "{date}" is the sample date, e.g. "May 26, 2026".
OLDER_REPORT_LABEL: str = (
    "Older report, newly added: sampled {date}. This report was filed earlier "
    "but only recently added to ACKuifer."
)
OLDER_REPORT_SHORT: str = "Older report, newly added"

# D-3 street names: a trailing abbreviated suffix is spelled out for display and
# on new results (key: lower case, no trailing dot). Only the last word changes,
# so a leading "St" (Saint) is never touched.
STREET_SUFFIXES: dict = {
    "rd": "Road", "ave": "Avenue", "av": "Avenue", "ln": "Lane", "dr": "Drive",
    "st": "Street", "ct": "Court", "cir": "Circle", "pl": "Place", "ter": "Terrace",
    "blvd": "Boulevard", "hwy": "Highway",
}
# Known corrections (whole street name, case-insensitive), applied after the
# suffix map. The map search only finds the spelled-out ordinal streets, so
# they are also applied to search queries.
# NEVER merge different real streets here. Known pairs that must stay separate:
#   Pochick Avenue (Surfside) and Pochick Street (Siasconset).
#   Miacomet Avenue and Miacomet Road; West Miacomet Avenue and West Miacomet Road;
#   Somerset Road and Somerset Lane (operator 2026-11-17: all six exist).
STREET_NAME_CORRECTIONS: dict = {
    "2nd Way": "Second Way",
    "3rd Way": "Third Way",
    "4th Way": "Fourth Way",
    "5th Way": "Fifth Way",
    # Operator-confirmed on the map 2026-11-16: every "Millbrook" result is on Millbrook Road.
    "Millbrook": "Millbrook Road",
}
# D-8 known corrections, decided by the operator 2026-11-17 from the staging
# street-name report (whole street name after the rules above, case-insensitive).
# Display only, NOT applied to search: several keys are also village or place
# names ("Madaket", "Quidnet", "Tom Nevers"), and a search for those must still
# find the place. Same never-merge rule as above.
STREET_NAME_DISPLAY_CORRECTIONS: dict = {
    # Missing suffix
    "Dukes": "Dukes Road",
    "Madaket": "Madaket Road",
    "Okorwaw": "Okorwaw Avenue",
    "Quidnet": "Quidnet Road",
    "Sesapana": "Sesapana Road",
    "Tennessee": "Tennessee Avenue",
    "Vestal": "Vestal Street",
    "Chuck Hollow": "Chuck Hollow Road",
    "Wood Hollow": "Wood Hollow Road",
    "Hummock Pond": "Hummock Pond Road",
    "Tom Nevers": "Tom Nevers Road",
    "Eel Point": "Eel Point Road",  # operator checked the map: all on Eel Point Road
    # Apostrophes
    "Helen's Drive": "Helens Drive",
    "Lover's Lane": "Lovers Lane",
    "Pippen's Way": "Pippens Way",
    # Wrong suffix (the other street does not exist)
    "Long Pond Road": "Long Pond Drive",
    "Shawkemo Hills Road": "Shawkemo Hills Lane",
    # Typo (both spellings resolve to the same place)
    "Briar Patch Road": "Brier Patch Road",
    # Free text after the street
    "Camp Richard - well 1": "Camp Richard",
    "Rugged Road - Main House -": "Rugged Road",
}

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
