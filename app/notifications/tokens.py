"""Signed, time-limited tokens for unsubscribe, manage, and confirm links.

Uses itsdangerous with SECRET_KEY from config.py.
- Unsubscribe tokens: never expire (one-click unsubscribe must always work)
- Confirm tokens: never expire (confirmation links must always work)
- Manage tokens: expire after 7 days per PRD Section 7.4
"""

import logging

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.config import settings

logger = logging.getLogger(__name__)

MANAGE_TOKEN_MAX_AGE = 7 * 24 * 60 * 60  # 7 days in seconds

_serializer = URLSafeTimedSerializer(settings.secret_key)

# Salt values prevent token reuse across different purposes
_UNSUBSCRIBE_SALT = "unsubscribe"
_MANAGE_SALT = "manage"
_CONFIRM_SALT = "confirm"


def generate_confirm_token(user_id: str) -> str:
    """Generate a signed confirmation token that never expires."""
    return _serializer.dumps(user_id, salt=_CONFIRM_SALT)


def generate_unsubscribe_token(user_id: str) -> str:
    """Generate a signed unsubscribe token that never expires."""
    return _serializer.dumps(user_id, salt=_UNSUBSCRIBE_SALT)


def generate_manage_token(user_id: str) -> str:
    """Generate a signed manage-subscriptions token (expires in 7 days)."""
    return _serializer.dumps(user_id, salt=_MANAGE_SALT)


def verify_confirm_token(token: str):
    """Verify a confirmation token. Returns user_id or None.

    Confirmation tokens never expire.
    """
    try:
        return _serializer.loads(token, salt=_CONFIRM_SALT)
    except BadSignature:
        logger.warning("Invalid confirm token")
        return None


def verify_unsubscribe_token(token: str):
    """Verify an unsubscribe token. Returns user_id or None.

    Unsubscribe tokens never expire — max_age is not enforced.
    """
    try:
        return _serializer.loads(token, salt=_UNSUBSCRIBE_SALT)
    except BadSignature:
        logger.warning("Invalid unsubscribe token")
        return None


def verify_manage_token(token: str):
    """Verify a manage-subscriptions token. Returns user_id or None.

    Manage tokens expire after 7 days.
    """
    try:
        return _serializer.loads(token, salt=_MANAGE_SALT, max_age=MANAGE_TOKEN_MAX_AGE)
    except SignatureExpired:
        logger.warning("Expired manage token")
        return None
    except BadSignature:
        logger.warning("Invalid manage token")
        return None
