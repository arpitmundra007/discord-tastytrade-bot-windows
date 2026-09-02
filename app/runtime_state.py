"""
Process-wide runtime state that the dashboard can flip live, without a
restart - currently just the pause/kill switch. Kept separate from Settings
because this is transient (resets on restart) rather than configuration.
"""
from __future__ import annotations
import time

_paused = False
_started_at = time.monotonic()
_discord_error: str | None = None
_discord_identity: str | None = None
_position_alert: str | None = None


def is_paused() -> bool:
    return _paused


def set_paused(value: bool):
    global _paused
    _paused = value


def uptime_seconds() -> float:
    return round(time.monotonic() - _started_at, 1)


def get_discord_error() -> str | None:
    return _discord_error


def set_discord_error(value: str | None):
    global _discord_error
    _discord_error = value


def get_discord_identity() -> str | None:
    """The connected Discord account's display identity (e.g. 'someuser' or
    'someuser#1234'), set once on a successful login. Deliberately NOT
    cleared on a routine gateway disconnect/reconnect (see on_disconnect in
    discord_selfbot.py) - a brief network hiccup shouldn't make the
    dashboard show 'not logged in' when the account is actually fine and
    about to auto-reconnect. Only clearing this (see set_discord_identity
    below) or a full process restart resets it - matching this module's
    existing philosophy that transient state here should reflect what's
    actually true, not flicker on every minor blip."""
    return _discord_identity


def set_discord_identity(value: str | None):
    global _discord_identity
    _discord_identity = value


def get_position_alert() -> str | None:
    return _position_alert


def set_position_alert(value: str | None):
    """A loud, sticky dashboard warning - reserved for 'a position may
    currently be unprotected' situations (e.g. the partial-take-profit leg
    filled but the follow-up breakeven-stop/runner order failed to submit
    after retries). Cleared explicitly once the situation is confirmed
    resolved - never auto-expires, since a stale silent state here is worse
    than an over-persistent warning."""
    global _position_alert
    _position_alert = value
