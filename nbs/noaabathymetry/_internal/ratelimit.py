"""
ratelimit.py - Command usage tracking and rate limiting.

Tracks per-command invocation counts in fixed time windows (minute, hour, day)
using a single row per command in the ``command_usage`` table.  All commands
log their usage; only specific commands (e.g. status) enforce limits.

Fails open — any error in the rate limit machinery is logged and swallowed
so that the actual command always proceeds.
"""

import datetime
import logging
import sqlite3

logger = logging.getLogger("noaabathymetry")

# Rate limits for the status command.
_STATUS_LIMITS = {
    "minute": {"max": 50, "seconds": 60},
    "hour": {"max": 200, "seconds": 3600},
    "day": {"max": 1000, "seconds": 86400},
}


def ensure_usage_table(conn):
    """Create the ``command_usage`` table if it does not exist."""
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS command_usage (
                command TEXT PRIMARY KEY,
                minute_start TEXT,
                minute_count INTEGER DEFAULT 0,
                hour_start TEXT,
                hour_count INTEGER DEFAULT 0,
                day_start TEXT,
                day_count INTEGER DEFAULT 0
            );
        """)
        conn.commit()
    except sqlite3.Error:
        pass


def _utc_window_starts():
    """Return the current minute, hour, and day window start times (UTC ISO 8601)."""
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    minute_start = now.replace(second=0, microsecond=0).isoformat()
    hour_start = now.replace(minute=0, second=0, microsecond=0).isoformat()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    return minute_start, hour_start, day_start


def _seconds_until_next(window):
    """Return seconds until the next window boundary for *window* ('minute', 'hour', 'day')."""
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    if window == "minute":
        next_boundary = (now + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
    elif window == "hour":
        next_boundary = (now + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        next_boundary = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1, int((next_boundary - now).total_seconds()))


def _format_wait(seconds):
    """Format a wait duration as a human-readable string."""
    if seconds < 60:
        return f"{seconds} seconds" if seconds != 1 else "1 second"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} minutes" if minutes != 1 else "1 minute"
    hours = seconds // 3600
    return f"{hours} hours" if hours != 1 else "1 hour"


def log_command(conn, command):
    """Log a command invocation by incrementing the usage counter.

    For each time window (minute, hour, day): if the stored window start
    matches the current window, the count is incremented.  If it's a
    different window (past or future due to clock changes), the count
    resets to 1.

    Fails open — any error is logged and swallowed.
    """
    try:
        minute_start, hour_start, day_start = _utc_window_starts()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT minute_start, minute_count, hour_start, hour_count, "
            "day_start, day_count FROM command_usage WHERE command = ?",
            (command,),
        )
        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                "INSERT INTO command_usage(command, minute_start, minute_count, "
                "hour_start, hour_count, day_start, day_count) "
                "VALUES(?, ?, 1, ?, 1, ?, 1)",
                (command, minute_start, hour_start, day_start),
            )
        else:
            m_start, m_count, h_start, h_count, d_start, d_count = row
            m_count = m_count + 1 if m_start == minute_start else 1
            h_count = h_count + 1 if h_start == hour_start else 1
            d_count = d_count + 1 if d_start == day_start else 1
            cursor.execute(
                "UPDATE command_usage SET "
                "minute_start = ?, minute_count = ?, "
                "hour_start = ?, hour_count = ?, "
                "day_start = ?, day_count = ? "
                "WHERE command = ?",
                (minute_start, m_count, hour_start, h_count,
                 day_start, d_count, command),
            )
        conn.commit()
    except Exception as e:
        logger.debug("Rate limit tracking failed: %s", e)


def check_rate_limit(conn, command):
    """Log the command and check rate limits.

    Raises ``ValueError`` if any limit is exceeded, with a message
    indicating how long to wait.  Only call this for commands that
    should be rate-limited (e.g. status).

    Fails open — if the check itself errors, the command proceeds.
    """
    try:
        log_command(conn, command)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT minute_start, minute_count, hour_start, hour_count, "
            "day_start, day_count FROM command_usage WHERE command = ?",
            (command,),
        )
        row = cursor.fetchone()
        if row is None:
            return

        minute_start, hour_start, day_start = _utc_window_starts()
        m_start, m_count, h_start, h_count, d_start, d_count = row

        checks = [
            (m_start == minute_start, m_count, _STATUS_LIMITS["minute"]["max"], "minute"),
            (h_start == hour_start, h_count, _STATUS_LIMITS["hour"]["max"], "hour"),
            (d_start == day_start, d_count, _STATUS_LIMITS["day"]["max"], "day"),
        ]
        for in_window, count, limit, window in checks:
            if in_window and count > limit:
                wait = _seconds_until_next(window)
                raise ValueError(
                    "Please slow down. You are making too many requests. "
                    f"You can make your next request in {_format_wait(wait)}."
                )
    except ValueError:
        raise
    except Exception as e:
        logger.debug("Rate limit check failed: %s", e)
