"""Tests for the command usage tracking and rate limiting module."""

import datetime
import sqlite3
from unittest import mock

import pytest

from nbs.noaabathymetry._internal.ratelimit import (
    ensure_usage_table,
    log_command,
    check_rate_limit,
    _utc_window_starts,
    _seconds_until_next,
    _format_wait,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn():
    """Create an in-memory SQLite DB with the command_usage table."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_usage_table(conn)
    return conn


def _get_row(conn, command):
    """Return the command_usage row for *command* as a dict, or None."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM command_usage WHERE command = ?", (command,))
    row = cur.fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# ensure_usage_table
# ---------------------------------------------------------------------------


class TestEnsureUsageTable:
    def test_creates_table(self):
        conn = sqlite3.connect(":memory:")
        ensure_usage_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='command_usage'")
        assert cur.fetchone() is not None

    def test_idempotent(self):
        conn = sqlite3.connect(":memory:")
        ensure_usage_table(conn)
        ensure_usage_table(conn)  # no error


# ---------------------------------------------------------------------------
# _utc_window_starts
# ---------------------------------------------------------------------------


class TestUtcWindowStarts:
    def test_returns_three_strings(self):
        m, h, d = _utc_window_starts()
        assert isinstance(m, str)
        assert isinstance(h, str)
        assert isinstance(d, str)

    def test_minute_has_zero_seconds(self):
        m, _, _ = _utc_window_starts()
        # ISO format: YYYY-MM-DDTHH:MM:SS
        assert m.endswith(":00")

    def test_hour_has_zero_minutes_seconds(self):
        _, h, _ = _utc_window_starts()
        assert h.endswith(":00:00")

    def test_day_starts_at_midnight(self):
        _, _, d = _utc_window_starts()
        assert "T00:00:00" in d


# ---------------------------------------------------------------------------
# _seconds_until_next
# ---------------------------------------------------------------------------


class TestSecondsUntilNext:
    def test_minute_is_positive(self):
        s = _seconds_until_next("minute")
        assert 1 <= s <= 60

    def test_hour_is_positive(self):
        s = _seconds_until_next("hour")
        assert 1 <= s <= 3600

    def test_day_is_positive(self):
        s = _seconds_until_next("day")
        assert 1 <= s <= 86400


# ---------------------------------------------------------------------------
# _format_wait
# ---------------------------------------------------------------------------


class TestFormatWait:
    def test_seconds(self):
        assert _format_wait(1) == "1 second"
        assert _format_wait(45) == "45 seconds"

    def test_minutes(self):
        assert _format_wait(60) == "1 minute"
        assert _format_wait(120) == "2 minutes"
        assert _format_wait(3599) == "59 minutes"

    def test_hours(self):
        assert _format_wait(3600) == "1 hour"
        assert _format_wait(7200) == "2 hours"


# ---------------------------------------------------------------------------
# log_command
# ---------------------------------------------------------------------------


class TestLogCommand:
    def test_first_call_inserts_row(self):
        conn = _make_conn()
        log_command(conn, "status")
        row = _get_row(conn, "status")
        assert row is not None
        assert row["minute_count"] == 1
        assert row["hour_count"] == 1
        assert row["day_count"] == 1

    def test_second_call_increments(self):
        conn = _make_conn()
        log_command(conn, "status")
        log_command(conn, "status")
        row = _get_row(conn, "status")
        assert row["minute_count"] == 2
        assert row["hour_count"] == 2
        assert row["day_count"] == 2

    def test_different_commands_separate(self):
        conn = _make_conn()
        log_command(conn, "fetch")
        log_command(conn, "status")
        assert _get_row(conn, "fetch")["minute_count"] == 1
        assert _get_row(conn, "status")["minute_count"] == 1

    def test_new_minute_resets_minute_count(self):
        conn = _make_conn()
        log_command(conn, "status")
        # Simulate a new minute by changing the stored minute_start
        conn.execute(
            "UPDATE command_usage SET minute_start = '2000-01-01T00:00:00' WHERE command = 'status'")
        conn.commit()
        log_command(conn, "status")
        row = _get_row(conn, "status")
        assert row["minute_count"] == 1  # reset
        assert row["hour_count"] == 2    # still same hour

    def test_new_hour_resets_hour_count(self):
        conn = _make_conn()
        log_command(conn, "status")
        conn.execute(
            "UPDATE command_usage SET hour_start = '2000-01-01T00:00:00' WHERE command = 'status'")
        conn.commit()
        log_command(conn, "status")
        row = _get_row(conn, "status")
        assert row["hour_count"] == 1  # reset
        assert row["day_count"] == 2   # still same day

    def test_new_day_resets_day_count(self):
        conn = _make_conn()
        log_command(conn, "status")
        conn.execute(
            "UPDATE command_usage SET day_start = '2000-01-01T00:00:00' WHERE command = 'status'")
        conn.commit()
        log_command(conn, "status")
        row = _get_row(conn, "status")
        assert row["day_count"] == 1  # reset

    def test_fails_open_on_error(self):
        conn = _make_conn()
        conn.close()  # closed connection
        log_command(conn, "status")  # should not raise


# ---------------------------------------------------------------------------
# check_rate_limit
# ---------------------------------------------------------------------------


class TestCheckRateLimit:
    def test_under_limit_passes(self):
        conn = _make_conn()
        check_rate_limit(conn, "status")  # should not raise

    def test_minute_limit_exceeded(self):
        conn = _make_conn()
        # Set count to the limit
        m, h, d = _utc_window_starts()
        conn.execute(
            "INSERT INTO command_usage(command, minute_start, minute_count, "
            "hour_start, hour_count, day_start, day_count) "
            "VALUES(?, ?, 50, ?, 50, ?, 50)",
            ("status", m, h, d))
        conn.commit()
        with pytest.raises(ValueError, match="slow down"):
            check_rate_limit(conn, "status")

    def test_hour_limit_exceeded(self):
        conn = _make_conn()
        m, h, d = _utc_window_starts()
        conn.execute(
            "INSERT INTO command_usage(command, minute_start, minute_count, "
            "hour_start, hour_count, day_start, day_count) "
            "VALUES(?, ?, 1, ?, 250, ?, 250)",
            ("status", m, h, d))
        conn.commit()
        with pytest.raises(ValueError, match="slow down"):
            check_rate_limit(conn, "status")

    def test_day_limit_exceeded(self):
        conn = _make_conn()
        m, h, d = _utc_window_starts()
        conn.execute(
            "INSERT INTO command_usage(command, minute_start, minute_count, "
            "hour_start, hour_count, day_start, day_count) "
            "VALUES(?, ?, 1, ?, 1, ?, 1200)",
            ("status", m, h, d))
        conn.commit()
        with pytest.raises(ValueError, match="slow down"):
            check_rate_limit(conn, "status")

    def test_minute_checked_first(self):
        """Minute limit triggers before hour limit even if both exceeded."""
        conn = _make_conn()
        m, h, d = _utc_window_starts()
        conn.execute(
            "INSERT INTO command_usage(command, minute_start, minute_count, "
            "hour_start, hour_count, day_start, day_count) "
            "VALUES(?, ?, 50, ?, 200, ?, 1000)",
            ("status", m, h, d))
        conn.commit()
        with pytest.raises(ValueError, match="second"):
            check_rate_limit(conn, "status")

    def test_stale_window_resets(self):
        """Counts from a past window should reset, not block."""
        conn = _make_conn()
        # All counts high but windows are from the past
        conn.execute(
            "INSERT INTO command_usage(command, minute_start, minute_count, "
            "hour_start, hour_count, day_start, day_count) "
            "VALUES('status', '2000-01-01T00:00:00', 999, "
            "'2000-01-01T00:00:00', 999, '2000-01-01T00:00:00', 999)")
        conn.commit()
        check_rate_limit(conn, "status")  # should not raise (counts reset to 1)

    def test_fails_open_on_error(self):
        conn = _make_conn()
        conn.close()
        check_rate_limit(conn, "status")  # should not raise

    def test_error_message_includes_wait_time(self):
        conn = _make_conn()
        m, h, d = _utc_window_starts()
        conn.execute(
            "INSERT INTO command_usage(command, minute_start, minute_count, "
            "hour_start, hour_count, day_start, day_count) "
            "VALUES(?, ?, 50, ?, 50, ?, 50)",
            ("status", m, h, d))
        conn.commit()
        with pytest.raises(ValueError, match="next request in"):
            check_rate_limit(conn, "status")

    def test_non_rate_limited_command_not_blocked(self):
        """log_command for fetch/mosaic should not raise even at high counts."""
        conn = _make_conn()
        for _ in range(60):
            log_command(conn, "fetch")
        # fetch is never rate-limited, only logged
        row = _get_row(conn, "fetch")
        assert row["minute_count"] == 60


# ---------------------------------------------------------------------------
# Clock edge cases
# ---------------------------------------------------------------------------


class TestClockEdgeCases:
    def test_clock_backward_resets(self):
        """If stored window is in the future (clock jumped back), reset."""
        conn = _make_conn()
        # Store a window start in the future
        conn.execute(
            "INSERT INTO command_usage(command, minute_start, minute_count, "
            "hour_start, hour_count, day_start, day_count) "
            "VALUES('status', '2099-01-01T00:00:00', 50, "
            "'2099-01-01T00:00:00', 200, '2099-01-01T00:00:00', 1000)")
        conn.commit()
        # Current window won't match the future window, so counts reset
        check_rate_limit(conn, "status")  # should not raise
        row = _get_row(conn, "status")
        assert row["minute_count"] == 1
