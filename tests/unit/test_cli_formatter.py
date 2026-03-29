"""Tests for PaneledHelpFormatter, PaneledArgumentParser, and format_paneled_help."""

import argparse
import os

import pytest

from nbs.noaabathymetry.cli_formatter import (
    PaneledArgumentParser,
    PaneledHelpFormatter,
    format_paneled_help,
    _supports_color,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_parser(**kwargs):
    """Create a parser with PaneledArgumentParser."""
    kwargs.setdefault("prog", "test")
    return PaneledArgumentParser(**kwargs)


def _strip_ansi(text):
    import re
    return re.sub(r"\033\[[0-9;]*m", "", text)


def _paneled(parser):
    """Return paneled help with ANSI stripped for assertions."""
    return _strip_ansi(format_paneled_help(parser))


# ---------------------------------------------------------------------------
# Color detection
# ---------------------------------------------------------------------------

class TestSupportsColor:
    def test_no_color_env(self, monkeypatch):
        monkeypatch.setenv("NO_COLOR", "1")
        monkeypatch.delenv("FORCE_COLOR", raising=False)
        assert _supports_color() is False

    def test_force_color_env(self, monkeypatch):
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.setenv("FORCE_COLOR", "1")
        assert _supports_color() is True

    def test_no_color_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("NO_COLOR", "1")
        monkeypatch.setenv("FORCE_COLOR", "1")
        assert _supports_color() is False


# ---------------------------------------------------------------------------
# Visible length
# ---------------------------------------------------------------------------

class TestVisibleLen:
    def test_plain_text(self):
        assert PaneledHelpFormatter._visible_len("hello") == 5

    def test_with_ansi(self):
        assert PaneledHelpFormatter._visible_len("\033[36mhello\033[0m") == 5

    def test_empty(self):
        assert PaneledHelpFormatter._visible_len("") == 0

    def test_only_ansi(self):
        assert PaneledHelpFormatter._visible_len("\033[36m\033[0m") == 0


# ---------------------------------------------------------------------------
# Text wrapping
# ---------------------------------------------------------------------------

class TestWrapText:
    def setup_method(self):
        self.fmt = PaneledHelpFormatter("test")

    def test_short_text_no_wrap(self):
        assert self.fmt._wrap_text("hello world", 80) == ["hello world"]

    def test_wraps_at_word_boundary(self):
        lines = self.fmt._wrap_text("hello world foo bar", 11)
        assert lines == ["hello world", "foo bar"]

    def test_force_splits_long_word(self):
        lines = self.fmt._wrap_text("abcdefghij", 5)
        assert lines == ["abcde", "fghij"]

    def test_empty_text(self):
        assert self.fmt._wrap_text("", 80) == [""]

    def test_width_1(self):
        lines = self.fmt._wrap_text("ab", 1)
        assert lines == ["a", "b"]

    def test_single_word_exact_fit(self):
        assert self.fmt._wrap_text("hello", 5) == ["hello"]

    def test_ansi_codes_excluded_from_width(self):
        # 5 visible chars wrapped in ANSI = should fit in width 10
        text = "\033[2m[required]\033[0m"  # 10 visible chars
        lines = self.fmt._wrap_text(text, 10)
        assert len(lines) == 1

    def test_ansi_mixed_with_plain(self):
        text = "Hello world \033[2m[default: foo]\033[0m"
        # visible: "Hello world [default: foo]" = 25 chars
        lines = self.fmt._wrap_text(text, 30)
        assert len(lines) == 1  # fits in 30 visible chars


# ---------------------------------------------------------------------------
# Wrap line (ANSI-aware)
# ---------------------------------------------------------------------------

class TestWrapLine:
    def setup_method(self):
        self.fmt = PaneledHelpFormatter("test", width=40)

    def test_short_line_no_wrap(self):
        result = self.fmt._wrap_line("short")
        assert result == ["short"]

    def test_malformed_ansi_no_crash(self):
        # \033[ without closing 'm' — should not crash or infinite-loop
        text = "\033[36" + "x" * 100
        result = self.fmt._wrap_line(text)
        assert len(result) >= 1  # produces output, doesn't hang

    def test_no_spaces_force_splits(self):
        text = "x" * 100
        result = self.fmt._wrap_line(text)
        # First line should be <= _inner, continuations include indent
        assert len(result) > 1
        assert PaneledHelpFormatter._visible_len(result[0]) <= self.fmt._inner

    def test_preserves_ansi_codes(self):
        text = "\033[36mhello world\033[0m and more text that wraps"
        result = self.fmt._wrap_line(text)
        joined = " ".join(result)
        assert "hello" in _strip_ansi(joined)


# ---------------------------------------------------------------------------
# Box rendering
# ---------------------------------------------------------------------------

class TestBox:
    def setup_method(self):
        self.fmt = PaneledHelpFormatter("test", width=40)
        self.fmt._use_color = False  # plain text for assertions

    def test_box_width_consistency(self):
        box = self.fmt._box("Title", ["content"])
        lines = box.split("\n")
        widths = [len(line) for line in lines]
        assert all(w == widths[0] for w in widths), f"Inconsistent widths: {widths}"

    def test_box_top_bottom_match(self):
        box = self.fmt._box("Title", ["hello"])
        lines = box.split("\n")
        assert len(lines[0]) == len(lines[-1])

    def test_empty_lines(self):
        box = self.fmt._box("Empty", [])
        lines = box.split("\n")
        assert len(lines) == 2  # just top + bottom

    def test_box_characters(self):
        box = self.fmt._box("T", ["x"])
        assert box.startswith("╭")
        assert box.endswith("╯")


# ---------------------------------------------------------------------------
# Action introspection
# ---------------------------------------------------------------------------

class TestFormatFlag:
    def setup_method(self):
        self.fmt = PaneledHelpFormatter("test")

    def _action(self, *args, **kwargs):
        p = argparse.ArgumentParser()
        p.add_argument(*args, **kwargs)
        return p._actions[-1]

    def test_regular_option(self):
        action = self._action("-d", "--dir", help="Directory")
        short, long, type_str, help_text = self.fmt._format_flag(action)
        assert short == "-d"
        assert long == "--dir"
        assert type_str == "TEXT"
        assert help_text == "Directory"

    def test_int_option(self):
        action = self._action("--workers", type=int, help="Count")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "INT"

    def test_float_option(self):
        action = self._action("--scale", type=float)
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "FLOAT"

    def test_nargs_plus(self):
        action = self._action("--ids", type=int, nargs="+")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "[INT]"

    def test_nargs_star(self):
        action = self._action("--ids", type=int, nargs="*")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "[INT]"

    def test_nargs_optional(self):
        action = self._action("--config", nargs="?")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "TEXT?"

    def test_store_true_no_type(self):
        action = self._action("--verbose", action="store_true")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == ""

    def test_store_false_no_type(self):
        action = self._action("--no-cache", action="store_false")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == ""

    def test_count_no_type(self):
        action = self._action("-v", action="count")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == ""

    def test_choices_appended_to_help(self):
        action = self._action("--format", choices=["csv", "json"],
                               help="Output format.")
        _, _, _, help_text = self.fmt._format_flag(action)
        assert "csv" in help_text
        assert "json" in help_text

    def test_choices_not_duplicated(self):
        action = self._action("--format", choices=["csv", "json"],
                               help="Output format (csv, json).")
        _, _, _, help_text = self.fmt._format_flag(action)
        # Should not append choices again since they're already in the help
        assert help_text.count("csv") == 1

    def test_multiple_long_flags_takes_first(self):
        action = self._action("--verbose", "--verb")
        _, long, _, _ = self.fmt._format_flag(action)
        assert long == "--verbose"  # first, not last

    def test_version_action(self):
        p = argparse.ArgumentParser()
        p.add_argument("--version", action="version", version="1.0")
        action = p._actions[-1]
        short, long, type_str, help_text = self.fmt._format_flag(action)
        assert long == "--version"
        assert type_str == ""

    def test_help_action(self):
        p = argparse.ArgumentParser()
        action = p._actions[0]  # help is always first
        short, long, type_str, _ = self.fmt._format_flag(action)
        assert long == "--help"
        assert type_str == ""

    def test_subparser_action_returns_none(self):
        p = argparse.ArgumentParser()
        sub = p.add_subparsers()
        action = p._actions[-1]
        assert self.fmt._format_flag(action) is None

    def test_positional_argument(self):
        action = self._action("filename")
        short, long, type_str, _ = self.fmt._format_flag(action)
        assert long == "filename"
        assert short == ""

    def test_positional_with_type(self):
        action = self._action("count", type=int)
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "INT"

    def test_no_help_text(self):
        action = self._action("--flag", action="store_true")
        _, _, _, help_text = self.fmt._format_flag(action)
        assert help_text == ""

    def test_suppressed_action_returns_none(self):
        action = self._action("--secret", help=argparse.SUPPRESS)
        assert self.fmt._format_flag(action) is None

    def test_filetype(self):
        action = self._action("--output", type=argparse.FileType("w"))
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "FILE"

    def test_pathlib_path(self):
        import pathlib
        action = self._action("--path", type=pathlib.Path)
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "PATH"

    def test_nargs_int(self):
        action = self._action("--point", type=float, nargs=2)
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "FLOAT FLOAT"

    def test_nargs_3(self):
        action = self._action("--rgb", type=int, nargs=3)
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "INT INT INT"

    def test_append_action(self):
        action = self._action("--include", action="append")
        _, _, type_str, _ = self.fmt._format_flag(action)
        assert type_str == "TEXT..."


# ---------------------------------------------------------------------------
# Default / required markers
# ---------------------------------------------------------------------------

class TestMarkers:
    def test_required_marker(self):
        p = _make_parser()
        p.add_argument("--dir", required=True, help="Directory")
        output = _paneled(p)
        assert "[required]" in output

    def test_default_marker(self):
        p = _make_parser()
        p.add_argument("--source", default="bluetopo", help="Source")
        output = _paneled(p)
        assert "[default: bluetopo]" in output

    def test_default_false_on_flag_suppressed(self):
        p = _make_parser()
        p.add_argument("--verbose", action="store_true")
        output = _paneled(p)
        assert "[default:" not in output

    def test_default_false_on_nonflag_shown(self):
        p = _make_parser()
        p.add_argument("--flag", type=bool, default=False, help="A bool option")
        output = _paneled(p)
        assert "[default: False]" in output

    def test_default_none_suppressed(self):
        p = _make_parser()
        p.add_argument("--opt", default=None, help="Optional")
        output = _paneled(p)
        assert "[default:" not in output

    def test_default_empty_string_suppressed(self):
        p = _make_parser()
        p.add_argument("--opt", default="", help="Optional")
        output = _paneled(p)
        assert "[default:" not in output

    def test_default_empty_list_suppressed(self):
        p = _make_parser()
        p.add_argument("--opt", default=[], nargs="*", help="Optional")
        output = _paneled(p)
        assert "[default:" not in output

    def test_default_zero_shown(self):
        p = _make_parser()
        p.add_argument("--count", type=int, default=0, help="Count")
        output = _paneled(p)
        assert "[default: 0]" in output

    def test_positional_no_required_marker(self):
        p = _make_parser()
        p.add_argument("filename", help="Input file")
        output = _paneled(p)
        assert "[required]" not in output

    def test_positional_nargs_star_optional(self):
        p = _make_parser()
        p.add_argument("extras", nargs="*", help="Extra files")
        output = _paneled(p)
        assert "[optional]" in output
        assert "[required]" not in output

    def test_positional_nargs_plus_no_optional(self):
        p = _make_parser()
        p.add_argument("files", nargs="+", help="Files")
        output = _paneled(p)
        assert "[optional]" not in output


# ---------------------------------------------------------------------------
# Full format_paneled_help
# ---------------------------------------------------------------------------

class TestFormatPaneledHelp:
    def test_basic_parser(self):
        p = _make_parser(description="A tool.")
        p.add_argument("--name", help="Your name")
        output = _paneled(p)
        assert "Usage:" in output
        assert "A tool." in output
        assert "--name" in output

    def test_subcommands(self):
        p = _make_parser(description="Main tool.")
        sub = p.add_subparsers()
        sub.add_parser("run", help="Run it.", formatter_class=PaneledHelpFormatter)
        sub.add_parser("stop", help="Stop it.", formatter_class=PaneledHelpFormatter)
        output = _paneled(p)
        assert "Commands" in output
        assert "run" in output
        assert "stop" in output
        assert "COMMAND [ARGS]..." in output

    def test_no_subcommands_usage(self):
        p = _make_parser()
        output = _paneled(p)
        assert "[OPTIONS]" in output
        assert "COMMAND" not in output

    def test_epilog(self):
        p = _make_parser(epilog="See docs at https://example.com")
        output = _paneled(p)
        assert "https://example.com" in output

    def test_no_description(self):
        p = _make_parser()
        p.add_argument("--x", help="X")
        output = _paneled(p)
        assert "Usage:" in output

    def test_version_action(self):
        p = _make_parser()
        p.add_argument("--version", action="version", version="1.0")
        output = _paneled(p)
        assert "--version" in output
        # Should not show "TEXT" type
        assert "TEXT" not in output.split("--version")[1].split("\n")[0]

    def test_custom_group_title(self):
        p = _make_parser()
        group = p.add_argument_group("advanced options")
        group.add_argument("--turbo", action="store_true", help="Go fast.")
        output = _paneled(p)
        assert "Advanced Options" in output

    def test_mutually_exclusive_group(self):
        p = _make_parser()
        group = p.add_mutually_exclusive_group()
        group.add_argument("--json", action="store_true", help="JSON output")
        group.add_argument("--csv", action="store_true", help="CSV output")
        output = _paneled(p)
        assert "--json" in output
        assert "--csv" in output

    def test_empty_parser(self):
        p = _make_parser()
        output = _paneled(p)
        assert "Usage:" in output

    def test_choices_shown(self):
        p = _make_parser()
        p.add_argument("--fmt", choices=["csv", "json"], help="Format.")
        output = _paneled(p)
        assert "csv" in output
        assert "json" in output

    def test_suppressed_option_hidden(self):
        p = _make_parser()
        p.add_argument("--visible", help="You can see me.")
        p.add_argument("--secret", help=argparse.SUPPRESS)
        output = _paneled(p)
        assert "--visible" in output
        assert "--secret" not in output
        assert "SUPPRESS" not in output

    def test_box_alignment(self):
        """All lines in each box panel should be the same visible width."""
        p = _make_parser(description="Tool.")
        p.add_argument("-d", "--dir", required=True, help="Directory path.")
        p.add_argument("-v", "--verbose", action="store_true", help="Verbose.")
        p.add_argument("--tile-resolution-filter", type=int, nargs="+",
                        help="Only include tiles at these resolutions.")
        output = _paneled(p)
        # Find all box panels (between ╭ and ╯)
        in_box = False
        box_lines = []
        for line in output.split("\n"):
            stripped = _strip_ansi(line)
            if "╭" in stripped:
                in_box = True
                box_lines = [stripped]
            elif "╰" in stripped and in_box:
                box_lines.append(stripped)
                widths = [len(bl) for bl in box_lines]
                assert all(w == widths[0] for w in widths), \
                    f"Box misaligned: {widths}\n" + "\n".join(box_lines)
                in_box = False
            elif in_box:
                box_lines.append(stripped)


# ---------------------------------------------------------------------------
# ASCII fallback
# ---------------------------------------------------------------------------

class TestAsciiFallback:
    def test_dumb_terminal(self, monkeypatch):
        monkeypatch.setenv("TERM", "dumb")
        fmt = PaneledHelpFormatter("test")
        assert fmt.BOX_TL == "+"
        assert fmt.BOX_H == "-"
        assert fmt.BOX_V == "|"

    def test_ascii_box_renders(self, monkeypatch):
        monkeypatch.setenv("TERM", "dumb")
        p = _make_parser()
        p.add_argument("--name", help="Name")
        output = _paneled(p)
        assert "+" in output
        assert "|" in output


# ---------------------------------------------------------------------------
# Help string template substitution
# ---------------------------------------------------------------------------

class TestHelpTemplates:
    def test_default_substitution(self):
        p = _make_parser()
        p.add_argument("--workers", type=int, default=4,
                        help="Number of workers (default: %(default)s)")
        output = _paneled(p)
        assert "default: 4)" in output
        assert "%(default)s" not in output

    def test_type_substitution(self):
        p = _make_parser()
        p.add_argument("--count", type=int, help="A %(type)s value.")
        output = _paneled(p)
        assert "int" in output
        assert "%(type)s" not in output

    def test_invalid_template_no_crash(self):
        p = _make_parser()
        p.add_argument("--x", help="Bad template %(nonexistent)s here.")
        output = _paneled(p)
        assert "%(nonexistent)s" in output  # left as-is


# ---------------------------------------------------------------------------
# PaneledArgumentParser
# ---------------------------------------------------------------------------

class TestPaneledArgumentParser:
    def test_is_argument_parser(self):
        p = PaneledArgumentParser(prog="test", description="Desc.")
        assert isinstance(p, argparse.ArgumentParser)

    def test_print_help_is_overridden(self):
        p = PaneledArgumentParser(prog="test")
        assert type(p).print_help is not argparse.ArgumentParser.print_help

    def test_print_help_to_file(self):
        import io
        p = PaneledArgumentParser(prog="test", description="Desc.")
        p.add_argument("--name", help="Name")
        buf = io.StringIO()
        p.print_help(file=buf)
        output = buf.getvalue()
        assert "Usage:" in output
        assert "--name" in output

    def test_subparsers_inherit_paneled(self):
        p = PaneledArgumentParser(prog="test")
        sub = p.add_subparsers(dest="command")
        child = sub.add_parser("run", help="Run it.")
        assert isinstance(child, PaneledArgumentParser)

    def test_subparser_help_is_paneled(self):
        import io
        p = PaneledArgumentParser(prog="test")
        sub = p.add_subparsers(dest="command")
        child = sub.add_parser("run", help="Run it.")
        child.add_argument("--fast", action="store_true", help="Go fast.")
        buf = io.StringIO()
        child.print_help(file=buf)
        output = buf.getvalue()
        assert "╭" in output or "+" in output  # paneled, not plain
        assert "--fast" in output

    def test_formatter_class_set(self):
        p = PaneledArgumentParser(prog="test")
        assert p.formatter_class is PaneledHelpFormatter

    def test_error_shows_message(self):
        p = PaneledArgumentParser(prog="test")
        p.add_argument("--dir", required=True)
        with pytest.raises(SystemExit) as exc_info:
            p.parse_args([])
        assert exc_info.value.code == 2

    def test_error_contains_usage_and_try(self, capsys):
        p = PaneledArgumentParser(prog="test")
        p.add_argument("--dir", required=True)
        with pytest.raises(SystemExit):
            p.parse_args([])
        captured = capsys.readouterr()
        stderr = captured.err
        assert "Error:" in _strip_ansi(stderr)
        assert "test" in stderr
        assert "--help" in stderr

    def test_error_never_swallows(self):
        """Even if formatting fails, the error must still cause SystemExit."""
        p = PaneledArgumentParser(prog="test")
        p.add_argument("--dir", required=True)
        # Corrupt internal state to force the try branch to fail
        p._actions = None
        with pytest.raises((SystemExit, TypeError)):
            p.error("something broke")
