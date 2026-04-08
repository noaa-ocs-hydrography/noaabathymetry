"""Paneled help formatter for argparse — Typer-style output, zero dependencies.

Drop this single file into any Python project to give your argparse CLI
a modern, Typer-style help menu with Unicode box panels, ANSI colors,
and columnar option layout.  No external dependencies — stdlib only.

Quick start
-----------

1. Copy this file into your project.

2. Use ``PaneledArgumentParser`` as a drop-in replacement for ``ArgumentParser``::

    from yourpackage.cli_formatter import PaneledArgumentParser

    parser = PaneledArgumentParser(prog="mytool", description="My cool tool.")
    parser.add_argument("-n", "--name", required=True, help="Your name.")
    parser.add_argument("--count", type=int, default=3, help="Repeat count.")
    parser.add_argument("--verbose", action="store_true", help="Verbose output.")
    args = parser.parse_args()

   Running ``mytool --help`` will produce paneled output like::

        Usage: mytool [OPTIONS]

        My cool tool.

       ╭─ Options ─────────────────────────────────────────────────────╮
       │ --help     -h        show this help message and exit          │
       │ --name     -n  TEXT  Your name.  [required]                   │
       │ --count        INT   Repeat count.  [default: 3]              │
       │ --verbose             Verbose output.                         │
       ╰───────────────────────────────────────────────────────────────╯

Subcommands
-----------

Subparsers automatically inherit paneled help — no extra setup::

    parser = PaneledArgumentParser(prog="mytool", description="My tool.")
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="Run the task.")
    run.add_argument("-d", "--dir", required=True, help="Working directory.")

Both ``mytool --help`` and ``mytool run --help`` produce paneled output.

Customization
-------------

Subclass ``PaneledHelpFormatter`` to change colors, box style, or type
names, then pass it to ``PaneledArgumentParser``::

    class MyFormatter(PaneledHelpFormatter):
        MAX_WIDTH = 100                # wider output
        CYAN = "\\033[38;5;75m"         # lighter blue for flags
        GREEN = "\\033[38;5;114m"       # softer green for commands
        BOX_TL = BOX_TR = BOX_BL = BOX_BR = "+"  # ASCII corners
        BOX_H = "-"
        BOX_V = "|"

    parser = PaneledArgumentParser(
        prog="mytool", formatter_class=MyFormatter,
    )

Environment variables
---------------------

- ``NO_COLOR=1`` — disable all ANSI colors (https://no-color.org)
- ``FORCE_COLOR=1`` — force colors even when not a TTY
- ``TERM=dumb`` — fall back to ASCII box-drawing characters

Features
--------

- Typer-style Unicode box panels with ANSI colors
- 4-column option layout: long flag, short flag, type, description
- Automatic ``[required]`` and ``[default: ...]`` markers
- ``choices`` values appended to help text
- Handles ``--version``, ``FileType``, ``Path``, ``nargs``, positional arguments
- ``append`` / ``extend`` actions shown as repeatable (``TEXT...``)
- ANSI-aware text wrapping (escape codes excluded from width)
- ``%(default)s`` and other argparse template variables resolved
- ASCII fallback for limited terminals
- Safe color detection for macOS, Linux, and Windows
- Subparsers automatically inherit paneled help
- Zero external dependencies — Python 3.9+ stdlib only

Note on argparse internals
--------------------------

This module accesses ``parser._action_groups``, ``group._group_actions``,
``action._choices_actions``, and private action subclasses
(``_SubParsersAction``, ``_StoreTrueAction``, etc.).  These are stable across
CPython 3.9–3.13 but are not part of argparse's public contract.  If a future
Python version changes these internals, ``format_paneled_help`` will need
updating; the argparse fallback (``format_help()``) will still work.
"""

import argparse
import os
import pathlib
import re
import shutil
import sys

__all__ = [
    "PaneledArgumentParser",
    "PaneledHelpFormatter",
    "format_paneled_help",
]

_ANSI_RE = re.compile(r"\033\[[0-9;]*m")

# Action types that take no value (boolean flags).
_FLAG_ACTIONS = (
    argparse._StoreTrueAction,
    argparse._StoreFalseAction,
    argparse._StoreConstAction,
    argparse._CountAction,
)

# Action types that are repeatable (called multiple times).
_REPEAT_ACTIONS = (argparse._AppendAction,)
try:
    _REPEAT_ACTIONS = (*_REPEAT_ACTIONS, argparse._ExtendAction)
except AttributeError:
    pass  # _ExtendAction not available (added in Python 3.8)


def _supports_color():
    """Return True if the terminal likely supports ANSI color output.

    Respects the ``NO_COLOR`` (https://no-color.org) and ``FORCE_COLOR``
    environment variables.  Falls back to ``sys.stdout.isatty()``.
    On Windows, requires a known modern terminal (Windows Terminal or
    VS Code) to avoid emitting raw escape codes on legacy cmd.exe.
    """
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    stdout = getattr(sys, "stdout", None)
    if stdout is None or not hasattr(stdout, "isatty") or not stdout.isatty():
        return False
    if sys.platform == "win32":
        if not (os.environ.get("WT_SESSION") or os.environ.get("TERM_PROGRAM")):
            return False
    return True


def _supports_unicode():
    """Return True if the terminal likely supports Unicode box-drawing."""
    if os.environ.get("TERM") == "dumb":
        return False
    return True


class PaneledHelpFormatter(argparse.HelpFormatter):
    """Argparse formatter that enables paneled help via ``format_paneled_help``.

    Pass as ``formatter_class`` to ``ArgumentParser``.  The actual paneled
    rendering is done by the standalone :func:`format_paneled_help` function;
    this class provides helper methods and satisfies argparse's internal calls
    to ``format_help()`` during parser setup (e.g. ``add_subparsers``).

    Class attributes you may override:

    ``MAX_WIDTH``
        Maximum output width in columns (default 80).
    ``RESET``, ``BOLD``, ``DIM``, ``CYAN``, ``GREEN``, ``RED``,
    ``ORANGE``, ``BRIGHT_YELLOW``
        ANSI escape codes for styling.
    ``BOX_TL``, ``BOX_TR``, ``BOX_BL``, ``BOX_BR``, ``BOX_H``, ``BOX_V``
        Box-drawing characters.  Set to ASCII equivalents (``+``, ``-``,
        ``|``) for terminals that lack Unicode support.
    ``_TYPE_NAMES``
        Dict mapping Python types to display strings (e.g.
        ``{int: "INT", float: "FLOAT"}``).  Add entries for custom types.
    """

    MAX_WIDTH = 80

    # ANSI codes
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[38;5;114m"
    RED = "\033[31m"
    ORANGE = "\033[38;5;216m"
    BRIGHT_YELLOW = "\033[93m"

    # Box-drawing characters (overridable for ASCII fallback)
    BOX_TL = "╭"
    BOX_TR = "╮"
    BOX_BL = "╰"
    BOX_BR = "╯"
    BOX_H = "─"
    BOX_V = "│"

    _TYPE_NAMES = {int: "INT", float: "FLOAT", str: "TEXT", bool: "BOOL"}

    def __init__(self, prog, indent_increment=2, max_help_position=30, width=None):
        if width is None:
            width = shutil.get_terminal_size().columns
        super().__init__(prog, indent_increment, max_help_position,
                         max(min(width, self.MAX_WIDTH), 40))
        self._use_color = _supports_color()
        if not _supports_unicode():
            self.BOX_TL = self.BOX_TR = self.BOX_BL = self.BOX_BR = "+"
            self.BOX_H = "-"
            self.BOX_V = "|"
        self._inner = self._width - 4  # visible content width inside box

    # ------------------------------------------------------------------
    # Color & measurement
    # ------------------------------------------------------------------

    def _c(self, code, text):
        """Wrap *text* in ANSI *code* if color is enabled."""
        return f"{code}{text}{self.RESET}" if self._use_color else text

    @staticmethod
    def _visible_len(text):
        """Return the visible length of *text*, excluding ANSI escape codes."""
        return len(_ANSI_RE.sub("", text))

    # ------------------------------------------------------------------
    # Text wrapping
    # ------------------------------------------------------------------

    def _wrap_text(self, text, width):
        """Word-wrap text to *width* visible characters, returning a list of lines.

        ANSI escape codes are excluded from width calculations.
        Words longer than *width* are force-split to prevent overflow.
        """
        width = max(width, 1)
        if self._visible_len(text) <= width:
            return [text]
        words = text.split()
        lines = []
        current = ""
        current_vis = 0
        for word in words:
            word_vis = self._visible_len(word)
            if word_vis > width:
                if current:
                    lines.append(current)
                    current = ""
                    current_vis = 0
                # Force-split long words (strip ANSI for splitting)
                plain = _ANSI_RE.sub("", word)
                while len(plain) > width:
                    lines.append(plain[:width])
                    plain = plain[width:]
                current = plain
                current_vis = len(plain)
            elif not current:
                current = word
                current_vis = word_vis
            elif current_vis + 1 + word_vis <= width:
                current += " " + word
                current_vis += 1 + word_vis
            else:
                lines.append(current)
                current = word
                current_vis = word_vis
        if current:
            lines.append(current)
        return lines or [""]

    def _wrap_line(self, text, indent=6):
        """ANSI-aware line wrapping to fit inside the box.

        Continuation lines are indented by *indent* spaces.  Includes a
        safety limit to prevent infinite loops on pathological input.
        """
        lines = []
        max_iterations = 50  # safety limit
        iteration = 0
        while self._visible_len(text) > self._inner:
            iteration += 1
            if iteration > max_iterations:
                break

            plain = _ANSI_RE.sub("", text)
            space = plain.rfind(" ", 0, self._inner)
            if space <= 0:
                space = self._inner

            # Map visible position back to the raw (ANSI-containing) string
            visible = 0
            actual = 0
            while visible < space and actual < len(text):
                if text[actual] == "\033":
                    end = text.find("m", actual)
                    if end == -1:
                        actual += 1
                        visible += 1
                        continue
                    actual = end + 1
                else:
                    visible += 1
                    actual += 1

            if actual == 0:
                actual = 1

            lines.append(text[:actual])
            remainder = text[actual:].lstrip()
            if not remainder:
                return lines
            text = " " * indent + remainder
        lines.append(text)
        return lines

    # ------------------------------------------------------------------
    # Box rendering
    # ------------------------------------------------------------------

    def _box(self, title, lines):
        """Render *lines* inside a Unicode box panel with *title*.

        All rows are exactly ``self._width`` visible characters wide::

            ╭─ Title ──────────╮
            │ content          │
            ╰──────────────────╯
        """
        title_vlen = self._visible_len(title)
        fill = max(0, self._width - 5 - title_vlen)
        dashes = self._width - 2
        tl, tr, bl, br = self.BOX_TL, self.BOX_TR, self.BOX_BL, self.BOX_BR
        h, v = self.BOX_H, self.BOX_V

        out = []
        if self._use_color:
            out.append(f"{self.DIM}{tl}{h} {self.RESET}{title}"
                       f"{self.DIM} {h * fill}{tr}{self.RESET}")
        else:
            out.append(f"{tl}{h} {title} {h * fill}{tr}")

        for line in lines:
            for wrapped in self._wrap_line(line):
                vis = self._visible_len(wrapped)
                pad = max(0, self._inner - vis)
                if self._use_color:
                    out.append(f"{self.DIM}{v}{self.RESET} "
                               f"{wrapped}{' ' * pad} "
                               f"{self.DIM}{v}{self.RESET}")
                else:
                    out.append(f"{v} {wrapped}{' ' * pad} {v}")

        if self._use_color:
            out.append(f"{self.DIM}{bl}{h * dashes}{br}{self.RESET}")
        else:
            out.append(f"{bl}{h * dashes}{br}")
        return "\n".join(out)

    # ------------------------------------------------------------------
    # Action introspection
    # ------------------------------------------------------------------

    def _format_flag(self, action):
        """Extract ``(short, long, type_str, help)`` from an argparse action.

        Returns ``None`` for subparser actions (handled separately)
        and for actions with ``help=argparse.SUPPRESS``.
        """
        if isinstance(action, argparse._SubParsersAction):
            return None

        # Suppressed actions should not appear in help
        if action.help is argparse.SUPPRESS:
            return None

        # Version action
        if isinstance(action, argparse._VersionAction):
            short = next((o for o in action.option_strings
                          if not o.startswith("--")), "")
            long_flag = next((o for o in action.option_strings
                              if o.startswith("--")), "--version")
            return (short, long_flag, "",
                    action.help or "Show version and exit.")

        # Help action
        if isinstance(action, argparse._HelpAction):
            short = next((o for o in action.option_strings
                          if not o.startswith("--")), "")
            long_flag = next((o for o in action.option_strings
                              if o.startswith("--")), "--help")
            return (short, long_flag, "",
                    action.help or "Show this message and exit.")

        # Regular options and positional arguments
        short = ""
        long_flag = ""
        # Take the FIRST matching flag (argparse convention: primary name first)
        for opt in action.option_strings:
            if opt.startswith("--") and not long_flag:
                long_flag = opt
            elif not opt.startswith("--") and not short:
                short = opt
        if not short and not long_flag:
            long_flag = action.dest  # positional argument

        # Type indicator
        type_str = ""
        is_flag = isinstance(action, _FLAG_ACTIONS)
        has_options = bool(action.option_strings)
        if not is_flag:
            atype = action.type
            if atype is None and has_options:
                atype = str  # options default to str
            if atype is not None:
                if isinstance(atype, argparse.FileType):
                    type_str = "FILE"
                elif isinstance(atype, type) and issubclass(atype, pathlib.PurePath):
                    type_str = "PATH"
                else:
                    type_str = self._TYPE_NAMES.get(atype, "VALUE")
                # nargs indicators
                if action.nargs in ("+", "*"):
                    type_str = f"[{type_str}]"
                elif action.nargs == "?":
                    type_str = f"{type_str}?"
                elif isinstance(action.nargs, int) and action.nargs > 1:
                    type_str = " ".join([type_str] * action.nargs)
            # Append/extend actions are repeatable
            if isinstance(action, _REPEAT_ACTIONS):
                if not type_str:
                    type_str = "TEXT"
                type_str = f"{type_str}..."

        # Resolve %(default)s, %(type)s, %(prog)s etc. in help strings
        help_text = action.help or ""
        if help_text and "%" in help_text:
            try:
                params = dict(vars(action), prog=self._prog)
                for k, v in list(params.items()):
                    if v is argparse.SUPPRESS:
                        del params[k]
                help_text = help_text % params
            except (KeyError, TypeError, ValueError):
                pass
        if action.choices and not is_flag:
            choices_str = ", ".join(str(c) for c in action.choices)
            if choices_str not in help_text:
                help_text = (f"{help_text}  ({choices_str})"
                             if help_text else f"({choices_str})")

        return (short, long_flag, type_str, help_text)

    # ------------------------------------------------------------------
    # Column formatting
    # ------------------------------------------------------------------

    def _format_commands(self, subcommands):
        """Format subcommands as a 2-column list: name + description."""
        if not subcommands:
            return []
        max_name = max(self._visible_len(n) for n, _ in subcommands)
        col_w = max(max_name + 2, 14)
        desc_w = max(self._inner - col_w, 20)
        lines = []
        for name, desc in subcommands:
            pad = col_w - self._visible_len(name)
            if desc:
                desc_lines = self._wrap_text(desc, desc_w)
                lines.append(f"{name}{' ' * pad}{desc_lines[0]}")
                for wline in desc_lines[1:]:
                    lines.append(f"{' ' * col_w}{wline}")
            else:
                lines.append(name)
        return lines

    def _format_options(self, options):
        """Format options as a 4-column table: long, short, type, description.

        *options* is a list of ``(short, long, type_str, desc)`` tuples.
        """
        if not options:
            return []
        max_long = max((len(lg) for _, lg, _, _ in options), default=0)
        max_short = max((len(sh) for sh, _, _, _ in options), default=0)
        max_type = max((len(tp) for _, _, tp, _ in options), default=0)

        long_w = max_long + 2
        short_w = max_short + 2 if max_short else 0
        type_w = max_type + 2 if max_type else 0
        fixed_w = long_w + short_w + type_w
        desc_w = max(self._inner - fixed_w, 20)

        lines = []
        for short, long_flag, type_str, desc in options:
            long_col = (self._c(self.CYAN, long_flag)
                        + " " * (long_w - len(long_flag)))
            short_col = ""
            if short_w:
                if short:
                    short_col = (self._c(self.CYAN, short)
                                 + " " * (short_w - len(short)))
                else:
                    short_col = " " * short_w
            type_col = ""
            if type_w:
                if type_str:
                    type_col = (self._c(self.DIM, type_str)
                                + " " * (type_w - len(type_str)))
                else:
                    type_col = " " * type_w

            prefix = f"{long_col}{short_col}{type_col}"
            prefix_pad = " " * fixed_w

            if desc:
                desc_lines = self._wrap_text(desc, desc_w)
                lines.append(f"{prefix}{desc_lines[0]}")
                for wline in desc_lines[1:]:
                    lines.append(f"{prefix_pad}{wline}")
            else:
                lines.append(prefix.rstrip())
        return lines

    # ------------------------------------------------------------------
    # Argparse integration
    # ------------------------------------------------------------------

    def format_help(self):
        """Delegate to the default argparse formatter.

        Argparse calls this internally during ``add_subparsers()`` setup.
        The paneled output is produced by :func:`format_paneled_help` instead.
        """
        return super().format_help()


def format_paneled_help(parser):
    """Build a Typer-style paneled help string from an ``ArgumentParser``.

    Reads the parser's action groups directly to produce a formatted help
    string with ANSI colors, Unicode box panels, and columnar option layout.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        The parser (or subparser) to format.

    Returns
    -------
    str
        The formatted help string, ready for ``print()``.
    """
    width = min(shutil.get_terminal_size().columns, PaneledHelpFormatter.MAX_WIDTH)
    fmt = PaneledHelpFormatter(parser.prog, width=width)

    out = [""]

    # Usage line
    has_subparsers = any(isinstance(a, argparse._SubParsersAction)
                         for a in parser._actions)
    if has_subparsers:
        usage_rest = f"{parser.prog} [OPTIONS] COMMAND [ARGS]..."
    else:
        usage_rest = f"{parser.prog} [OPTIONS]"
    out.append(f" {fmt._c(fmt.BOLD + fmt.ORANGE, 'Usage:')} {usage_rest}")
    out.append("")

    # Description
    if parser.description:
        out.append(f" {parser.description}")
        out.append("")

    # Action groups
    for group in parser._action_groups:
        subcommands = []
        options = []

        for action in group._group_actions:
            if isinstance(action, argparse._SubParsersAction):
                for name in action.choices:
                    desc = ""
                    for sub_action in action._choices_actions:
                        if sub_action.dest == name:
                            desc = sub_action.help or ""
                            break
                    subcommands.append((fmt._c(fmt.GREEN, name), desc))
                continue

            result = fmt._format_flag(action)
            if result is None:
                continue
            short, long_flag, type_str, desc = result

            # Default / required markers
            is_positional = not action.option_strings
            markers = []
            if action.required and not is_positional:
                markers.append(fmt._c(fmt.BRIGHT_YELLOW, "[required]"))
            elif is_positional and action.nargs in ("*", "?"):
                markers.append(fmt._c(fmt.DIM, "[optional]"))
            else:
                is_flag = isinstance(action, _FLAG_ACTIONS)
                is_special = isinstance(action, (argparse._HelpAction,
                                                 argparse._VersionAction))
                if not is_flag and not is_special:
                    default = action.default
                    if default not in (None, argparse.SUPPRESS):
                        default_str = str(default)
                        if default_str not in ("", "[]", "()"):
                            markers.append(
                                fmt._c(fmt.DIM, f"[default: {default}]"))
            if markers:
                desc = (desc + "  " + " ".join(markers)
                        if desc else " ".join(markers))
            options.append((short, long_flag, type_str, desc))

        if subcommands:
            styled = fmt._c(fmt.ORANGE, "Commands")
            out.append(fmt._box(styled, fmt._format_commands(subcommands)))

        if options:
            title = (group.title or "options").title()
            styled = fmt._c(fmt.ORANGE, title)
            out.append(fmt._box(styled, fmt._format_options(options)))

    # Epilog
    if parser.epilog:
        out.append(f" {parser.epilog}")

    out.append("")
    return "\n".join(out)


class PaneledArgumentParser(argparse.ArgumentParser):
    """``ArgumentParser`` with Typer-style paneled help output.

    Drop-in replacement for ``argparse.ArgumentParser``.  ``--help``
    produces paneled output with ANSI colors and columnar layout.
    Subparsers created via ``add_subparsers`` automatically inherit
    paneled help — no extra configuration needed.

    Example::

        parser = PaneledArgumentParser(prog="mytool", description="My tool.")
        parser.add_argument("--verbose", action="store_true", help="Verbose.")

        sub = parser.add_subparsers(dest="command")
        run = sub.add_parser("run", help="Run the task.")
        run.add_argument("-d", "--dir", required=True, help="Directory.")
        # run --help is also paneled automatically
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("formatter_class", PaneledHelpFormatter)
        super().__init__(*args, **kwargs)

    def print_help(self, file=None):
        """Print paneled help to *file* (default: ``sys.stdout``)."""
        if file is None:
            file = sys.stdout
        file.write(format_paneled_help(self))
        file.write("\n")

    def error(self, message):
        """Print a styled error message with usage hint, then exit.

        Falls back to argparse's default ``error()`` if formatting fails,
        so error information is never lost.
        """
        try:
            fmt = PaneledHelpFormatter(self.prog)
            error_label = fmt._c(fmt.BOLD + fmt.RED, "Error:")
            usage = f"{self.prog} [OPTIONS]"
            if any(isinstance(a, argparse._SubParsersAction)
                   for a in self._actions):
                usage += " COMMAND [ARGS]..."
            usage_label = fmt._c(fmt.BOLD + fmt.ORANGE, "Usage:")
            error_lines = message.split("\n")
            error_block = [f" {error_label} {error_lines[0]}"]
            for continuation in error_lines[1:]:
                error_block.append(f" {continuation}")
            lines = [
                "",
                f" {usage_label} {usage}",
                "",
                *error_block,
                "",
                f" Try '{self.prog} --help' for more information.",
                "",
            ]
            self.exit(2, "\n".join(lines) + "\n")
        except Exception:
            super().error(message)

    def add_subparsers(self, **kwargs):
        """Add subparsers that automatically use paneled help."""
        kwargs.setdefault("parser_class", PaneledArgumentParser)
        return super().add_subparsers(**kwargs)
