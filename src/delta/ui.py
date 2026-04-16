"""User interaction utilities: phases, issues, confirmations, formatted output.

ALL visible output goes through logging.getLogger('delta') so it appears in
both the terminal and log files identically. The ONLY exception is the
progress bar, which uses sys.stderr with \\r (terminal only).
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from enum import Enum

import click

from delta.exceptions import AbortedError

logger = logging.getLogger("delta")

# Ensure delta logger always has a stderr handler (for read-only commands
# that don't call setup_logging). LogManager replaces this on start().
if not logger.handlers:
    _default_handler = logging.StreamHandler(sys.stderr)
    _default_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_default_handler)
    logger.setLevel(logging.DEBUG)


def _log(msg: str) -> None:
    """Log a message through the delta logger (goes to terminal + log file)."""
    logger.info("%s", msg)


def _styled(msg: str) -> str:
    """Apply style for terminal. DualHandler strips ANSI for the log file."""
    return msg


# ======================================================================
# Issue collector — accumulates warnings and errors for final summary
# ======================================================================

class IssueLevel(str, Enum):
    WARNING = "warning"
    ERROR = "error"


@dataclass
class Issue:
    level: IssueLevel
    phase: str
    message: str


class IssueCollector:
    """Collects warnings and errors across all phases for a final summary."""

    def __init__(self) -> None:
        self._issues: list[Issue] = []

    def warning(self, phase: str, message: str) -> None:
        self._issues.append(Issue(IssueLevel.WARNING, phase, message))
        print_warning(message)

    def error(self, phase: str, message: str) -> None:
        self._issues.append(Issue(IssueLevel.ERROR, phase, message))
        print_error(message)

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self._issues if i.level == IssueLevel.WARNING]

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self._issues if i.level == IssueLevel.ERROR]

    @property
    def has_issues(self) -> bool:
        return bool(self._issues)

    def print_summary(self) -> None:
        """Print a summary of all collected issues."""
        if not self._issues:
            return

        w_count = len(self.warnings)
        e_count = len(self.errors)

        border_color = "red" if e_count else "yellow"
        _log("")
        _log(click.style(f"  {'─' * 50}", fg=border_color))
        parts = []
        if e_count:
            parts.append(f"{e_count} error(s)")
        if w_count:
            parts.append(f"{w_count} warning(s)")
        _log(click.style(f"  SUMMARY: {', '.join(parts)}", fg=border_color, bold=True))
        _log(click.style(f"  {'─' * 50}", fg=border_color))
        for issue in self._issues:
            icon = "✗" if issue.level == IssueLevel.ERROR else "⚠"
            color = "red" if issue.level == IssueLevel.ERROR else "yellow"
            _log(
                click.style(f"  [{issue.phase}]", dim=True)
                + click.style(f" {icon} {issue.message}", fg=color)
            )
        _log(click.style(f"  {'─' * 50}", fg=border_color))


# Global collector — reset per command
_collector: IssueCollector | None = None


def start_collector() -> IssueCollector:
    global _collector
    _collector = IssueCollector()
    return _collector


def get_collector() -> IssueCollector | None:
    return _collector


# ======================================================================
# Phase headers
# ======================================================================

def print_phase(title: str) -> None:
    _log("")
    _log(click.style(f"  ── {title} ──", fg="cyan", bold=True))


def print_subphase(title: str) -> None:
    _log(click.style(f"     {title}", fg="cyan"))


# ======================================================================
# Basic output — all through logger
# ======================================================================

def print_header(text: str) -> None:
    _log(click.style(f"\n{'─' * 60}", fg="cyan"))
    _log(click.style(f"  {text}", fg="cyan", bold=True))
    _log(click.style(f"{'─' * 60}", fg="cyan"))


def print_success(text: str) -> None:
    _log(click.style(f"  ✓ {text}", fg="green"))


def print_warning(text: str) -> None:
    _log(click.style(f"  ⚠ {text}", fg="yellow"))


def print_error(text: str) -> None:
    _log(click.style(f"  ✗ {text}", fg="red"))


def print_info(text: str) -> None:
    _log(click.style(f"  {text}", fg="white"))


def print_dim(text: str) -> None:
    _log(click.style(f"    {text}", dim=True))


def print_cmd_output(line: str) -> None:
    _log(click.style(f"      > {line}", dim=True))


def print_cmd_stderr(line: str) -> None:
    _log(click.style(f"      ! {line}", fg="yellow"))


def print_file_change(change_type: str, path: str) -> None:
    colors = {"M": "yellow", "A": "green", "D": "red", "L": "cyan"}
    color = colors.get(change_type, "white")
    _log(click.style(f"    {change_type} ", fg=color, bold=True) + path)


# ======================================================================
# Progress bar — terminal only (NOT in log file)
# ======================================================================

_progress_len = 0


def show_progress(text: str) -> None:
    """Show a progress line that overwrites itself. Terminal only."""
    global _progress_len
    # Single write: clear + new content — no flicker
    padded = text.ljust(_progress_len)
    sys.stderr.write(f"\r{padded}")
    sys.stderr.flush()
    _progress_len = max(_progress_len, len(text))


def clear_progress() -> None:
    """Clear the progress line."""
    global _progress_len
    if _progress_len:
        sys.stderr.write("\r" + " " * _progress_len + "\r")
        sys.stderr.flush()
        _progress_len = 0


# ======================================================================
# Confirmations
# ======================================================================

def _log_to_file(msg: str) -> None:
    """Log only to log file, not terminal. For input() prompts that are already visible."""
    for handler in logger.handlers:
        if hasattr(handler, '_log_file') and handler._log_file:
            handler._log_file.write(msg + "\n")
            handler._log_file.flush()


def confirm(message: str, *, auto_yes: bool = False) -> bool:
    if auto_yes:
        _log(f"  {message} [y/n]: y (auto)")
        return True
    while True:
        response = input(f"  {message} [y/n]: ").strip().lower()
        _log_to_file(f"  {message} [y/n]: {response}")
        if response == "y":
            return True
        if response == "n":
            return False
        _log("  Please enter 'y' or 'n'.")


def confirm_or_abort(message: str, *, auto_yes: bool = False) -> None:
    if not confirm(message, auto_yes=auto_yes):
        raise AbortedError("Operation aborted by user.")


# ======================================================================
# Formatting
# ======================================================================

def format_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}" if unit != "B" else f"{size_bytes} {unit}"
        size_bytes /= 1024  # type: ignore[assignment]
    return f"{size_bytes:.1f} TB"


def format_duration(seconds: float) -> str:
    """Format seconds as m:ss or h:mm:ss."""
    s = int(seconds)
    if s < 3600:
        return f"{s // 60}:{s % 60:02d}"
    return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def format_time_ago(iso_timestamp: str) -> str:
    """Format an ISO timestamp as relative time or date.

    <24h: '5 minutes ago', '2 hours ago'
    >=24h: '2025-03-23 15:30'
    """
    from datetime import datetime
    try:
        ts = datetime.fromisoformat(iso_timestamp)
        delta = datetime.now() - ts
        seconds = delta.total_seconds()
        if seconds < 60:
            return "just now"
        if seconds < 3600:
            m = int(seconds / 60)
            return f"{m} minute{'s' if m != 1 else ''} ago"
        if seconds < 86400:
            h = int(seconds / 3600)
            return f"{h} hour{'s' if h != 1 else ''} ago"
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso_timestamp


def format_size_colored(old_size: int, new_size: int) -> str:
    """Format size change with color: green if smaller, red if larger."""
    diff = new_size - old_size
    new_str = format_size(new_size)
    if diff > 0:
        return click.style(f"{new_str} (+{format_size(diff)})", fg="red")
    elif diff < 0:
        return click.style(f"{new_str} ({format_size(abs(diff))} smaller)", fg="green")
    return new_str


def print_entity_list(entities: list[dict], active: str = "") -> None:
    if not entities:
        print_info("(none)")
        return
    for e in entities:
        marker = click.style("* ", fg="green", bold=True) if e["name"] == active else "  "
        type_tag = click.style(
            f"[{e['type']}]",
            fg="blue" if e["type"] == "baseline" else "magenta",
        )
        name = click.style(e["name"], bold=True)
        desc = ""
        if e.get("description"):
            desc = click.style(f" — {e['description']}", dim=True)
        _log(f"{marker}{type_tag} {name}{desc}")
