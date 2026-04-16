"""Logging system: dual output to console and per-run log files."""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import TextIO

import click

from delta import ui


class DualHandler(logging.Handler):
    """Handler that writes to both console (via click.echo) and an optional file."""

    def __init__(self, log_file: TextIO | None = None):
        super().__init__()
        self.log_file = log_file
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            # Use click.echo for terminal output (captured by CliRunner in tests)
            click.echo(msg)
            if self.log_file:
                # Strip ANSI codes for file output
                clean = click.unstyle(msg)
                self.log_file.write(clean + "\n")
                self.log_file.flush()
        except Exception:
            self.handleError(record)


class LogManager:
    """Manages per-run log files and the delta logger."""

    def __init__(self, logs_dir: Path, filename_pattern: str, max_count: int, max_size_mb: int):
        self.logs_dir = logs_dir
        self.filename_pattern = filename_pattern
        self.max_count = max_count
        self.max_size_mb = max_size_mb
        self._log_file: TextIO | None = None
        self._log_path: Path | None = None
        self._logger = logging.getLogger("delta")
        self._logger.setLevel(logging.DEBUG)
        self._logger.handlers.clear()
        self._command: str = ""
        self._should_log_to_file: bool = False
        # Always add a basic console handler by default
        self._handler = DualHandler()
        self._logger.addHandler(self._handler)

    def start(self, command: str, log_to_file: bool = True) -> None:
        """Start a logging session for a specific command."""
        self._command = command
        self._should_log_to_file = log_to_file

        if log_to_file:
            self.logs_dir.mkdir(parents=True, exist_ok=True)
            # Temporary filename; result will be appended on finish
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._log_path = self.logs_dir / f"{timestamp}_{command}_running.log"
            self._log_file = open(self._log_path, "w", encoding="utf-8")

        # Replace handler
        self._logger.handlers.clear()
        self._handler = DualHandler(log_file=self._log_file)
        self._logger.addHandler(self._handler)

    def finish(self, success: bool = True) -> None:
        """Finalize the log file, renaming with the result."""
        if self._log_file:
            self._log_file.close()
            self._log_file = None

        if self._log_path and self._log_path.exists():
            result = "success" if success else "failure"
            final_name = self._log_path.name.replace("_running.log", f"_{result}.log")
            final_path = self._log_path.parent / final_name
            self._log_path.rename(final_path)
            self._log_path = final_path

        # Reset handler to console-only
        self._logger.handlers.clear()
        self._handler = DualHandler()
        self._logger.addHandler(self._handler)

    def finish_interrupted(self) -> None:
        """Finalize log file as interrupted (Ctrl+C)."""
        if self._log_file:
            self._log_file.close()
            self._log_file = None

        if self._log_path and self._log_path.exists():
            final_name = self._log_path.name.replace("_running.log", "_interrupted.log")
            final_path = self._log_path.parent / final_name
            self._log_path.rename(final_path)
            self._log_path = final_path

        self._logger.handlers.clear()
        self._handler = DualHandler()
        self._logger.addHandler(self._handler)

    def finish_cancelled(self) -> None:
        """Finalize log file as cancelled (user said no)."""
        if self._log_file:
            self._log_file.close()
            self._log_file = None

        if self._log_path and self._log_path.exists():
            final_name = self._log_path.name.replace("_running.log", "_cancelled.log")
            final_path = self._log_path.parent / final_name
            self._log_path.rename(final_path)
            self._log_path = final_path

        self._logger.handlers.clear()
        self._handler = DualHandler()
        self._logger.addHandler(self._handler)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def check_log_limits(self) -> None:
        """Check log count and size, warn if over limits."""
        if not self.logs_dir.exists():
            return
        logs = list(self.logs_dir.glob("*.log"))
        total_size = sum(f.stat().st_size for f in logs)
        total_mb = total_size / (1024 * 1024)

        warnings = []
        if len(logs) > self.max_count:
            warnings.append(f"Log file count ({len(logs)}) exceeds limit ({self.max_count}).")
        if total_mb > self.max_size_mb:
            warnings.append(
                f"Log total size ({total_mb:.1f} MB) exceeds limit ({self.max_size_mb} MB)."
            )
        if warnings:
            click.echo()
            for w in warnings:
                ui.print_warning(w)
            ui.print_warning("Run 'delta log clean' to remove old logs.")

    def list_logs(self) -> list[Path]:
        """Return all log files sorted newest first."""
        if not self.logs_dir.exists():
            return []
        return sorted(self.logs_dir.glob("*.log"), reverse=True)

    def clean_logs(self, keep: int = 0) -> int:
        """Remove log files, optionally keeping the N most recent. Returns count removed."""
        logs = self.list_logs()
        to_remove = logs[keep:] if keep > 0 else logs
        for f in to_remove:
            f.unlink()
        return len(to_remove)
