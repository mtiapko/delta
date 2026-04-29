"""Baseline operations: create, refresh, manage tracked paths."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime

from delta.connection import Connection
from delta.models import (
    BaselineMetadata,
    CommandBlock,
    VariableSpec,
)
from delta.ownership import compute_ownership
from delta.remote_cmd import execute_commands, resolve_description, validate_variables
from delta.storage import Storage

logger = logging.getLogger("delta")


def create_baseline(
    conn: Connection,
    storage: Storage,
    *,
    name: str,
    tracked_paths: list[str],
    description: str = "",
    ignore_patterns: list[str] | None = None,
    variables: list[VariableSpec] | None = None,
    on_fetch: CommandBlock | None = None,
    resolved_vars: dict[str, str] | None = None,
) -> BaselineMetadata:
    """Create a new baseline by downloading the current device state."""
    ignore_patterns = ignore_patterns or []
    variables = variables or []
    on_fetch = on_fetch or CommandBlock()
    resolved_vars = resolved_vars or {}

    if variables:
        resolved_vars = validate_variables(variables, resolved_vars)

    # Pre-fetch commands
    outputs: dict[str, str] = {}
    if on_fetch.pre:
        outputs.update(
            execute_commands(conn, on_fetch.pre, resolved_vars, phase="pre-fetch")
        )

    # Scan files
    from delta import ui
    ui.print_phase("SCANNING FILES")
    ui.print_info(f"Paths: {', '.join(tracked_paths)}")
    files = conn.list_files(tracked_paths, ignore_patterns)
    regular = [f for f in files if not f.is_symlink]
    total_size = sum(f.size for f in regular)
    ui.print_info(f"Found {len(files)} files ({ui.format_size(total_size)})")

    if not files:
        ui.print_warning("No files found in tracked paths.")

    # Download regular files (symlinks are metadata-only)
    ui.print_phase("DOWNLOADING FILES")
    files_dir = storage.baseline_files_dir(name)
    file_paths = [f.path for f in regular]
    file_sizes_map = {f.path: f.size for f in regular}
    if file_paths:
        conn.download_files(
            file_paths, files_dir,
            label="Capturing baseline", total_size=total_size,
            file_sizes=file_sizes_map,
        )
    ui.print_success(f"{len(file_paths)} files downloaded.")

    # Compute ownership (from regular files only)
    ownership = compute_ownership(regular)

    # Post-fetch commands
    if on_fetch.post:
        outputs.update(
            execute_commands(conn, on_fetch.post, resolved_vars, phase="post-fetch")
        )

    # Resolve description
    created_at = datetime.now().isoformat()
    resolved_description = resolve_description(
        description, resolved_vars, outputs,
    )

    # Extract symlink targets
    symlink_targets = {f.path: f.symlink_target for f in files if f.is_symlink}

    meta = BaselineMetadata(
        name=name,
        description=resolved_description,
        created_at=created_at,
        tracked_paths=tracked_paths,
        ignore_patterns=ignore_patterns,
        variables=variables,
        on_fetch=on_fetch,
        ownership=ownership,
        command_outputs=outputs,
        symlink_targets=symlink_targets,
        file_count=len(files),
        total_size=total_size,
    )
    storage.save_baseline(meta)
    logger.info("Baseline '%s' created: %d files.", name, len(files))
    return meta


def refresh_baseline(
    conn: Connection,
    storage: Storage,
    name: str,
    *,
    resolved_vars: dict[str, str] | None = None,
    force: bool = False,
) -> BaselineMetadata:
    """Refresh baseline files from device.

    Default: incremental — download new/changed, remove untracked.
    With force: full re-download from scratch.
    """
    resolved_vars = resolved_vars or {}
    meta = storage.load_baseline(name)

    # Merge command_outputs + --var (--var wins)
    all_vars: dict[str, str] = dict(meta.command_outputs)
    all_vars.update(resolved_vars)

    if meta.variables:
        all_vars.update(validate_variables(meta.variables, all_vars))

    outputs: dict[str, str] = {}
    if meta.on_fetch.pre:
        outputs.update(
            execute_commands(conn, meta.on_fetch.pre, all_vars, phase="pre-fetch")
        )

    from delta import ui
    ui.print_phase("SCANNING FILES")
    files = conn.list_files(meta.tracked_paths, meta.ignore_patterns)
    regular = [f for f in files if not f.is_symlink]

    files_dir = storage.baseline_files_dir(name)

    if force:
        ui.print_phase("DOWNLOADING ALL FILES")
        if files_dir.exists():
            shutil.rmtree(files_dir)
        files_dir.mkdir(parents=True)
        file_paths = [f.path for f in regular]
        if file_paths:
            conn.download_files(file_paths, files_dir, label="Refreshing baseline (full)")
    else:
        # Incremental: compare with existing files
        files_dir.mkdir(parents=True, exist_ok=True)

        # Find existing baseline file checksums
        from delta.connection import compute_local_md5
        existing: dict[str, str] = {}
        for f in files_dir.rglob("*"):
            if f.is_file():
                rp = "/" + str(f.relative_to(files_dir))
                existing[rp] = compute_local_md5(f)

        remote_paths = {f.path for f in regular}
        remote_md5 = {f.path: f.md5 for f in regular}

        # Download new or changed files
        to_download = []
        for f in regular:
            if f.path not in existing or existing[f.path] != f.md5:
                to_download.append(f.path)

        # Remove files no longer tracked
        to_remove = [rp for rp in existing if rp not in remote_paths]
        if to_remove:
            for rp in to_remove:
                fp = files_dir / rp.lstrip("/")
                if fp.exists():
                    fp.unlink()
            ui.print_info(f"Removed {len(to_remove)} untracked files.")

        if to_download:
            ui.print_phase("DOWNLOADING CHANGES")
            conn.download_files(to_download, files_dir, label="Refreshing baseline")
            ui.print_success(f"{len(to_download)} files updated.")
        else:
            ui.print_success("Baseline already up to date.")

    ownership = compute_ownership(regular)

    if meta.on_fetch.post:
        all_vars.update(outputs)
        outputs.update(
            execute_commands(conn, meta.on_fetch.post, all_vars, phase="post-fetch")
        )

    meta.ownership = ownership
    meta.command_outputs.update(outputs)
    meta.symlink_targets = {f.path: f.symlink_target for f in files if f.is_symlink}
    meta.file_count = len(files)
    meta.total_size = sum(f.size for f in regular)
    meta.created_at = datetime.now().isoformat()
    storage.save_baseline(meta)

    logger.info("Baseline '%s' refreshed: %d files.", name, len(files))
    return meta


def add_ignore_pattern(storage: Storage, baseline_name: str, pattern: str) -> None:
    meta = storage.load_baseline(baseline_name)
    if pattern not in meta.ignore_patterns:
        meta.ignore_patterns.append(pattern)
        storage.save_baseline(meta)
        logger.info("Added ignore pattern '%s' to baseline '%s'.", pattern, baseline_name)
    else:
        logger.info("Pattern '%s' already exists in baseline '%s'.", pattern, baseline_name)


def remove_ignore_pattern(storage: Storage, baseline_name: str, pattern: str) -> None:
    meta = storage.load_baseline(baseline_name)
    if pattern in meta.ignore_patterns:
        meta.ignore_patterns.remove(pattern)
        storage.save_baseline(meta)
        logger.info("Removed ignore pattern '%s' from baseline '%s'.", pattern, baseline_name)


def add_tracked_path(storage: Storage, baseline_name: str, path: str) -> None:
    meta = storage.load_baseline(baseline_name)
    if path not in meta.tracked_paths:
        meta.tracked_paths.append(path)
        storage.save_baseline(meta)
        logger.info("Added tracked path '%s' to baseline '%s'.", path, baseline_name)


def remove_tracked_path(storage: Storage, baseline_name: str, path: str) -> None:
    meta = storage.load_baseline(baseline_name)
    if path in meta.tracked_paths:
        meta.tracked_paths.remove(path)
        storage.save_baseline(meta)
        logger.info("Removed tracked path '%s' from baseline '%s'.", path, baseline_name)
