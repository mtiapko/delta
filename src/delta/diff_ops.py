"""Diff operations: compare cached scan with reference, display changes.

All operations are LOCAL — no SSH. Device data comes from cache
populated by 'delta fetch'.
"""

from __future__ import annotations

import difflib
import logging
from pathlib import Path

import click

from delta.connection import compute_local_md5
from delta.models import (
    DeltaConfig,
    DiffResult,
    EntityType,
    FileInfo,
    ResolvedConfig,
    ScanResult,
    TrackedValue,
    matches_any_pattern,
)
from delta.storage import Storage

logger = logging.getLogger("delta")


# ======================================================================
# Ignore patterns
# ======================================================================

def collect_ignore_patterns(
    storage: Storage,
    ref_name: str,
    ref_type: EntityType,
    extra_ignore: list[str] | None = None,
) -> list[str]:
    """Collect ignore patterns from entity metadata + CLI.

    Entity is self-contained — no global config reading at runtime.
    """
    patterns: list[str] = []

    if ref_type == EntityType.BASELINE:
        patterns.extend(storage.load_baseline(ref_name).ignore_patterns)
    else:
        pm = storage.load_patch(ref_name)
        patterns.extend(pm.ignore_patterns)

    if extra_ignore:
        patterns.extend(extra_ignore)
    return patterns


# ======================================================================
# Resolve config for --show-config
# ======================================================================

def resolve_config(
    storage: Storage,
    operation: str,
    ref_name: str,
    ref_type: EntityType,
    *,
    config: DeltaConfig | None = None,
    template_name: str = "",
    extra_ignore: list[str] | None = None,
) -> ResolvedConfig:
    """Build full resolved config with source annotations."""
    from delta.models import TrackedValue, ResolvedConfig

    rc = ResolvedConfig(operation=operation, reference=ref_name, reference_type=ref_type.value)

    if config:
        rc.host = config.ssh.host
        rc.transfer_method = config.transfer.method.value
        rc.transfer_compress = config.transfer.compress

    # Tracked paths
    if ref_type == EntityType.BASELINE:
        bl = storage.load_baseline(ref_name)
        rc.tracked_paths = bl.tracked_paths
    else:
        pm = storage.load_patch(ref_name)
        try:
            bl = storage.load_baseline(pm.baseline)
            rc.tracked_paths = bl.tracked_paths
        except Exception:
            bl = None

    # Ignore patterns (from entity metadata only — self-contained)
    if ref_type == EntityType.BASELINE:
        bl_meta = storage.load_baseline(ref_name)
        for p in bl_meta.ignore_patterns:
            rc.ignore_patterns.append(TrackedValue(p, f"baseline/{ref_name}"))
        # Description
        if bl_meta.description:
            rc.description = TrackedValue(bl_meta.description, f"baseline/{ref_name}")
        # Variables
        for v in bl_meta.variables:
            rc.variables.append(TrackedValue(f"{v.name} (required={v.required})", f"baseline/{ref_name}"))
        # Commands
        for c in bl_meta.on_fetch.pre:
            rc.on_fetch_pre.append(TrackedValue(c.cmd, f"baseline/{ref_name}"))
        for c in bl_meta.on_fetch.post:
            rc.on_fetch_post.append(TrackedValue(c.cmd, f"baseline/{ref_name}"))
    else:
        pm = storage.load_patch(ref_name)
        # Baseline layer
        try:
            bl_meta = storage.load_baseline(pm.baseline)
            for p in bl_meta.ignore_patterns:
                rc.ignore_patterns.append(TrackedValue(p, f"baseline/{pm.baseline}"))
            for c in bl_meta.on_fetch.pre:
                rc.on_fetch_pre.append(TrackedValue(c.cmd, f"baseline/{pm.baseline}"))
            for c in bl_meta.on_fetch.post:
                rc.on_fetch_post.append(TrackedValue(c.cmd, f"baseline/{pm.baseline}"))
        except Exception:
            pass
        # Patch layer
        for p in pm.ignore_patterns:
            rc.ignore_patterns.append(TrackedValue(p, f"patch/{ref_name}"))
        if pm.description:
            rc.description = TrackedValue(pm.description, f"patch/{ref_name}")
        for v in pm.variables:
            rc.variables.append(TrackedValue(f"{v.name} (required={v.required})", f"patch/{ref_name}"))
        for c in pm.on_fetch.pre:
            rc.on_fetch_pre.append(TrackedValue(c.cmd, f"patch/{ref_name}"))
        for c in pm.on_fetch.post:
            rc.on_fetch_post.append(TrackedValue(c.cmd, f"patch/{ref_name}"))
        for c in pm.on_apply.pre:
            rc.on_apply_pre.append(TrackedValue(c.cmd, f"patch/{ref_name}"))
        for c in pm.on_apply.post:
            rc.on_apply_post.append(TrackedValue(c.cmd, f"patch/{ref_name}"))

    # Template layer (if specified)
    if template_name:
        try:
            tmpl = storage.load_template(template_name)
            for p in tmpl.ignore_patterns:
                rc.ignore_patterns.append(TrackedValue(p, f"template/{template_name}"))
            if tmpl.description and not rc.description:
                rc.description = TrackedValue(tmpl.description, f"template/{template_name}")
            for v in tmpl.variables:
                rc.variables.append(TrackedValue(f"{v.name}", f"template/{template_name}"))
            for c in tmpl.on_fetch.pre:
                rc.on_fetch_pre.append(TrackedValue(c.cmd, f"template/{template_name}"))
            for c in tmpl.on_apply.pre:
                rc.on_apply_pre.append(TrackedValue(c.cmd, f"template/{template_name}"))
            for c in tmpl.on_apply.post:
                rc.on_apply_post.append(TrackedValue(c.cmd, f"template/{template_name}"))
        except Exception:
            pass

    # CLI extra ignore
    if extra_ignore:
        for p in extra_ignore:
            rc.ignore_patterns.append(TrackedValue(p, "--ignore CLI"))

    return rc


# ======================================================================
# Compare scan with reference (LOCAL only)
# ======================================================================

def compare_with_reference(
    storage: Storage,
    scan: ScanResult,
    ref_name: str,
    ref_type: EntityType,
    extra_ignore: list[str] | None = None,
) -> DiffResult:
    """Compare a cached scan result against a baseline or patch."""
    ignore_patterns = collect_ignore_patterns(storage, ref_name, ref_type, extra_ignore)

    device_map = {f.path: f for f in scan.files}
    if ignore_patterns:
        device_map = {p: f for p, f in device_map.items()
                      if not matches_any_pattern(p, ignore_patterns)}

    ref_checksums = _build_reference_checksums(storage, ref_name, ref_type)
    if ignore_patterns:
        ref_checksums = {p: v for p, v in ref_checksums.items()
                         if not matches_any_pattern(p, ignore_patterns)}

    modified: list[FileInfo] = []
    created: list[FileInfo] = []
    deleted: list[str] = []

    for rpath, finfo in device_map.items():
        if rpath in ref_checksums:
            if finfo.is_symlink:
                if finfo.symlink_target != ref_checksums[rpath]:
                    modified.append(finfo)
            elif finfo.md5 != ref_checksums[rpath]:
                modified.append(finfo)
        else:
            created.append(finfo)

    for rpath in ref_checksums:
        if rpath not in device_map:
            deleted.append(rpath)

    return DiffResult(
        reference_name=ref_name, reference_type=ref_type,
        modified=modified, created=created, deleted=deleted,
    )


def compare_entities(
    storage: Storage,
    name_a: str, type_a: EntityType,
    name_b: str, type_b: EntityType,
) -> DiffResult:
    """Compare two local entities (no SSH)."""
    ca = _build_reference_checksums(storage, name_a, type_a)
    cb = _build_reference_checksums(storage, name_b, type_b)
    modified, created, deleted = [], [], []
    for rpath, md5 in cb.items():
        if rpath in ca:
            if md5 != ca[rpath]:
                modified.append(FileInfo(path=rpath, md5=md5))
        else:
            created.append(FileInfo(path=rpath, md5=md5))
    for rpath in ca:
        if rpath not in cb:
            deleted.append(rpath)
    return DiffResult(reference_name=name_a, reference_type=type_a,
                      modified=modified, created=created, deleted=deleted)


# ======================================================================
# Display — summary (for delta status)
# ======================================================================

def print_diff_summary(diff_result: DiffResult, *, filter_paths: list[str] | None = None) -> None:
    """Print compact one-line-per-file summary."""
    from delta import ui
    modified = sorted(diff_result.modified, key=lambda x: x.path)
    created = sorted(diff_result.created, key=lambda x: x.path)
    deleted = sorted(diff_result.deleted)
    if filter_paths:
        modified = [f for f in modified if _path_matches(f.path, filter_paths)]
        created = [f for f in created if _path_matches(f.path, filter_paths)]
        deleted = [p for p in deleted if _path_matches(p, filter_paths)]
    if not modified and not created and not deleted:
        ui.print_success("No matching changes.")
        return
    ui.print_header(f"Changes vs {diff_result.reference_name}")
    for f in modified:
        ui.print_file_change("M", f.path)
    for f in created:
        ui.print_file_change("A", f.path)
    for p in deleted:
        ui.print_file_change("D", p)
    _print_totals(DiffResult(
        reference_name=diff_result.reference_name, reference_type=diff_result.reference_type,
        modified=modified, created=created, deleted=deleted))


# ======================================================================
# Display — detailed diff (for delta diff)
# ======================================================================

def print_diff(
    storage: Storage,
    diff_result: DiffResult,
    ref_name: str,
    ref_type: EntityType,
    *,
    filter_paths: list[str] | None = None,
    show_new_deleted: bool = False,
    show_binary: bool = False,
) -> None:
    """Print detailed diff."""
    from delta import ui
    cache_dir = storage.cache_files_dir
    old_label = f"{ref_type.value}/{ref_name}"

    modified = sorted(diff_result.modified, key=lambda x: x.path)
    created = sorted(diff_result.created, key=lambda x: x.path)
    deleted = sorted(diff_result.deleted)

    if filter_paths:
        modified = [f for f in modified if _path_matches(f.path, filter_paths)]
        created = [f for f in created if _path_matches(f.path, filter_paths)]
        deleted = [p for p in deleted if _path_matches(p, filter_paths)]

    if not modified and not created and not deleted:
        ui.print_info("No matching changes.")
        return

    has_cached = False
    for finfo in modified:
        cached = cache_dir / finfo.path.lstrip("/")
        if cached.exists():
            has_cached = True
            old_file = _resolve_entity_file(storage, ref_name, ref_type, finfo.path)
            _print_unified_diff(finfo.path, old_file, cached, old_label, "device",
                                show_binary=show_binary)
        else:
            ui.print_file_change("M", f"{finfo.path}  (not fetched)")

    if modified and not has_cached:
        ui.print_warning("Files not fetched. Run 'delta fetch' to download changes.")

    # Created — one-liner or content
    if created:
        click.echo(click.style(f"\n{'─' * 60}", fg="cyan"))
        if show_new_deleted:
            for finfo in created:
                local = cache_dir / finfo.path.lstrip("/")
                _print_new_file(finfo.path, local)
        else:
            click.echo(click.style("  Added files:", fg="green", bold=True))
            for finfo in created:
                ui.print_file_change("A", finfo.path)

    # Deleted — one-liner or content
    if deleted:
        click.echo(click.style(f"\n{'─' * 60}", fg="cyan"))
        if show_new_deleted:
            for rpath in deleted:
                old_file = _resolve_entity_file(storage, ref_name, ref_type, rpath)
                _print_deleted_file(rpath, old_file)
        else:
            click.echo(click.style("  Deleted files:", fg="red", bold=True))
            for rpath in deleted:
                ui.print_file_change("D", rpath)

    _print_totals(DiffResult(
        reference_name=ref_name, reference_type=ref_type,
        modified=modified, created=created, deleted=deleted,
    ))


def print_detailed_local_diff(
    storage: Storage, diff_result: DiffResult,
    name_a: str, type_a: EntityType,
    name_b: str, type_b: EntityType,
    *, filter_paths: list[str] | None = None,
    show_binary: bool = False,
) -> None:
    modified = sorted(diff_result.modified, key=lambda x: x.path)
    if filter_paths:
        modified = [f for f in modified if _path_matches(f.path, filter_paths)]
    for finfo in modified:
        old = _resolve_entity_file(storage, name_a, type_a, finfo.path)
        new = _resolve_entity_file(storage, name_b, type_b, finfo.path)
        _print_unified_diff(finfo.path, old, new,
                            f"{type_a.value}/{name_a}", f"{type_b.value}/{name_b}",
                            show_binary=show_binary)


# ======================================================================
# Internal helpers
# ======================================================================

def _build_reference_checksums(
    storage: Storage, ref_name: str, ref_type: EntityType,
) -> dict[str, str]:
    """Build path→value map. Value is md5 for files, symlink_target for symlinks."""
    checksums: dict[str, str] = {}
    if ref_type == EntityType.BASELINE:
        files_dir = storage.baseline_files_dir(ref_name)
        meta = storage.load_baseline(ref_name)
        checksums.update(meta.symlink_targets)
        # Use cached checksums (baseline can have thousands of files)
        checksums.update(_load_or_compute_checksums(storage, ref_name, files_dir))
    else:
        pm = storage.load_patch(ref_name)
        try:
            base_dir = storage.baseline_files_dir(pm.baseline)
            base_checksums = _load_or_compute_checksums(storage, pm.baseline, base_dir)
            checksums.update(base_checksums)
            base_meta = storage.load_baseline(pm.baseline)
            checksums.update(base_meta.symlink_targets)
        except Exception:
            pass
        files_dir = storage.patch_files_dir(ref_name)
        for rpath in pm.deleted_files:
            checksums.pop(rpath, None)
        checksums.update(pm.symlink_targets)
        # Patch files — usually few, compute directly
        for f in files_dir.rglob("*"):
            if f.is_file():
                checksums["/" + str(f.relative_to(files_dir))] = compute_local_md5(f)
    return checksums


def _load_or_compute_checksums(storage: Storage, name: str, files_dir: Path) -> dict[str, str]:
    """Load cached checksums or compute and cache them."""
    import json
    cache_path = storage._baseline_dir(name) / ".checksums.json"

    # Check if cache is valid (exists and newer than any file)
    if cache_path.exists():
        cache_mtime = cache_path.stat().st_mtime
        # Quick check: if any file is newer, invalidate
        stale = False
        for f in files_dir.rglob("*"):
            if f.is_file() and f.stat().st_mtime > cache_mtime:
                stale = True
                break
        if not stale:
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass

    # Compute and cache
    checksums: dict[str, str] = {}
    for f in files_dir.rglob("*"):
        if f.is_file():
            checksums["/" + str(f.relative_to(files_dir))] = compute_local_md5(f)

    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(checksums, f, separators=(",", ":"))
    except Exception:
        pass

    return checksums


def _resolve_entity_file(
    storage: Storage, name: str, entity_type: EntityType, remote_path: str,
) -> Path | None:
    if entity_type == EntityType.BASELINE:
        f = storage.get_baseline_file(name, remote_path)
        return f if f.exists() else None
    f = storage.get_patch_file(name, remote_path)
    if f.exists():
        return f
    try:
        pm = storage.load_patch(name)
        f = storage.get_baseline_file(pm.baseline, remote_path)
        return f if f.exists() else None
    except Exception:
        return None


def _path_matches(file_path: str, filter_paths: list[str]) -> bool:
    """Match paths: exact, directory prefix, or glob pattern."""
    from delta.staging_ops import _matches_paths
    return _matches_paths(file_path, filter_paths)


def _print_totals(diff_result: DiffResult) -> None:
    from delta import ui
    parts = []
    if diff_result.modified:
        parts.append(f"{len(diff_result.modified)} modified")
    if diff_result.created:
        parts.append(f"{len(diff_result.created)} added")
    if diff_result.deleted:
        parts.append(f"{len(diff_result.deleted)} deleted")
    if parts:
        ui.print_info(f"\nTotal: {', '.join(parts)}")


def _is_binary(path: Path) -> bool:
    """Check if file is likely binary by reading first 8KB."""
    try:
        chunk = path.read_bytes()[:8192]
        return b"\x00" in chunk
    except Exception:
        return True


def _print_unified_diff(
    remote_path: str, old_file: Path | None, new_file: Path | None,
    old_label: str, new_label: str, *, show_binary: bool = False,
) -> None:
    click.echo(click.style(f"\n{'─' * 60}", fg="cyan"))
    click.echo(click.style(f"  {remote_path}", fg="yellow", bold=True))
    if not old_file:
        click.echo(click.style("  (reference file not available)", dim=True))
        return
    if not new_file or not new_file.exists():
        click.echo(click.style("  (device file not fetched)", dim=True))
        return

    old_is_bin = _is_binary(old_file)
    new_is_bin = _is_binary(new_file)
    if (old_is_bin or new_is_bin) and not show_binary:
        click.echo(click.style("  Binary files differ", dim=True))
        old_size = old_file.stat().st_size if old_file.exists() else 0
        new_size = new_file.stat().st_size
        if old_size != new_size:
            click.echo(click.style(f"  {old_size} → {new_size} bytes", dim=True))
        return

    try:
        old_lines = old_file.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        new_lines = new_file.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    except Exception:
        click.echo(click.style("  (unreadable)", dim=True))
        return
    diff_lines = list(difflib.unified_diff(
        old_lines, new_lines,
        fromfile=f"{old_label}:{remote_path}", tofile=f"{new_label}:{remote_path}", lineterm=""))
    if not diff_lines:
        click.echo(click.style("  (content identical)", dim=True))
        return
    for line in diff_lines:
        line = line.rstrip("\n")
        if line.startswith("+++") or line.startswith("---"):
            click.echo(click.style(line, bold=True))
        elif line.startswith("@@"):
            click.echo(click.style(line, fg="cyan"))
        elif line.startswith("+"):
            click.echo(click.style(line, fg="green"))
        elif line.startswith("-"):
            click.echo(click.style(line, fg="red"))
        else:
            click.echo(line)


def _print_new_file(remote_path: str, local_file: Path | None) -> None:
    click.echo(click.style(f"\n{'─' * 60}", fg="cyan"))
    click.echo(click.style(f"  New: {remote_path}", fg="green", bold=True))
    if local_file and local_file.exists():
        try:
            for line in local_file.read_text(encoding="utf-8", errors="replace").splitlines():
                click.echo(click.style(f"  + {line}", fg="green"))
        except Exception:
            click.echo(click.style("  (binary file)", dim=True))
    else:
        click.echo(click.style("  (not fetched)", dim=True))


def _print_deleted_file(remote_path: str, old_file: Path | None) -> None:
    click.echo(click.style(f"\n{'─' * 60}", fg="cyan"))
    click.echo(click.style(f"  Deleted: {remote_path}", fg="red", bold=True))
    if old_file and old_file.exists():
        try:
            for line in old_file.read_text(encoding="utf-8", errors="replace").splitlines():
                click.echo(click.style(f"  - {line}", fg="red"))
        except Exception:
            click.echo(click.style("  (binary file)", dim=True))
