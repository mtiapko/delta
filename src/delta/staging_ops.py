"""Staging operations: add, remove, status, commit, create.

All operations are LOCAL — uses cached files from 'delta fetch'.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path

from delta.models import (
    CommandBlock,
    DiffResult,
    EntityType,
    FileInfo,
    OwnershipData,
    PatchMetadata,
    StagingManifest,
    VariableSpec,
)
from delta.ownership import compute_ownership
from delta.storage import Storage

logger = logging.getLogger("delta")


# ======================================================================
# Create patch (empty or from existing entity)
# ======================================================================

def create_patch(
    storage: Storage,
    name: str,
    *,
    from_entity: str | None = None,
    description: str = "",
) -> PatchMetadata:
    """Create a new patch, optionally copying from an existing entity.

    If from_entity is None, creates from the active baseline/patch.
    If from_entity is a patch, copies its files and metadata.
    If from_entity is a baseline, creates an empty patch based on it.
    """
    from delta import ui

    if from_entity:
        from_type = storage.get_entity_type(from_entity)
    else:
        # Must have an active reference
        state = storage.load_state()
        if not state.active:
            raise ValueError("No active reference. Use --from <baseline> or 'delta use <name>' first.")
        from_entity = state.active
        from_type = storage.get_entity_type(from_entity)

    # Determine parent baseline
    if from_type == EntityType.BASELINE:
        parent_baseline = from_entity
    else:
        parent_patch = storage.load_patch(from_entity)
        parent_baseline = parent_patch.baseline

    now = datetime.now().isoformat()
    meta = PatchMetadata(
        name=name,
        baseline=parent_baseline,
        description=description,
        created_at=now,
        updated_at=now,
    )

    if from_type == EntityType.BASELINE:
        # Inherit everything from baseline (self-contained — no config merge)
        bl = storage.load_baseline(from_entity)
        meta.ignore_patterns = list(bl.ignore_patterns)
        meta.variables = list(bl.variables)
        meta.on_fetch = CommandBlock(pre=list(bl.on_fetch.pre), post=list(bl.on_fetch.post))
        meta.ownership = bl.ownership
        meta.command_outputs = dict(bl.command_outputs)
        if not description and bl.description:
            meta.description = bl.description
    else:
        # Copy everything from source patch
        src_meta = storage.load_patch(from_entity)
        meta.modified_files = list(src_meta.modified_files)
        meta.created_files = list(src_meta.created_files)
        meta.deleted_files = list(src_meta.deleted_files)
        meta.symlink_targets = dict(src_meta.symlink_targets)
        meta.ignore_patterns = list(src_meta.ignore_patterns)
        meta.variables = list(src_meta.variables)
        meta.on_fetch = CommandBlock(pre=list(src_meta.on_fetch.pre), post=list(src_meta.on_fetch.post))
        meta.on_apply = CommandBlock(pre=list(src_meta.on_apply.pre), post=list(src_meta.on_apply.post))
        meta.ownership = src_meta.ownership
        meta.command_outputs = dict(src_meta.command_outputs)
        if not description:
            meta.description = src_meta.description

        # Copy patch files
        src_files = storage.patch_files_dir(from_entity)
        dst_files = storage.patch_files_dir(name)
        if src_files.exists():
            for f in src_files.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(src_files)
                    dst = dst_files / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(f), str(dst))

    storage.save_patch(meta)

    total = len(meta.modified_files) + len(meta.created_files) + len(meta.deleted_files)
    if from_type == EntityType.PATCH:
        ui.print_success(f"Patch '{name}' created from '{from_entity}' ({total} files).")
    else:
        ui.print_success(f"Patch '{name}' created (baseline: {parent_baseline}).")
    return meta


# ======================================================================
# Stage add / remove / status
# ======================================================================

def stage_add(
    storage: Storage,
    diff_result: DiffResult,
    paths: list[str] | None = None,
) -> StagingManifest:
    """Add files to staging. paths=None means all.

    Files are NOT copied — manifest records that they live in cache.
    """
    from delta import ui

    manifest = storage.load_staging()
    manifest.reference = diff_result.reference_name

    if paths is None:
        add_mod = diff_result.modified
        add_new = diff_result.created
        add_del = diff_result.deleted
    else:
        add_mod = [f for f in diff_result.modified if _matches_paths(f.path, paths)]
        add_new = [f for f in diff_result.created if _matches_paths(f.path, paths)]
        add_del = [p for p in diff_result.deleted if _matches_paths(p, paths)]

    added = 0

    for finfo in add_mod:
        if finfo.path not in manifest.modified:
            manifest.modified.append(finfo.path)
            manifest.sources[finfo.path] = "cache"
            added += 1

    for finfo in add_new:
        if finfo.path not in manifest.created:
            manifest.created.append(finfo.path)
            manifest.sources[finfo.path] = "cache"
            added += 1

    for rpath in add_del:
        if rpath not in manifest.deleted:
            manifest.deleted.append(rpath)
            added += 1

    storage.save_staging(manifest)
    if added:
        ui.print_success(f"{added} items staged.")
    else:
        ui.print_info("Nothing new to stage.")
    return manifest


def stage_remove(storage: Storage, paths: list[str]) -> StagingManifest:
    from delta import ui
    manifest = storage.load_staging()

    all_staged = list(manifest.modified) + list(manifest.created) + list(manifest.deleted)
    to_remove = [p for p in all_staged if _matches_paths(p, paths)]

    removed = 0
    for path in to_remove:
        source = manifest.sources.get(path, "cache")
        if manifest.remove_file(path):
            removed += 1
            # Only unlink if file lives in staging (local), never touch cache
            if source == "local":
                staged = storage.get_staging_file(path)
                if staged.exists():
                    staged.unlink()

    storage.save_staging(manifest)
    if removed:
        ui.print_success(f"{removed} items removed from staging.")
    else:
        ui.print_warning("No matching files found in staging.")
    return manifest


def stage_status(storage: Storage) -> StagingManifest:
    from delta import ui
    manifest = storage.load_staging()
    if manifest.is_empty:
        ui.print_info("Staging is empty. Use 'delta patch add' to stage changes.")
        return manifest
    ui.print_header(f"Staging (vs {manifest.reference})")
    for f in sorted(manifest.modified):
        ui.print_file_change("M", f)
    for f in sorted(manifest.created):
        ui.print_file_change("A", f)
    for f in sorted(manifest.deleted):
        ui.print_file_change("D", f)
    parts = []
    if manifest.modified:
        parts.append(f"{len(manifest.modified)} modified")
    if manifest.created:
        parts.append(f"{len(manifest.created)} added")
    if manifest.deleted:
        parts.append(f"{len(manifest.deleted)} deleted")
    ui.print_info(f"\nTotal: {', '.join(parts)}")
    return manifest


# ======================================================================
# Commit (merge staged into active patch)
# ======================================================================

def commit_to_patch(storage: Storage, patch_name: str) -> PatchMetadata:
    """Merge staged changes into existing patch.

    New files are added, existing paths are updated. Nothing is removed
    unless explicitly in manifest.deleted.
    """
    from delta import ui

    manifest = storage.load_staging()
    if manifest.is_empty:
        raise ValueError("Nothing to commit. Use 'delta patch add' first.")

    meta = storage.load_patch(patch_name)
    patch_files_dir = storage.patch_files_dir(patch_name)

    # Merge file lists
    new_files = 0
    updated_files = 0
    missing_files: list[str] = []

    for rpath in manifest.modified:
        src = storage.resolve_staged_file(manifest, rpath)
        if src:
            dst = patch_files_dir / rpath.lstrip("/")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
        else:
            missing_files.append(rpath)
            continue

        if rpath not in meta.modified_files:
            if rpath not in meta.created_files:
                meta.modified_files.append(rpath)
                new_files += 1
            # else: already tracked as created, just file updated
        else:
            updated_files += 1

    for rpath in manifest.created:
        src = storage.resolve_staged_file(manifest, rpath)
        if src:
            dst = patch_files_dir / rpath.lstrip("/")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
        else:
            missing_files.append(rpath)
            continue

        if rpath not in meta.created_files and rpath not in meta.modified_files:
            meta.created_files.append(rpath)
            new_files += 1

    for rpath in manifest.deleted:
        was_created = rpath in meta.created_files
        was_modified = rpath in meta.modified_files

        if rpath in meta.modified_files:
            meta.modified_files.remove(rpath)
        if rpath in meta.created_files:
            meta.created_files.remove(rpath)

        # Only add to deleted_files if file exists in baseline
        # (deleting a file that was only created by this patch = just undo)
        if was_created and not was_modified:
            # File was only added by this patch, not in baseline → just remove
            new_files += 1
        elif rpath not in meta.deleted_files:
            meta.deleted_files.append(rpath)
            new_files += 1

        f = patch_files_dir / rpath.lstrip("/")
        if f.exists():
            f.unlink()

    if missing_files:
        ui.print_warning(
            f"{len(missing_files)} files not found (cache cleared?): "
            + ", ".join(missing_files[:3])
            + ("..." if len(missing_files) > 3 else "")
        )

    # Update symlink targets from scan
    scan = storage.load_scan()
    if scan:
        scan_map = {f.path: f for f in scan.files}
        for rpath in manifest.modified + manifest.created:
            if rpath in scan_map and scan_map[rpath].is_symlink:
                meta.symlink_targets[rpath] = scan_map[rpath].symlink_target

        # Update ownership from new files
        all_paths = set(meta.modified_files + meta.created_files)
        relevant = [scan_map[p] for p in all_paths if p in scan_map and not scan_map[p].is_symlink]
        if relevant:
            meta.ownership = compute_ownership(relevant)

    meta.updated_at = datetime.now().isoformat()
    storage.save_patch(meta)
    storage.clear_staging()

    total_all = len(meta.modified_files) + len(meta.created_files) + len(meta.deleted_files)
    parts = []
    if new_files:
        parts.append(f"{new_files} new")
    if updated_files:
        parts.append(f"{updated_files} updated")
    changes = ", ".join(parts) if parts else "no new"
    ui.print_success(
        f"Committed to '{patch_name}': "
        f"{changes}, {total_all} total in patch."
    )
    return meta


def _matches_paths(file_path: str, filter_paths: list[str]) -> bool:
    """Check if file_path matches any filter. Supports:
    - Exact match: /etc/config.conf
    - Directory prefix: /etc/ or /etc
    - Glob patterns: /etc/*.conf (single * does NOT cross /)
    - Recursive glob: /etc/**/*.json
    """
    import fnmatch
    import re

    for p in filter_paths:
        if file_path == p:
            return True

        if "**" in p:
            if _matches_recursive_glob(file_path, p):
                return True
        elif "*" in p or "?" in p:
            # Convert glob to regex where * doesn't cross /
            # * → [^/]*, ? → [^/]
            regex = re.escape(p).replace(r"\*", "[^/]*").replace(r"\?", "[^/]")
            if re.fullmatch(regex, file_path):
                return True
        elif file_path.startswith(p.rstrip("/") + "/"):
            return True
    return False


def _matches_recursive_glob(path: str, pattern: str) -> bool:
    """Match pattern with ** against path. ** crosses directory boundaries."""
    import re
    # Convert: /etc/**/*.json → /etc/.*/[^/]*\.json
    # First escape, then replace \*\* with .*, then \* with [^/]*
    regex = re.escape(pattern)
    regex = regex.replace(r"\*\*", "DOUBLE_STAR_MARKER")
    regex = regex.replace(r"\*", "[^/]*").replace(r"\?", "[^/]")
    regex = regex.replace("DOUBLE_STAR_MARKER", ".*")
    return bool(re.fullmatch(regex, path))


# ======================================================================
# Local file staging (bypasses fetch/diff)
# ======================================================================

def stage_add_local(
    storage: Storage,
    remote_path: str,
    local_file: Path,
    ref_name: str,
    ref_type: EntityType,
) -> None:
    """Stage a local file for a specific device path."""
    from delta import ui
    from delta.diff_ops import _build_reference_checksums
    from delta.connection import compute_local_md5

    manifest = storage.load_staging()
    manifest.reference = ref_name

    # Determine if this is modify or create
    ref_checksums = _build_reference_checksums(storage, ref_name, ref_type)
    is_modify = remote_path in ref_checksums

    # Copy file to staging
    dst = storage.staging_files_dir / remote_path.lstrip("/")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(local_file), str(dst))

    if is_modify:
        if remote_path not in manifest.modified:
            manifest.modified.append(remote_path)
        change_type = "M"
    else:
        if remote_path not in manifest.created:
            manifest.created.append(remote_path)
        change_type = "A"

    manifest.sources[remote_path] = "local"
    storage.save_staging(manifest)
    ui.print_file_change(change_type, remote_path)
    ui.print_success(f"Staged from local file.")


def stage_add_delete(storage: Storage, paths: list[str]) -> None:
    """Mark files for deletion (no fetch needed)."""
    from delta import ui

    manifest = storage.load_staging()
    if not manifest.reference:
        state = storage.load_state()
        manifest.reference = state.active

    added = 0
    for rpath in paths:
        if rpath not in manifest.deleted:
            manifest.deleted.append(rpath)
            # Remove from modified/created if was there
            if rpath in manifest.modified:
                manifest.modified.remove(rpath)
            if rpath in manifest.created:
                manifest.created.remove(rpath)
            added += 1

    storage.save_staging(manifest)
    for rpath in paths:
        ui.print_file_change("D", rpath)
    if added:
        ui.print_success(f"{added} files marked for deletion.")


def stage_add_force(
    storage: Storage,
    paths: list[str],
    ref_name: str,
    ref_type: EntityType,
) -> None:
    """Stage files from baseline even if unchanged on device."""
    from delta import ui
    from delta.diff_ops import _resolve_entity_file

    manifest = storage.load_staging()
    manifest.reference = ref_name

    added = 0
    for rpath in paths:
        src = _resolve_entity_file(storage, ref_name, ref_type, rpath)
        if not src or not src.exists():
            ui.print_warning(f"Not found in reference: {rpath}")
            continue

        dst = storage.staging_files_dir / rpath.lstrip("/")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dst))

        if rpath not in manifest.modified:
            manifest.modified.append(rpath)
        manifest.sources[rpath] = "local"
        added += 1
        ui.print_file_change("M", f"{rpath} (force)")

    storage.save_staging(manifest)
    if added:
        ui.print_success(f"{added} files force-staged from reference.")
