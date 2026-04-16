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
        # Inherit settings from baseline (self-contained)
        bl = storage.load_baseline(from_entity)
        config = storage.load_config()
        meta.ignore_patterns = list(bl.ignore_patterns)
        meta.variables = list(bl.variables)
        # Merge: config on_fetch + baseline on_fetch
        meta.on_fetch = CommandBlock(
            pre=list(config.on_fetch.pre) + list(bl.on_fetch.pre),
            post=list(bl.on_fetch.post) + list(config.on_fetch.post),
        )
        # Config on_apply (baselines don't have on_apply)
        meta.on_apply = CommandBlock(
            pre=list(config.on_apply.pre),
            post=list(config.on_apply.post),
        )
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
    """Add files to staging. paths=None means all."""
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
    files_to_copy: list[str] = []

    for finfo in add_mod:
        if finfo.path not in manifest.modified:
            manifest.modified.append(finfo.path)
            if not finfo.is_symlink:
                files_to_copy.append(finfo.path)
            added += 1

    for finfo in add_new:
        if finfo.path not in manifest.created:
            manifest.created.append(finfo.path)
            if not finfo.is_symlink:
                files_to_copy.append(finfo.path)
            added += 1

    for rpath in add_del:
        if rpath not in manifest.deleted:
            manifest.deleted.append(rpath)
            added += 1

    # Copy fetched files from cache to staging
    cache_dir = storage.cache_files_dir
    staging_dir = storage.staging_files_dir
    for rpath in files_to_copy:
        src = cache_dir / rpath.lstrip("/")
        if src.exists():
            dst = staging_dir / rpath.lstrip("/")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))

    storage.save_staging(manifest)
    if added:
        ui.print_success(f"{added} items staged.")
    else:
        ui.print_info("Nothing new to stage.")
    return manifest


def stage_remove(storage: Storage, paths: list[str]) -> StagingManifest:
    from delta import ui
    manifest = storage.load_staging()
    removed = 0
    for path in paths:
        if manifest.remove_file(path):
            removed += 1
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
    staging_files = storage.staging_files_dir

    # Merge file lists
    new_modified = 0
    new_created = 0
    new_deleted = 0

    for rpath in manifest.modified:
        # Copy file to patch dir
        src = staging_files / rpath.lstrip("/")
        if src.exists():
            dst = patch_files_dir / rpath.lstrip("/")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
        # Add to modified list if not already there
        if rpath not in meta.modified_files:
            # Maybe it was in created_files before
            if rpath in meta.created_files:
                pass  # Already tracked as created, just update the file
            else:
                meta.modified_files.append(rpath)
                new_modified += 1

    for rpath in manifest.created:
        src = staging_files / rpath.lstrip("/")
        if src.exists():
            dst = patch_files_dir / rpath.lstrip("/")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
        if rpath not in meta.created_files and rpath not in meta.modified_files:
            meta.created_files.append(rpath)
            new_created += 1

    for rpath in manifest.deleted:
        if rpath not in meta.deleted_files:
            meta.deleted_files.append(rpath)
            new_deleted += 1
        # Remove from modified/created if was there
        if rpath in meta.modified_files:
            meta.modified_files.remove(rpath)
        if rpath in meta.created_files:
            meta.created_files.remove(rpath)
        # Remove file from patch dir if exists
        f = patch_files_dir / rpath.lstrip("/")
        if f.exists():
            f.unlink()

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

    total_new = new_modified + new_created + new_deleted
    total_all = len(meta.modified_files) + len(meta.created_files) + len(meta.deleted_files)
    ui.print_success(
        f"Committed to '{patch_name}': "
        f"+{total_new} new changes, {total_all} total in patch."
    )
    return meta


def _matches_paths(file_path: str, filter_paths: list[str]) -> bool:
    for p in filter_paths:
        if file_path == p or file_path.startswith(p.rstrip("/") + "/"):
            return True
    return False


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
        added += 1
        ui.print_file_change("M", f"{rpath} (force)")

    storage.save_staging(manifest)
    if added:
        ui.print_success(f"{added} files force-staged from reference.")
