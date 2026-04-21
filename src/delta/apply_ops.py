"""Apply a patch to a device via SSH."""

from __future__ import annotations

import logging
from pathlib import Path

from delta.connection import Connection
from delta.models import PatchMetadata
from delta.ownership import get_file_ownership
from delta.remote_cmd import execute_commands, validate_variables
from delta.storage import Storage

logger = logging.getLogger("delta")


def apply_patch(
    conn: Connection,
    storage: Storage,
    patch_name: str,
    *,
    resolved_vars: dict[str, str] | None = None,
    skip_pre_cmds: bool = False,
    skip_post_cmds: bool = False,
    skip_upload: bool = False,
    skip_delete: bool = False,
    skip_permissions: bool = False,
) -> None:
    """Apply a patch to the device."""
    from delta import ui

    resolved_vars = resolved_vars or {}
    meta = storage.load_patch(patch_name)

    # Merge command_outputs + built-in vars + --var (--var wins)
    all_vars: dict[str, str] = dict(meta.command_outputs)
    all_vars["PATCH_HASH"] = storage.compute_patch_hash(patch_name)
    all_vars["PATCH_NAME"] = patch_name
    all_vars["PATCH_DESC"] = meta.description
    all_vars.update(resolved_vars)

    # Validate variables
    all_cmds_skipped = skip_pre_cmds and skip_post_cmds
    if meta.variables and not all_cmds_skipped:
        all_vars.update(validate_variables(meta.variables, all_vars))

    # Pre-apply commands
    outputs: dict[str, str] = {}
    if meta.on_apply.pre and not skip_pre_cmds:
        outputs.update(execute_commands(conn, meta.on_apply.pre, all_vars, phase="pre-apply"))
    elif meta.on_apply.pre and skip_pre_cmds:
        ui.print_info("Skipping pre-apply commands.")

    # Upload regular files (non-symlinks)
    if not skip_upload:
        ui.print_phase("UPLOADING FILES")
        files_to_upload: list[tuple[Path, str]] = []
        collector = ui.get_collector()
        for rpath in meta.modified_files + meta.created_files:
            if rpath in meta.symlink_targets:
                continue  # Symlinks handled separately
            local_file = storage.get_patch_file(patch_name, rpath)
            if local_file.exists():
                files_to_upload.append((local_file, rpath))
            else:
                # Try baseline fallback
                try:
                    bl_file = storage.get_baseline_file(meta.baseline, rpath)
                    if bl_file.exists():
                        files_to_upload.append((bl_file, rpath))
                        continue
                except Exception:
                    pass
                if collector:
                    collector.warning("upload", f"File missing locally: {rpath}")
                else:
                    ui.print_warning(f"File missing locally: {rpath}")

        if files_to_upload:
            conn.upload_files(files_to_upload, label="Applying patch")
        ui.print_success(f"{len(files_to_upload)} files uploaded.")

        # Create symlinks
        if meta.symlink_targets:
            symlink_pairs = [(p, t) for p, t in meta.symlink_targets.items()
                             if p in meta.modified_files or p in meta.created_files]
            if symlink_pairs:
                conn.create_symlinks(symlink_pairs)
                ui.print_success(f"{len(symlink_pairs)} symlinks created.")
    else:
        ui.print_info("Skipping file upload.")

    # Delete files
    if meta.deleted_files and not skip_delete:
        ui.print_phase("DELETING FILES")
        conn.delete_remote_files(meta.deleted_files)
        ui.print_success(f"{len(meta.deleted_files)} files deleted.")
    elif meta.deleted_files and skip_delete:
        ui.print_info("Skipping file deletion.")

    # Restore permissions (for regular files only)
    if not skip_permissions:
        ui.print_phase("RESTORING PERMISSIONS")
        regular_files = [rp for rp in meta.modified_files + meta.created_files
                         if rp not in meta.symlink_targets]
        ownership_entries = []
        for rpath in regular_files:
            owner, group, mode = get_file_ownership(rpath, meta.ownership)
            ownership_entries.append((rpath, owner, group, mode))
        if ownership_entries:
            conn.set_ownership_bulk(ownership_entries)
        ui.print_success(f"{len(ownership_entries)} permissions set.")
    else:
        ui.print_info("Skipping permissions.")

    # Post-apply commands (include outputs from pre-apply)
    if meta.on_apply.post and not skip_post_cmds:
        all_vars.update(outputs)  # Pre-apply outputs available in post-apply
        outputs.update(execute_commands(conn, meta.on_apply.post, all_vars, phase="post-apply"))
    elif meta.on_apply.post and skip_post_cmds:
        ui.print_info("Skipping post-apply commands.")

    if outputs:
        meta.command_outputs.update(outputs)
        storage.save_patch(meta)

    ui.print_success(f"Patch '{patch_name}' applied.")


def _print_apply_plan(meta: PatchMetadata, host: str = "", var_map: dict[str, str] | None = None,
                     patch_hash: str = "") -> None:
    from delta import ui
    from delta.remote_cmd import substitute_variables

    var_map = var_map or {}
    all_vars = dict(meta.command_outputs)
    all_vars["PATCH_HASH"] = patch_hash
    all_vars["PATCH_NAME"] = meta.name
    all_vars["PATCH_DESC"] = meta.description
    all_vars.update(var_map)

    ui.print_header(f"Apply: {meta.name} → {host or '(host)'}")
    if meta.description:
        ui.print_info(f"Description: {meta.description}")

    if meta.modified_files:
        ui.print_info(f"\nModified ({len(meta.modified_files)}):")
        for f in sorted(meta.modified_files):
            ui.print_file_change("M", f)
    if meta.created_files:
        ui.print_info(f"\nCreated ({len(meta.created_files)}):")
        for f in sorted(meta.created_files):
            ui.print_file_change("A", f)
    if meta.deleted_files:
        ui.print_info(f"\nDeleted ({len(meta.deleted_files)}):")
        for f in sorted(meta.deleted_files):
            ui.print_file_change("D", f)

    if meta.on_apply.pre:
        ui.print_info(f"\nPre-apply commands ({len(meta.on_apply.pre)}):")
        for c in meta.on_apply.pre:
            resolved = substitute_variables(c.cmd, all_vars)
            opt = " [optional]" if c.optional else ""
            ui.print_info(f"    {resolved}{opt}")
    if meta.on_apply.post:
        ui.print_info(f"\nPost-apply commands ({len(meta.on_apply.post)}):")
        for c in meta.on_apply.post:
            resolved = substitute_variables(c.cmd, all_vars)
            opt = " [optional]" if c.optional else ""
            ui.print_info(f"    {resolved}{opt}")
