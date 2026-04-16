"""CLI entry point for Delta — git-like device version control."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import click
import yaml

from delta import __version__, ui
from delta.exceptions import AbortedError, DeltaError, NotFoundError, ValidationError
from delta.log_manager import LogManager
from delta.models import (
    BaselineMetadata, CommandBlock, CommandSpec, DeltaConfig, DeltaState,
    EntityType, PatchMetadata, SSHConfig, Template, VariableSpec, merge_settings,
)
from delta.storage import Storage

logger = logging.getLogger("delta")


# ======================================================================
# Shared context
# ======================================================================

class DeltaContext:
    def __init__(self) -> None:
        self.storage = Storage()
        self.auto_yes: bool = False
        self.var_map: dict[str, str] = {}
        self.log_manager: LogManager | None = None
        self.show_config: bool = False
        self._interrupted: bool = False
        self._cancelled: bool = False

    def require_init(self) -> None:
        """Require .delta to exist."""
        self.storage.require_initialized()

    def ensure_init(self) -> None:
        """Create .delta if not exists (for config commands only)."""
        if not self.storage.is_initialized:
            self.storage.init(DeltaConfig())

    def get_config(self) -> DeltaConfig:
        return self.storage.load_config()

    def get_state(self) -> DeltaState:
        return self.storage.load_state()

    def save_state(self, state: DeltaState) -> None:
        self.storage.save_state(state)

    def setup_logging(self, command: str, log_to_file: bool = True) -> LogManager:
        config = self.get_config()
        actually_log = log_to_file and config.log_enabled
        lm = LogManager(logs_dir=self.storage.logs_dir,
                        filename_pattern=config.log_filename_pattern,
                        max_count=config.log_max_count, max_size_mb=config.log_max_size_mb)
        lm.start(command, log_to_file=actually_log)
        self.log_manager = lm
        ui.start_collector()
        return lm

    def finish_logging(self, success: bool = True) -> None:
        c = ui.get_collector()
        if c and c.has_issues:
            c.print_summary()
        if self.log_manager:
            if self._interrupted:
                self.log_manager.finish_interrupted()
            elif self._cancelled:
                self.log_manager.finish_cancelled()
            else:
                if not success and self.log_manager._log_path:
                    p = self.log_manager._log_path
                    ui.print_info(f"Log: {p.parent / p.name.replace('_running.log', '_failure.log')}")
                self.log_manager.finish(success)
                self.log_manager.check_log_limits()


pass_ctx = click.make_pass_decorator(DeltaContext, ensure=True)


class DeltaGroup(click.Group):
    def invoke(self, ctx: click.Context) -> None:
        try:
            super().invoke(ctx)
        except DeltaError as e:
            ui.print_error(str(e))
            ctx.exit(1)
        except KeyboardInterrupt:
            ui.print_warning("\nInterrupted.")
            dctx = ctx.find_object(DeltaContext)
            if dctx and dctx.log_manager:
                dctx.log_manager.finish_interrupted()
            ctx.exit(130)


@click.group(cls=DeltaGroup)
@click.version_option(__version__, prog_name="delta")
@click.option("--show-config", is_flag=True, help="Show resolved config and exit.")
@click.option("--dir", "-C", "work_dir", default="", help="Working directory (default: current).")
@click.pass_context
def main(ctx: click.Context, show_config: bool, work_dir: str) -> None:
    """Delta — device filesystem version control."""
    dctx = ctx.ensure_object(DeltaContext)
    dctx.show_config = show_config
    if work_dir:
        dctx.storage = Storage(root=Path(work_dir))


# ======================================================================
# delta init (SSH)
# ======================================================================

@main.command()
@click.argument("name")
@click.option("--host", required=True)
@click.option("--port", default=22)
@click.option("--user", default="root")
@click.option("--key-file", default="")
@click.option("--path", "-p", "tracked_paths", multiple=True, required=True)
@click.option("--description", "-d", default="")
@click.option("--ignore", "-i", "ignore_patterns", multiple=True)
@click.option("--pre-cmd", multiple=True)
@click.option("--post-cmd", multiple=True)
@click.option("--config-file", type=click.Path(exists=True))
@click.option("--template", "template_name", default="")
@click.option("--compress/--no-compress", default=None)
@click.option("--skip-pre-cmds", is_flag=True)
@click.option("--skip-post-cmds", is_flag=True)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmations.")
@click.option("--var", "-V", multiple=True, help="Variable: KEY=VALUE.")
@pass_ctx
def init(ctx: DeltaContext, name: str, host: str, port: int, user: str,
         key_file: str, tracked_paths: tuple[str, ...], description: str,
         ignore_patterns: tuple[str, ...], pre_cmd: tuple[str, ...],
         post_cmd: tuple[str, ...], config_file: str | None,
         template_name: str, compress: bool | None,
         skip_pre_cmds: bool, skip_post_cmds: bool,
         yes: bool, var: tuple[str, ...]) -> None:
    """Initialize delta and create the first baseline."""
    _apply_yes(ctx, yes)
    _apply_vars(ctx, var)
    success = False
    try:
        ssh_config = SSHConfig(host=host, port=port, user=user, key_file=key_file)
        delta_config = DeltaConfig(ssh=ssh_config)
        if ctx.storage.is_initialized:
            delta_config = ctx.storage.load_config()
            delta_config.ssh = ssh_config
            ctx.storage.save_config(delta_config)
        else:
            ctx.storage.init(delta_config)
        lm = ctx.setup_logging("init")

        tmpl = _load_template(ctx.storage, template_name or delta_config.default_baseline_template)
        merged = merge_settings(
            template=tmpl, config_file=_load_config_file(config_file),
            cli_pre_cmd=_parse_cmd_args(pre_cmd) or None,
            cli_post_cmd=_parse_cmd_args(post_cmd) or None,
            cli_ignore=list(ignore_patterns) if ignore_patterns else None,
            cli_description=description)

        on_fetch = merged["on_fetch"]
        if skip_pre_cmds:
            on_fetch = CommandBlock(pre=[], post=on_fetch.post)
        if skip_post_cmds:
            on_fetch = CommandBlock(pre=on_fetch.pre, post=[])

        if ctx.storage.name_exists(name):
            ui.confirm_or_abort(f"'{name}' already exists. Overwrite?", auto_yes=ctx.auto_yes)
            ctx.storage.remove_baseline(name)

        from delta.baseline_ops import create_baseline
        from delta.connection import Connection
        if compress is not None:
            delta_config.transfer.compress = compress
        with Connection(ssh_config, delta_config.transfer) as conn:
            meta = create_baseline(
                conn, ctx.storage, name=name, tracked_paths=list(tracked_paths),
                description=merged["description"], ignore_patterns=merged["ignore_patterns"],
                variables=merged["variables"], on_fetch=on_fetch, resolved_vars=ctx.var_map)

        state = ctx.get_state()
        state.active = name
        ctx.save_state(state)
        ui.print_success(f"Baseline '{name}' created and set as active.")
        ui.print_info(f"Files: {meta.file_count}  Size: {ui.format_size(meta.total_size)}")
        success = True
    except AbortedError:
        ctx._cancelled = True
        ui.print_info("Cancelled.")
    except KeyboardInterrupt:
        ui.print_warning("\nInterrupted.")
        ctx._interrupted = True
    except DeltaError as e:
        _handle_error(e)
    except Exception as e:
        ui.print_error(f"Unexpected error: {e}")
        logger.exception("init failed")
    finally:
        ctx.finish_logging(success)
        if not success:
            sys.exit(130 if ctx._interrupted else 1)


# ======================================================================
# delta fetch (SSH)
# ======================================================================

@main.command()
@click.option("--scan", "scan_only", is_flag=True, help="Only scan checksums, don't download.")
@click.option("--detailed", "-d", is_flag=True, help="Show line-by-line diffs after fetch.")
@click.option("--compress/--no-compress", default=None)
@click.option("--skip-pre-cmds", is_flag=True)
@click.option("--skip-post-cmds", is_flag=True)
@click.option("--var", "-V", multiple=True, help="Variable: KEY=VALUE.")
@pass_ctx
def fetch(ctx: DeltaContext, scan_only: bool, detailed: bool, compress: bool | None,
          skip_pre_cmds: bool, skip_post_cmds: bool, var: tuple[str, ...]) -> None:
    """Fetch device state: scan checksums and download changed files.

    \b
    Examples:
      delta fetch            # Scan + download changes
      delta fetch --scan     # Only scan checksums (fast)
      delta fetch -d         # Fetch + show detailed diffs
    """
    ctx.require_init()
    _apply_vars(ctx, var)

    if ctx.show_config:
        _display_resolved_config(ctx, "fetch")
        return
    lm = ctx.setup_logging("fetch")
    success = False
    diff_result = None

    try:
        state = ctx.get_state()
        config = ctx.get_config()
        ref_name = state.active
        if not ref_name:
            ui.print_error("No active reference. Use 'delta use <n>' first.")
            sys.exit(1)
        ref_type = ctx.storage.get_entity_type(ref_name)

        tracked_paths = _get_tracked_paths(ctx.storage, ref_name, ref_type)
        from delta.diff_ops import collect_ignore_patterns
        ignore_patterns = collect_ignore_patterns(ctx.storage, ref_name, ref_type)

        if compress is not None:
            config.transfer.compress = compress

        from delta.connection import Connection
        from delta.diff_ops import compare_with_reference, print_diff_summary
        from delta.models import ScanResult

        with Connection(config.ssh, config.transfer) as conn:
            _run_entity_cmds(ctx, conn, ref_name, ref_type, "pre", skip_pre_cmds)

            ui.print_phase("SCANNING DEVICE")
            all_files = conn.list_files(tracked_paths, ignore_patterns or None)
            total_size = sum(f.size for f in all_files if not f.is_symlink)
            ui.print_info(f"Found {len(all_files)} files ({ui.format_size(total_size)})")

            scan = ScanResult(
                timestamp=datetime.now().isoformat(),
                host=config.ssh.host, reference=ref_name, files=all_files)

            diff_result = compare_with_reference(ctx.storage, scan, ref_name, ref_type)
            print_diff_summary(diff_result)

            if not scan_only and diff_result.has_changes:
                # Incremental: compare with previous scan to skip already-cached files
                prev_scan = ctx.storage.load_scan()
                prev_md5: dict[str, str] = {}
                if prev_scan:
                    prev_md5 = {f.path: f.md5 for f in prev_scan.files if not f.is_symlink}

                changed_files = diff_result.modified + diff_result.created
                cache_dir = ctx.storage.cache_files_dir

                to_download: list[str] = []
                skipped = 0
                for f in changed_files:
                    if f.is_symlink:
                        continue
                    # Skip if file md5 matches previous scan AND file exists in cache
                    cached_file = cache_dir / f.path.lstrip("/")
                    if (f.path in prev_md5
                            and f.md5 == prev_md5[f.path]
                            and cached_file.exists()):
                        skipped += 1
                    else:
                        to_download.append(f.path)

                if to_download:
                    ui.print_phase("DOWNLOADING CHANGES")
                    dl_size = sum(f.size for f in changed_files
                                  if not f.is_symlink and f.path in to_download)
                    dl_sizes = {f.path: f.size for f in changed_files
                                if not f.is_symlink and f.path in to_download}
                    conn.download_files(
                        to_download, cache_dir,
                        label="Fetching", total_size=dl_size, file_sizes=dl_sizes)
                    msg = f"{len(to_download)} files downloaded."
                    if skipped:
                        msg += f" {skipped} already cached."
                    ui.print_success(msg)
                elif skipped:
                    ui.print_success(f"All {skipped} changed files already in cache.")
            elif scan_only:
                ui.print_info("(scan only — files not downloaded)")

            ctx.storage.save_scan(scan)
            _run_entity_cmds(ctx, conn, ref_name, ref_type, "post", skip_post_cmds)

        if detailed and diff_result and diff_result.has_changes and not scan_only:
            from delta.diff_ops import print_diff
            print_diff(ctx.storage, diff_result, ref_name, ref_type)

        success = True
    except AbortedError:
        ctx._cancelled = True
        ui.print_info("Cancelled.")
    except KeyboardInterrupt:
        ui.print_warning("\nInterrupted.")
        ctx._interrupted = True
    except DeltaError as e:
        _handle_error(e)
    except Exception as e:
        ui.print_error(f"Unexpected error: {e}")
        logger.exception("fetch failed")
    finally:
        ctx.finish_logging(success)
        if not success:
            sys.exit(130 if ctx._interrupted else 1)


# ======================================================================
# delta diff (LOCAL)
# ======================================================================

@main.command()
@click.argument("paths", nargs=-1)
@click.option("--against", "against_name", default="", help="Compare against specific entity.")
@click.option("--full", is_flag=True, help="Also show content of new/deleted files.")
@click.option("--fetch", "do_fetch", is_flag=True, help="Fetch from device first.")
@click.option("--staged", "--cached", "staged", is_flag=True,
              help="Show diffs for staged files (like 'git diff --cached').")
@click.option("--ignore", "-i", "extra_ignore", multiple=True, help="Extra ignore patterns.")
@pass_ctx
def diff(ctx: DeltaContext, paths: tuple[str, ...], against_name: str,
         full: bool, do_fetch: bool, staged: bool, extra_ignore: tuple[str, ...]) -> None:
    """Show detailed changes between device and reference.

    \b
    Without arguments: line-by-line diff for all modified files,
    one-liner for added/deleted. With paths: only matching files.

    \b
    Examples:
      delta diff                       # Device vs reference
      delta diff --staged              # Staging vs reference (like git diff --cached)
      delta diff /etc/wpa.conf         # Only this file
      delta diff /etc/                 # All changes under /etc/
      delta diff --full                # Include new/deleted content
      delta diff --fetch               # Fetch first, then diff
      delta diff --against factory     # Compare with specific entity
    """
    ctx.require_init()
    if ctx.show_config:
        ref = against_name or ctx.get_state().active
        if ref:
            _display_resolved_config(ctx, "diff", extra_ignore=list(extra_ignore) if extra_ignore else None)
        return
    try:
        state = ctx.get_state()
        ref_name = against_name or state.active
        if not ref_name:
            ui.print_error("No active reference. Use 'delta use <n>' or '--against <n>'.")
            sys.exit(1)
        ref_type = ctx.storage.get_entity_type(ref_name)

        if staged:
            # Diff staging vs reference
            manifest = ctx.storage.load_staging()
            if manifest.is_empty:
                ui.print_success("Nothing staged.")
                return
            _print_staged_diff(ctx.storage, manifest, ref_name, ref_type,
                               filter_paths=list(paths) if paths else None,
                               show_new_deleted=full)
            return

        if do_fetch:
            ctx.invoke(fetch)

        scan = _require_scan(ctx)
        _validate_cache(scan, ctx.get_config(), ref_name)

        from delta.diff_ops import compare_with_reference, print_diff
        diff_result = compare_with_reference(
            ctx.storage, scan, ref_name, ref_type, list(extra_ignore) or None)

        if not diff_result.has_changes:
            ui.print_success("No changes detected.")
            return

        print_diff(
            ctx.storage, diff_result, ref_name, ref_type,
            filter_paths=list(paths) if paths else None,
            show_new_deleted=full,
        )

    except DeltaError as e:
        _handle_error(e)
        sys.exit(1)


def _print_staged_diff(storage, manifest, ref_name, ref_type, *,
                       filter_paths=None, show_new_deleted=False):
    """Show diffs for staged files vs reference."""
    from delta.diff_ops import _resolve_entity_file, _print_unified_diff, _path_matches
    import click as _click

    modified = sorted(manifest.modified)
    created = sorted(manifest.created)
    deleted = sorted(manifest.deleted)

    if filter_paths:
        modified = [p for p in modified if _path_matches(p, filter_paths)]
        created = [p for p in created if _path_matches(p, filter_paths)]
        deleted = [p for p in deleted if _path_matches(p, filter_paths)]

    if not modified and not created and not deleted:
        ui.print_info("No matching staged changes.")
        return

    # Modified — line-by-line diff
    for rpath in modified:
        old = _resolve_entity_file(storage, ref_name, ref_type, rpath)
        new = storage.resolve_staged_file(manifest, rpath)
        _print_unified_diff(rpath, old, new,
                            f"{ref_type.value}/{ref_name}", "staged")

    # Created
    if created:
        _click.echo(_click.style(f"\n{'─' * 60}", fg="cyan"))
        if show_new_deleted:
            for rpath in created:
                new = storage.resolve_staged_file(manifest, rpath)
                from delta.diff_ops import _print_new_file
                _print_new_file(rpath, new)
        else:
            _click.echo(_click.style("  Added files:", fg="green", bold=True))
            for rpath in created:
                ui.print_file_change("A", rpath)

    # Deleted
    if deleted:
        _click.echo(_click.style(f"\n{'─' * 60}", fg="cyan"))
        if show_new_deleted:
            for rpath in deleted:
                old = _resolve_entity_file(storage, ref_name, ref_type, rpath)
                from delta.diff_ops import _print_deleted_file
                _print_deleted_file(rpath, old)
        else:
            _click.echo(_click.style("  Deleted files:", fg="red", bold=True))
            for rpath in deleted:
                ui.print_file_change("D", rpath)
        sys.exit(1)


# ======================================================================
# delta status (LOCAL)
# ======================================================================

@main.command()
@pass_ctx
def status(ctx: DeltaContext) -> None:
    """Show current state: reference, changes, staging.

    \b
    Shows changes grouped like 'git status': staged files shown individually,
    unstaged changes with many files in a directory are shown as the directory.
    """
    ctx.require_init()
    state = ctx.get_state()
    config = ctx.get_config()

    ui.print_header("Delta Status")
    ui.print_info(f"SSH: {config.ssh.user}@{config.ssh.host}:{config.ssh.port}")

    if state.active:
        t = ctx.storage.get_entity_type(state.active)
        ui.print_info(f"Active: {state.active} ({t.value})")
    else:
        ui.print_info("Active: (none)")

    ui.print_info(f"Baselines: {len(ctx.storage.list_baselines())}  Patches: {len(ctx.storage.list_patches())}")

    scan = ctx.storage.load_scan()
    manifest = ctx.storage.load_staging()

    # Staged changes — show individually (like git)
    if not manifest.is_empty:
        ui.print_info(f"\nChanges to be committed (staging vs {manifest.reference}):")
        for p in sorted(manifest.modified):
            ui.print_file_change("M", p)
        for p in sorted(manifest.created):
            ui.print_file_change("A", p)
        for p in sorted(manifest.deleted):
            ui.print_file_change("D", p)

    # Unstaged changes — compress directories
    if scan and state.active:
        ui.print_info(f"\nLast fetch: {ui.format_time_ago(scan.timestamp)} from {scan.host} (vs {scan.reference})")

        from delta.diff_ops import compare_with_reference
        ref_type = ctx.storage.get_entity_type(state.active)
        diff_result = compare_with_reference(ctx.storage, scan, state.active, ref_type)

        # Filter out staged
        unstaged_mod = [f.path for f in diff_result.modified if f.path not in manifest.modified]
        unstaged_new = [f.path for f in diff_result.created if f.path not in manifest.created]
        unstaged_del = [p for p in diff_result.deleted if p not in manifest.deleted]

        if unstaged_mod or unstaged_new or unstaged_del:
            ui.print_info("\nChanges not staged for commit:")
            _print_compressed(unstaged_mod, "M")
            _print_compressed(unstaged_new, "A")
            _print_compressed(unstaged_del, "D")
        elif manifest.is_empty:
            ui.print_success("No changes vs reference.")
    elif not scan:
        ui.print_info("\nLast fetch: (none)")


def _print_compressed(paths: list[str], change_type: str, threshold: int = 3) -> None:
    """Print paths, collapsing directories with many files of same type."""
    if not paths:
        return

    # Group by parent directory
    from collections import defaultdict
    by_dir: dict[str, list[str]] = defaultdict(list)
    for p in paths:
        parent = os.path.dirname(p) or "/"
        by_dir[parent].append(p)

    # For each directory, either show files or collapse
    shown: set[str] = set()
    for parent in sorted(by_dir.keys()):
        files = by_dir[parent]
        if len(files) >= threshold:
            ui.print_file_change(change_type, f"{parent}/  ({len(files)} files)")
            shown.update(files)

    # Show remaining files individually
    for p in sorted(paths):
        if p not in shown:
            ui.print_file_change(change_type, p)


# ======================================================================
# delta use (LOCAL)
# ======================================================================

@main.command()
@click.argument("name")
@pass_ctx
def use(ctx: DeltaContext, name: str) -> None:
    """Set the active reference (baseline or patch)."""
    ctx.require_init()
    t = ctx.storage.get_entity_type(name)
    state = ctx.get_state()
    state.active = name
    ctx.save_state(state)
    ui.print_success(f"Active reference: {name} ({t.value})")


# ======================================================================
# delta apply (SSH)
# ======================================================================

@main.command()
@click.argument("name", required=False)
@click.option("--host", default="")
@click.option("--port", type=int, default=None)
@click.option("--user", default="")
@click.option("--key-file", default="")
@click.option("--dry-run", is_flag=True)
@click.option("--compress/--no-compress", default=None)
@click.option("--skip-pre-cmds", is_flag=True)
@click.option("--skip-post-cmds", is_flag=True)
@click.option("--skip-upload", is_flag=True)
@click.option("--skip-delete", is_flag=True)
@click.option("--skip-permissions", is_flag=True)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmations.")
@click.option("--var", "-V", multiple=True, help="Variable: KEY=VALUE.")
@pass_ctx
def apply(ctx: DeltaContext, name: str | None, host: str, port: int | None,
          user: str, key_file: str, dry_run: bool,
          compress: bool | None, skip_pre_cmds: bool, skip_post_cmds: bool,
          skip_upload: bool, skip_delete: bool, skip_permissions: bool,
          yes: bool, var: tuple[str, ...]) -> None:
    """Apply a patch to a device (SSH)."""
    ctx.require_init()
    _apply_yes(ctx, yes)
    _apply_vars(ctx, var)
    if ctx.show_config:
        pn = name or ctx.get_state().active
        if pn:
            _display_resolved_config(ctx, f"apply {pn}")
        return
    lm = ctx.setup_logging("apply", log_to_file=not dry_run)
    success = False
    try:
        config = ctx.get_config()
        patch_name = name or ctx.get_state().active
        if not patch_name:
            ui.print_error("No patch specified or active.")
            sys.exit(1)
        if ctx.storage.get_entity_type(patch_name) != EntityType.PATCH:
            ui.print_error(f"'{patch_name}' is not a patch.")
            sys.exit(1)
        if host:
            config.ssh.host = host
        if port:
            config.ssh.port = port
        if user:
            config.ssh.user = user
        if key_file:
            config.ssh.key_file = key_file
        if compress is not None:
            config.transfer.compress = compress

        meta = ctx.storage.load_patch(patch_name)

        # Validate variables in commands
        from delta.remote_cmd import check_undefined_variables, substitute_variables
        # Built-in vars + --var + variable specs + command_outputs + save_output keys
        defined_vars = (
            set(ctx.var_map.keys())
            | {v.name for v in meta.variables}
            | set(meta.command_outputs.keys())
            | {"PATCH_HASH", "PATCH_NAME"}
        )
        # Commands with save_output define vars for later commands
        all_cmds = meta.on_apply.pre + meta.on_apply.post + meta.on_fetch.pre + meta.on_fetch.post
        for cmd in all_cmds:
            if cmd.save_output and cmd.output_key:
                defined_vars.add(cmd.output_key)
        undefined = check_undefined_variables(all_cmds, defined_vars)
        if undefined:
            ui.print_error(f"Undefined variables in commands: {', '.join(undefined)}")
            sys.exit(1)

        # Show plan
        from delta.apply_ops import _print_apply_plan
        _print_apply_plan(meta, config.ssh.host, ctx.var_map)

        if dry_run:
            ui.print_info("\n(dry run — no changes applied)")
            success = True
            return

        ui.confirm_or_abort("Proceed?", auto_yes=ctx.auto_yes)

        from delta.apply_ops import apply_patch
        from delta.connection import Connection
        with Connection(config.ssh, config.transfer) as conn:
            apply_patch(conn, ctx.storage, patch_name, resolved_vars=ctx.var_map,
                        skip_pre_cmds=skip_pre_cmds, skip_post_cmds=skip_post_cmds,
                        skip_upload=skip_upload, skip_delete=skip_delete,
                        skip_permissions=skip_permissions)
        success = True
    except AbortedError:
        ctx._cancelled = True
        ui.print_info("Cancelled.")
    except KeyboardInterrupt:
        ui.print_warning("\nInterrupted.")
        ctx._interrupted = True
        logger.info("Apply interrupted by user.")
    except DeltaError as e:
        _handle_error(e)
    except Exception as e:
        ui.print_error(f"Unexpected error: {e}")
        logger.exception("apply failed")
    finally:
        ctx.finish_logging(success)
        if not success:
            sys.exit(130 if ctx._interrupted else 1)


# ======================================================================
# delta checkout (SSH)
# ======================================================================

@main.command()
@click.argument("paths", nargs=-1, required=True)
@click.option("--from", "from_name", default="", help="Restore from entity (default: active).")
@click.option("--dry-run", is_flag=True)
@click.option("--compress/--no-compress", default=None)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmations.")
@pass_ctx
def checkout(ctx: DeltaContext, paths: tuple[str, ...], from_name: str,
             dry_run: bool, compress: bool | None, yes: bool) -> None:
    """Restore files on device to reference state (SSH).

    \b
    Examples:
      delta checkout /etc/config.conf          # Restore one file
      delta checkout /etc/network/             # Restore directory
      delta checkout . --from factory          # Restore all from baseline
    """
    ctx.require_init()
    _apply_yes(ctx, yes)
    lm = ctx.setup_logging("checkout", log_to_file=not dry_run)
    success = False
    try:
        config = ctx.get_config()
        ref_name = from_name or ctx.get_state().active
        if not ref_name:
            ui.print_error("No active reference.")
            sys.exit(1)
        ref_type = ctx.storage.get_entity_type(ref_name)

        restore_all = "." in paths
        files: list[tuple[Path, str]] = []

        files_dir = (ctx.storage.baseline_files_dir(ref_name) if ref_type == EntityType.BASELINE
                     else ctx.storage.patch_files_dir(ref_name))
        baseline_dir: Path | None = None
        if ref_type == EntityType.PATCH:
            try:
                baseline_dir = ctx.storage.baseline_files_dir(ctx.storage.load_patch(ref_name).baseline)
            except Exception:
                pass

        from delta.diff_ops import _path_matches
        for f in files_dir.rglob("*"):
            if f.is_file():
                rp = "/" + str(f.relative_to(files_dir))
                if restore_all or _path_matches(rp, list(paths)):
                    files.append((f, rp))
        if baseline_dir and baseline_dir.exists():
            patch_paths = {rp for _, rp in files}
            for f in baseline_dir.rglob("*"):
                if f.is_file():
                    rp = "/" + str(f.relative_to(baseline_dir))
                    if rp not in patch_paths and (restore_all or _path_matches(rp, list(paths))):
                        files.append((f, rp))

        if not files:
            ui.print_warning("No matching files in reference.")
            success = True
            return

        ui.print_header(f"Checkout from {ref_type.value}/{ref_name}")
        total = sum(f.stat().st_size for f, _ in files)
        for f, rp in sorted(files, key=lambda x: x[1]):
            ui.print_info(f"  {rp}  ({ui.format_size(f.stat().st_size)})")
        ui.print_info(f"\n{len(files)} files, {ui.format_size(total)}")

        if dry_run:
            ui.print_info("\n(dry run)")
            success = True
            return

        ui.confirm_or_abort(f"Restore {len(files)} files on {config.ssh.host}?", auto_yes=ctx.auto_yes)
        if compress is not None:
            config.transfer.compress = compress
        from delta.connection import Connection
        from delta.ownership import get_file_ownership
        with Connection(config.ssh, config.transfer) as conn:
            conn.upload_files(files, label="Restoring files")
            meta = (ctx.storage.load_baseline(ref_name) if ref_type == EntityType.BASELINE
                    else ctx.storage.load_patch(ref_name))
            entries = [(rp, *get_file_ownership(rp, meta.ownership)) for _, rp in files]
            if entries:
                conn.set_ownership_bulk(entries)
        ui.print_success(f"{len(files)} files restored.")
        success = True
    except AbortedError:
        ctx._cancelled = True
        ui.print_info("Cancelled.")
    except KeyboardInterrupt:
        ui.print_warning("\nInterrupted.")
        ctx._interrupted = True
    except DeltaError as e:
        _handle_error(e)
    except Exception as e:
        ui.print_error(f"Unexpected error: {e}")
        logger.exception("checkout failed")
    finally:
        ctx.finish_logging(success)
        if not success:
            sys.exit(130 if ctx._interrupted else 1)


# ======================================================================
# delta compare (LOCAL)
# ======================================================================

@main.command()
@click.argument("entity_a")
@click.argument("entity_b")
@click.option("--detailed", "-d", is_flag=True)
@pass_ctx
def compare(ctx: DeltaContext, entity_a: str, entity_b: str, detailed: bool) -> None:
    """Compare two local entities without SSH."""
    ctx.require_init()
    try:
        ta = ctx.storage.get_entity_type(entity_a)
        tb = ctx.storage.get_entity_type(entity_b)
        from delta.diff_ops import compare_entities, print_detailed_local_diff, print_diff_summary
        dr = compare_entities(ctx.storage, entity_a, ta, entity_b, tb)
        print_diff_summary(dr)
        if detailed and dr.modified:
            print_detailed_local_diff(ctx.storage, dr, entity_a, ta, entity_b, tb)
    except DeltaError as e:
        _handle_error(e)
        sys.exit(1)


# ======================================================================
# delta baseline
# ======================================================================

@main.group(invoke_without_command=True)
@click.pass_context
def baseline(ctx: click.Context) -> None:
    """Manage baselines. Run without subcommand to list."""
    if ctx.invoked_subcommand is None:
        dctx = ctx.ensure_object(DeltaContext)
        dctx.require_init()
        entities = [e for e in dctx.storage.list_all_entities() if e["type"] == "baseline"]
        if not entities:
            ui.print_info("No baselines. Use 'delta init'.")
            return
        ui.print_entity_list(entities, active=dctx.get_state().active)

@baseline.command("info")
@click.argument("name", required=False)
@pass_ctx
def baseline_info(ctx: DeltaContext, name: str | None) -> None:
    """Show baseline details."""
    ctx.require_init()
    n = name or ctx.get_state().active
    if not n:
        ui.print_error("No baseline specified.")
        sys.exit(1)
    m = ctx.storage.load_baseline(n)
    ui.print_header(f"Baseline: {m.name}")
    if m.description:
        ui.print_info(f"Description: {m.description}")
    ui.print_info(f"Created: {m.created_at}")
    ui.print_info(f"Files: {m.file_count}  Size: {ui.format_size(m.total_size)}")
    ui.print_info(f"Paths: {', '.join(m.tracked_paths)}")
    if m.ignore_patterns:
        ui.print_info(f"Ignore: {', '.join(m.ignore_patterns)}")

@baseline.command("copy")
@click.argument("src")
@click.argument("dst")
@pass_ctx
def baseline_copy(ctx: DeltaContext, src: str, dst: str) -> None:
    """Copy a baseline."""
    ctx.require_init()
    ctx.storage.copy_baseline(src, dst)
    ui.print_success(f"'{src}' copied to '{dst}'.")

@baseline.command("rm")
@click.argument("name")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@pass_ctx
def baseline_rm(ctx: DeltaContext, name: str, yes: bool) -> None:
    """Remove a baseline."""
    ctx.require_init()
    _apply_yes(ctx, yes)
    ctx.storage.get_entity_type(name)
    ui.confirm_or_abort(f"Remove '{name}'?", auto_yes=ctx.auto_yes)
    ctx.storage.remove_baseline(name)
    state = ctx.get_state()
    if state.active == name:
        state.active = ""
        ctx.save_state(state)
    ui.print_success(f"'{name}' removed.")

@baseline.command("ignore")
@click.argument("pattern")
@click.option("-r", "--remove", is_flag=True)
@pass_ctx
def baseline_ignore(ctx: DeltaContext, pattern: str, remove: bool) -> None:
    """Add/remove ignore pattern on active baseline."""
    ctx.require_init()
    n = ctx.get_state().active
    if not n:
        ui.print_error("No active baseline.")
        sys.exit(1)
    from delta.baseline_ops import add_ignore_pattern, remove_ignore_pattern
    (remove_ignore_pattern if remove else add_ignore_pattern)(ctx.storage, n, pattern)

@baseline.command("track")
@click.argument("path")
@click.option("-r", "--remove", is_flag=True)
@pass_ctx
def baseline_track(ctx: DeltaContext, path: str, remove: bool) -> None:
    """Add/remove tracked path on active baseline."""
    ctx.require_init()
    n = ctx.get_state().active
    if not n:
        ui.print_error("No active baseline.")
        sys.exit(1)
    from delta.baseline_ops import add_tracked_path, remove_tracked_path
    (remove_tracked_path if remove else add_tracked_path)(ctx.storage, n, path)

@baseline.command("refresh")
@click.argument("name", required=False)
@click.option("--compress/--no-compress", default=None)
@pass_ctx
def baseline_refresh(ctx: DeltaContext, name: str | None, compress: bool | None) -> None:
    """Re-download baseline files (SSH)."""
    ctx.require_init()
    lm = ctx.setup_logging("baseline_refresh")
    success = False
    try:
        n = name or ctx.get_state().active
        if not n:
            ui.print_error("No baseline specified.")
            sys.exit(1)
        config = ctx.get_config()
        if compress is not None:
            config.transfer.compress = compress
        from delta.baseline_ops import refresh_baseline
        from delta.connection import Connection
        with Connection(config.ssh, config.transfer) as conn:
            refresh_baseline(conn, ctx.storage, n, resolved_vars=ctx.var_map)
        ui.print_success(f"'{n}' refreshed.")
        success = True
    except (AbortedError, DeltaError) as e:
        (ui.print_info if isinstance(e, AbortedError) else ui.print_error)(str(e))
    finally:
        ctx.finish_logging(success)
        if not success:
            sys.exit(1)


# ======================================================================
# delta patch
# ======================================================================

@main.group(invoke_without_command=True)
@click.pass_context
def patch(ctx: click.Context) -> None:
    """Manage patches. Run without subcommand to list."""
    if ctx.invoked_subcommand is None:
        dctx = ctx.ensure_object(DeltaContext)
        dctx.require_init()
        entities = [e for e in dctx.storage.list_all_entities() if e["type"] == "patch"]
        if not entities:
            ui.print_info("No patches.")
            return
        ui.print_entity_list(entities, active=dctx.get_state().active)

@patch.command("create")
@click.argument("name")
@click.option("--from", "from_entity", default="", help="Create from entity (default: active).")
@click.option("-m", "--message", default="", help="Patch description.")
@pass_ctx
def patch_create(ctx: DeltaContext, name: str, from_entity: str, message: str) -> None:
    """Create a new patch and switch to it.

    \b
    Creates an empty patch from active baseline, or copies from
    another entity with --from.

    \b
    Examples:
      delta patch create wifi                 # From active baseline
      delta patch create wifi-v2 --from wifi  # Copy from existing patch
      delta patch create clean --from factory # From specific baseline
    """
    ctx.require_init()
    if ctx.storage.name_exists(name):
        ui.print_error(f"'{name}' already exists.")
        sys.exit(1)
    try:
        from delta.staging_ops import create_patch
        meta = create_patch(
            ctx.storage, name,
            from_entity=from_entity or None,
            description=message,
        )
        state = ctx.get_state()
        state.active = name
        ctx.save_state(state)
        ui.print_info(f"Active: {name} (patch)")
    except (ValueError, DeltaError) as e:
        ui.print_error(str(e))
        sys.exit(1)

@patch.command("add")
@click.argument("paths", nargs=-1, required=True)
@click.option("--file", "-f", "local_file", default="", help="Local file to use as content.")
@click.option("--delete", is_flag=True, help="Mark file for deletion on device.")
@click.option("--force", is_flag=True, help="Add even if unchanged vs baseline.")
@click.option("--fetch", "do_fetch", is_flag=True, help="Fetch before staging.")
@pass_ctx
def patch_add(ctx: DeltaContext, paths: tuple[str, ...], local_file: str,
              delete: bool, force: bool, do_fetch: bool) -> None:
    """Stage files for next commit.

    \b
    Paths can be:
      - Exact:    /etc/config.conf
      - Directory: /etc/ or /etc (matches all files under)
      - Glob:     /etc/*.conf  /etc/**/*.json  (use quotes in shell)
      - Special:  . (all changes from device)

    \b
    Use --file to add a local file (bypasses fetch/diff).
    Use --delete to mark a file for deletion.
    Use --force to add from baseline even if unchanged.

    \b
    Examples:
      delta patch add .                                  # Stage all device changes
      delta patch add /etc/config.conf                   # Stage one file
      delta patch add "/etc/*.conf"                      # Glob pattern
      delta patch add "/var/**/*.log"                    # Recursive glob
      delta patch add /etc/app.conf --file ./my.conf     # Local file → device path
      delta patch add "/etc/*.old" --delete              # Delete matching on device
      delta patch add /etc/config.conf --force           # Add from baseline
    """
    ctx.require_init()
    state = ctx.get_state()
    ref_name = state.active
    if not ref_name:
        ui.print_error("No active reference.")
        sys.exit(1)
    ref_type = ctx.storage.get_entity_type(ref_name)

    # Mode 1: --file (local file → device path)
    if local_file:
        if len(paths) != 1:
            ui.print_error("--file requires exactly one target path.")
            sys.exit(1)
        remote_path = paths[0]
        src = Path(local_file)
        if not src.exists():
            ui.print_error(f"File not found: {local_file}")
            sys.exit(1)
        from delta.staging_ops import stage_add_local
        stage_add_local(ctx.storage, remote_path, src, ref_name, ref_type)
        return

    # Mode 2: --delete (mark for deletion)
    if delete:
        from delta.staging_ops import stage_add_delete
        stage_add_delete(ctx.storage, list(paths))
        return

    # Mode 3: --force (copy from baseline even if unchanged)
    if force:
        from delta.staging_ops import stage_add_force
        stage_add_force(ctx.storage, list(paths), ref_name, ref_type)
        return

    # Mode 4: normal (from fetch/diff)
    if do_fetch:
        ctx.invoke(fetch)
    scan = _require_scan(ctx)
    _validate_cache(scan, ctx.get_config(), ref_name)
    from delta.diff_ops import compare_with_reference
    from delta.staging_ops import stage_add
    dr = compare_with_reference(ctx.storage, scan, ref_name, ref_type)
    stage_add(ctx.storage, dr, None if "." in paths else list(paths))

@patch.command("edit")
@click.argument("remote_path")
@pass_ctx
def patch_edit(ctx: DeltaContext, remote_path: str) -> None:
    """Edit a file and stage it in the patch.

    \b
    Opens the file from baseline/patch in your editor.
    After saving, the file is staged for commit.

    \b
    Examples:
      delta patch edit /etc/config.conf
    """
    ctx.require_init()
    import shutil
    import tempfile

    state = ctx.get_state()
    ref_name = state.active
    if not ref_name:
        ui.print_error("No active reference.")
        sys.exit(1)
    ref_type = ctx.storage.get_entity_type(ref_name)

    # Find source file
    from delta.diff_ops import _resolve_entity_file
    src = _resolve_entity_file(ctx.storage, ref_name, ref_type, remote_path)

    config = ctx.get_config()
    editor = config.editor or os.environ.get("EDITOR", "") or os.environ.get("VISUAL", "")
    if not editor:
        ui.print_error("No editor. Set 'editor' in config or $EDITOR.")
        sys.exit(1)

    with tempfile.NamedTemporaryFile(suffix="_" + os.path.basename(remote_path),
                                     mode="w", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        if src and src.exists():
            tmp_path.write_bytes(src.read_bytes())
        else:
            ui.print_info(f"Creating new file: {remote_path}")

    os.system(f"{editor} {tmp_path}")

    if not tmp_path.exists() or (src and src.exists() and tmp_path.read_bytes() == src.read_bytes()):
        ui.print_info("No changes.")
        tmp_path.unlink(missing_ok=True)
        return

    from delta.staging_ops import stage_add_local
    stage_add_local(ctx.storage, remote_path, tmp_path, ref_name, ref_type)
    tmp_path.unlink(missing_ok=True)

@patch.command("remove")
@click.argument("paths", nargs=-1, required=True)
@pass_ctx
def patch_remove(ctx: DeltaContext, paths: tuple[str, ...]) -> None:
    """Remove files from staging.

    \b
    Supports same patterns as 'patch add':
      - Exact path, directory, or glob.

    \b
    Examples:
      delta patch remove /etc/config.conf
      delta patch remove /etc/
      delta patch remove "/etc/*.conf"
    """
    ctx.require_init()
    from delta.staging_ops import stage_remove
    stage_remove(ctx.storage, list(paths))

@patch.command("commit")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@pass_ctx
def patch_commit(ctx: DeltaContext, yes: bool) -> None:
    """Commit staged changes into the active patch."""
    ctx.require_init()
    _apply_yes(ctx, yes)
    lm = ctx.setup_logging("patch_commit")
    success = False
    try:
        state = ctx.get_state()
        if not state.active:
            ui.print_error("No active patch. Use 'delta patch create <n>' first.")
            sys.exit(1)
        t = ctx.storage.get_entity_type(state.active)
        if t != EntityType.PATCH:
            ui.print_error(f"Active '{state.active}' is a baseline. Use 'delta patch create <n>'.")
            sys.exit(1)

        from delta.staging_ops import stage_status, commit_to_patch
        manifest = stage_status(ctx.storage)
        if manifest.is_empty:
            ui.print_error("Nothing to commit.")
            sys.exit(1)

        ui.confirm_or_abort(
            f"Commit {manifest.total_files} changes to '{state.active}'?",
            auto_yes=ctx.auto_yes,
        )
        commit_to_patch(ctx.storage, state.active)
        success = True
    except AbortedError:
        ctx._cancelled = True
        ui.print_info("Cancelled.")
    except ValueError as e:
        ui.print_error(str(e))
    except DeltaError as e:
        _handle_error(e)
    finally:
        ctx.finish_logging(success)
        if not success:
            sys.exit(1)

@patch.command("info")
@click.argument("name", required=False)
@click.option("--detailed", "-d", is_flag=True, help="Show diffs vs baseline.")
@click.option("--hash", "show_hash", is_flag=True, help="Print only the hash (for scripts).")
@pass_ctx
def patch_info(ctx: DeltaContext, name: str | None, detailed: bool, show_hash: bool) -> None:
    """Show patch details and file list."""
    ctx.require_init()
    n = name or ctx.get_state().active
    if not n:
        ui.print_error("No patch specified.")
        sys.exit(1)

    if show_hash:
        click.echo(ctx.storage.compute_patch_hash(n))
        return

    m = ctx.storage.load_patch(n)
    patch_hash = ctx.storage.compute_patch_hash(n)
    ui.print_header(f"Patch: {m.name}")
    ui.print_info(f"Hash: {patch_hash}")
    if m.description:
        ui.print_info(f"Description: {m.description}")
    ui.print_info(f"Baseline: {m.baseline}")
    ui.print_info(f"Created: {m.created_at}")
    if m.updated_at and m.updated_at != m.created_at:
        ui.print_info(f"Updated: {m.updated_at}")
    if not m.on_apply.is_empty:
        ui.print_info(f"Apply commands: {len(m.on_apply.pre)} pre, {len(m.on_apply.post)} post")
    if not m.on_fetch.is_empty:
        ui.print_info(f"Fetch commands: {len(m.on_fetch.pre)} pre, {len(m.on_fetch.post)} post")

    # Always show file list
    total = len(m.modified_files) + len(m.created_files) + len(m.deleted_files)
    if total:
        ui.print_info(f"\nFiles ({total}):")
        for f in sorted(m.modified_files):
            ui.print_file_change("M", f)
        for f in sorted(m.created_files):
            ui.print_file_change("A", f)
        for f in sorted(m.deleted_files):
            ui.print_file_change("D", f)
    else:
        ui.print_info("\nNo files (empty patch).")

    if detailed and m.modified_files:
        from delta.diff_ops import _resolve_entity_file, _print_unified_diff
        for rpath in sorted(m.modified_files):
            old = _resolve_entity_file(ctx.storage, m.baseline, EntityType.BASELINE, rpath)
            new = ctx.storage.get_patch_file(n, rpath)
            _print_unified_diff(rpath, old, new if new.exists() else None,
                                f"baseline/{m.baseline}", f"patch/{n}")

@patch.command("rm")
@click.argument("name")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@pass_ctx
def patch_rm(ctx: DeltaContext, name: str, yes: bool) -> None:
    """Remove a patch."""
    ctx.require_init()
    _apply_yes(ctx, yes)
    ctx.storage.get_entity_type(name)
    ui.confirm_or_abort(f"Remove '{name}'?", auto_yes=ctx.auto_yes)
    ctx.storage.remove_patch(name)
    state = ctx.get_state()
    if state.active == name:
        state.active = ""
        ctx.save_state(state)
    ui.print_success(f"'{name}' removed.")


# ======================================================================
# delta template
# ======================================================================

@main.group()
def template():
    """Manage templates."""

@template.command("ls")
@pass_ctx
def template_ls(ctx: DeltaContext) -> None:
    """List templates."""
    ctx.require_init()
    for name in ctx.storage.list_templates():
        ui.print_info(f"  {name}")
    if not ctx.storage.list_templates():
        ui.print_info("No templates.")

@template.command("show")
@click.argument("name")
@pass_ctx
def template_show(ctx: DeltaContext, name: str) -> None:
    """Show template."""
    ctx.require_init()
    t = ctx.storage.load_template(name)
    for line in yaml.dump(t.to_dict(), default_flow_style=False, sort_keys=False).splitlines():
        ui.print_info(f"  {line}")

@template.command("rm")
@click.argument("name")
@pass_ctx
def template_rm(ctx: DeltaContext, name: str) -> None:
    """Remove template."""
    ctx.require_init()
    ctx.storage.remove_template(name)
    ui.print_success(f"Template '{name}' removed.")


# ======================================================================
# delta edit
# ======================================================================

@main.command()
@click.argument("target", type=click.Choice(["config", "template", "baseline", "patch", "ignore"]))
@click.argument("name", required=False, default="")
@click.option("--scaffold", is_flag=True, help="Add commented examples of all available fields.")
@pass_ctx
def edit(ctx: DeltaContext, target: str, name: str, scaffold: bool) -> None:
    """Open a file in editor.

    \b
    Use --scaffold to inject commented examples of all fields
    you can configure. Existing content is preserved.

    \b
    Examples:
      delta edit config                    # Edit config
      delta edit ignore                    # Edit .delta/ignore
      delta edit patch wifi                # Edit patch metadata
      delta edit patch wifi --scaffold     # Add all available fields as comments
      delta edit template my-tmpl          # Edit/create template
    """
    ctx.require_init()
    config = ctx.get_config()
    editor = config.editor or os.environ.get("EDITOR", "") or os.environ.get("VISUAL", "")
    if not editor:
        ui.print_error("No editor. Set 'editor' in config or $EDITOR.")
        sys.exit(1)
    try:
        if target == "ignore":
            path = ctx.storage.delta_dir / "ignore"
            if not path.exists():
                path.write_text(_SCAFFOLD_IGNORE)
                ui.print_info(f"Created: {path}")
        else:
            # Use active entity if name not specified
            edit_name = name
            if not edit_name and target in ("patch", "baseline"):
                edit_name = ctx.get_state().active
                if not edit_name:
                    ui.print_error(f"No {target} specified and no active reference.")
                    sys.exit(1)
            path = ctx.storage.get_edit_path(target, edit_name)
            if target == "template" and not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(_SCAFFOLD_TEMPLATE if scaffold else "# Delta template\n")
                ui.print_info(f"Created: {path}")

        if scaffold and path.exists() and target != "ignore":
            _inject_scaffold(path, target)
            ui.print_info("Scaffold comments added.")

        os.system(f"{editor} {path}")
    except DeltaError as e:
        _handle_error(e)
        sys.exit(1)


# ======================================================================
# delta schema
# ======================================================================

@main.command()
@click.argument("target", type=click.Choice(["config", "baseline", "patch", "template", "ignore"]))
def schema(target: str) -> None:
    """Show all available fields for a config/metadata type.

    \b
    Examples:
      delta schema patch       # Show all patch fields
      delta schema baseline    # Show all baseline fields
      delta schema config      # Show all config fields
      delta schema template    # Show all template fields
    """
    schemas = {
        "config": _SCHEMA_CONFIG,
        "baseline": _SCHEMA_BASELINE,
        "patch": _SCHEMA_PATCH,
        "template": _SCHEMA_TEMPLATE,
        "ignore": _SCHEMA_IGNORE,
    }
    click.echo(schemas[target])


# Schema definitions
_SCHEMA_CONFIG = """\
Config (.delta/config.yaml):
  ssh:
    host              (string)   Device SSH host
    port              (int)      SSH port [22]
    user              (string)   SSH user [root]
    key_file          (string)   Path to SSH private key
    connect_timeout   (int)      Connection timeout seconds [30]
  transfer:
    method            (string)   Transfer method: auto|rsync|tar|sftp [auto]
    compress          (bool)     Enable rsync compression [false]
  editor              (string)   Editor for 'delta edit' [$EDITOR]
  templates:
    default_patch     (string)   Default template for patch commit
    default_baseline  (string)   Default template for init
  log:
    enabled           (bool)     Enable log file creation [true]
    filename_pattern  (string)   Log filename pattern
    max_count         (int)      Max log files [50]
    max_size_mb       (int)      Max total log size MB [100]"""

_SCHEMA_BASELINE = """\
Baseline metadata (.delta/baselines/<name>/metadata.yaml):
  name                (string)   Baseline name (auto-set)
  description         (string)   Human-readable description
  tracked_paths       (list)     Remote paths to track
  ignore_patterns     (list)     Regex patterns to exclude
  variables           (list)     Variables for command substitution
    - name            (string)   Variable name
      required        (bool)     Must be provided via --var [true]
      default         (string)   Default value if not required
      description     (string)   Help text shown to user
  on_fetch          (block)    Commands when fetching (delta fetch)
    pre:              (list)     Run before fetch
    post:             (list)     Run after fetch
      - cmd           (string)   Shell command
        optional      (bool)     Continue on failure [false]
        save_output   (bool)     Save stdout [false]
        output_key    (string)   Key name for saved output
  ownership:
    default_owner     (string)   Default file owner [root]
    default_group     (string)   Default file group [root]
    default_mode      (string)   Default file mode [0644]
    exceptions        (list)     Per-file overrides
      - path          (string)   File path
        owner         (string)   Owner override
        group         (string)   Group override
        mode          (string)   Mode override"""

_SCHEMA_PATCH = """\
Patch metadata (.delta/patches/<name>/metadata.yaml):
  name                (string)   Patch name (auto-set)
  baseline            (string)   Parent baseline name (auto-set)
  description         (string)   Human-readable description
  ignore_patterns     (list)     Additional regex patterns to exclude
  variables           (list)     Variables for command substitution
    - name            (string)   Variable name
      required        (bool)     Must be provided via --var [true]
      default         (string)   Default value if not required
      description     (string)   Help text shown to user
  on_fetch          (block)    Commands when fetching (delta fetch)
    pre:              (list)     Run before fetch
    post:             (list)     Run after fetch
      - cmd           (string)   Shell command
        optional      (bool)     Continue on failure [false]
        save_output   (bool)     Save stdout [false]
        output_key    (string)   Key name for saved output
  on_apply            (block)    Commands when applying (delta apply)
    pre:              (list)     Run before uploading files
    post:             (list)     Run after uploading files
      - cmd           (string)   Shell command
        optional      (bool)     Continue on failure [false]
        save_output   (bool)     Save stdout [false]
        output_key    (string)   Key name for saved output
  ownership:
    default_owner     (string)   Default file owner [root]
    default_group     (string)   Default file group [root]
    default_mode      (string)   Default file mode [0644]"""

_SCHEMA_TEMPLATE = """\
Template (.delta/templates/<name>.yaml):
  description         (string)   Description template (supports ${VAR})
  tracked_paths       (list)     Default tracked paths
  ignore_patterns     (list)     Default ignore patterns
  variables           (list)     Variable definitions (same as baseline/patch)
  on_fetch          (block)    Default fetch commands (same as baseline/patch)
  on_apply            (block)    Default apply commands (same as patch)"""

_SCHEMA_IGNORE = """\
Ignore file (.delta/ignore):
  One regex pattern per line. Lines starting with # are comments.
  Patterns are matched against full file paths (e.g. /etc/app.log).
  Applied globally to all operations (fetch, diff, patch add).

  Examples:
    .*\\.log$              # All .log files
    .*\\.tmp$              # All .tmp files
    .*__pycache__.*        # Python cache
    /etc/machine-id        # Specific file
    /var/log/.*            # Everything under /var/log/"""


# Scaffold content
_SCAFFOLD_IGNORE = """\
# Delta ignore patterns (one regex per line)
# These are applied globally to all operations.
#
# .*\\.log$
# .*\\.tmp$
# .*__pycache__.*
# /etc/machine-id
"""

_SCAFFOLD_TEMPLATE = """\
# Delta template — uncomment and fill in fields you need.
#
# description: ""
#
# tracked_paths:
#   - /etc
#   - /opt/app
#
# ignore_patterns:
#   - ".*\\\\.log$"
#   - ".*\\\\.tmp$"
#
# variables:
#   - name: DEVICE_ID
#     required: true
#     default: ""
#     description: "Device identifier"
#
# on_fetch:
#   pre:
#     - cmd: "systemctl stop app"
#     - cmd: "uname -r"
#       save_output: true
#       output_key: kernel
#   post:
#     - cmd: "systemctl start app"
#
# on_apply:
#   pre:
#     - cmd: "systemctl stop app"
#   post:
#     - cmd: "systemctl daemon-reload"
#     - cmd: "systemctl start app"
#       optional: true
"""

_SCAFFOLD_BASELINE = """\
#
# Available fields (uncomment to use):
#
# description: ""
#
# ignore_patterns:
#   - ".*\\\\.log$"
#
# variables:
#   - name: DEVICE_ID
#     required: true
#     default: ""
#     description: "Device identifier"
#
# on_fetch:
#   pre:
#     - cmd: "systemctl stop app"
#     - cmd: "uname -r"
#       save_output: true
#       output_key: kernel
#   post:
#     - cmd: "systemctl start app"
#
# ownership:
#   default_owner: root
#   default_group: root
#   default_mode: "0644"
#   exceptions:
#     - path: /opt/app/bin/run
#       owner: app
#       group: app
#       mode: "0755"
"""

_SCAFFOLD_PATCH = """\
#
# Available fields (uncomment to use):
#
# description: ""
#
# ignore_patterns:
#   - ".*\\\\.log$"
#
# variables:
#   - name: DEVICE_ID
#     required: true
#     default: ""
#     description: "Device identifier"
#
# on_fetch:
#   pre:
#     - cmd: "systemctl stop app"
#   post:
#     - cmd: "systemctl start app"
#
# on_apply:
#   pre:
#     - cmd: "systemctl stop app"
#     - cmd: "test -f /etc/backup.conf || cp /etc/config.conf /etc/backup.conf"
#       optional: true
#   post:
#     - cmd: "systemctl daemon-reload"
#     - cmd: "systemctl start app"
#       optional: true
#
# ownership:
#   default_owner: root
#   default_group: root
#   default_mode: "0644"
"""


def _inject_scaffold(path: Path, target: str) -> None:
    """Append scaffold comments to existing file if not already present."""
    scaffolds = {
        "baseline": _SCAFFOLD_BASELINE,
        "patch": _SCAFFOLD_PATCH,
        "template": _SCAFFOLD_TEMPLATE,
        "config": "",  # Config already has all fields
    }
    scaffold = scaffolds.get(target, "")
    if not scaffold:
        return

    content = path.read_text(encoding="utf-8")
    # Don't add if scaffold marker already present
    if "Available fields" in content:
        return
    path.write_text(content.rstrip("\n") + "\n\n" + scaffold, encoding="utf-8")


# ======================================================================
# delta config
# ======================================================================

@main.group("config")
def config_group() -> None:
    """View and modify configuration."""

@config_group.command("show")
@pass_ctx
def config_show(ctx: DeltaContext) -> None:
    """Show config."""
    ctx.ensure_init()
    for line in yaml.dump(ctx.get_config().to_dict(), default_flow_style=False, sort_keys=False).splitlines():
        ui.print_info(f"  {line}")

@config_group.command("get")
@click.argument("key")
@pass_ctx
def config_get(ctx: DeltaContext, key: str) -> None:
    """Get config value (dot notation)."""
    ctx.ensure_init()
    val = _dict_get(ctx.get_config().to_dict(), key)
    if val is None:
        ui.print_error(f"Key '{key}' not found.")
        sys.exit(1)
    ui.print_info(str(val))

@config_group.command("set")
@click.argument("key")
@click.argument("value")
@pass_ctx
def config_set(ctx: DeltaContext, key: str, value: str) -> None:
    """Set config value (dot notation, auto-parses types).

    \b
    Examples:
      delta config set ssh.host 10.0.0.2
      delta config set transfer.compress true
      delta config set editor vim
      delta config set log.enabled false
    """
    ctx.ensure_init()
    d = ctx.get_config().to_dict()
    parsed = _parse_value(value)
    if not _dict_set(d, key, parsed):
        ui.print_error(f"Key '{key}' not found.")
        sys.exit(1)
    ctx.storage.save_config(DeltaConfig.from_dict(d))
    ui.print_success(f"{key} = {parsed}")


# ======================================================================
# delta cache clean
# ======================================================================

@main.command("cache")
@click.argument("action", type=click.Choice(["clean"]))
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@pass_ctx
def cache_cmd(ctx: DeltaContext, action: str, yes: bool) -> None:
    """Manage cached data from fetch.

    \b
    Examples:
      delta cache clean          # Remove all cached scan data and files
    """
    ctx.require_init()
    _apply_yes(ctx, yes)
    cache_dir = ctx.storage.cache_files_dir
    scan_file = ctx.storage.delta_dir / "cache" / "scan.yaml"

    has_cache = cache_dir.exists() or scan_file.exists()
    if not has_cache:
        ui.print_info("Cache is empty.")
        return

    # Calculate size
    total_size = 0
    file_count = 0
    if cache_dir.exists():
        for f in cache_dir.rglob("*"):
            if f.is_file():
                total_size += f.stat().st_size
                file_count += 1

    ui.confirm_or_abort(
        f"Remove cache ({file_count} files, {ui.format_size(total_size)})?",
        auto_yes=ctx.auto_yes,
    )

    import shutil
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    if scan_file.exists():
        scan_file.unlink()
    ui.print_success("Cache cleared.")


# ======================================================================
# delta copy-fields
# ======================================================================

@main.command("copy-fields")
@click.argument("source")
@click.argument("target")
@click.option("--fields", "-f", multiple=True, required=True)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@pass_ctx
def copy_fields(ctx: DeltaContext, source: str, target: str, fields: tuple[str, ...], yes: bool) -> None:
    """Copy fields between entities."""
    ctx.require_init()
    _apply_yes(ctx, yes)
    VALID = {"description", "variables", "ignore_patterns", "on_fetch", "on_apply"}
    bad = set(fields) - VALID
    if bad:
        ui.print_error(f"Invalid: {', '.join(bad)}. Valid: {', '.join(sorted(VALID))}")
        sys.exit(1)
    st = ctx.storage.get_entity_type(source)
    src = ctx.storage.load_baseline(source) if st == EntityType.BASELINE else ctx.storage.load_patch(source)
    tt = ctx.storage.get_entity_type(target)
    tgt = ctx.storage.load_baseline(target) if tt == EntityType.BASELINE else ctx.storage.load_patch(target)
    ui.confirm_or_abort(f"Copy {', '.join(fields)} from {source} to {target}?", auto_yes=ctx.auto_yes)
    for f in fields:
        setattr(tgt, f, getattr(src, f, None))
    (ctx.storage.save_baseline if tt == EntityType.BASELINE else ctx.storage.save_patch)(tgt)
    ui.print_success(f"Copied {len(fields)} field(s).")


# ======================================================================
# delta log
# ======================================================================

@main.group()
def log() -> None:
    """Manage logs."""

@log.command("ls")
@pass_ctx
def log_ls(ctx: DeltaContext) -> None:
    """List logs."""
    ctx.require_init()
    files = sorted(ctx.storage.logs_dir.glob("*.log"), reverse=True)
    for f in files[:20]:
        ui.print_info(f"  {f.name}  ({ui.format_size(f.stat().st_size)})")
    if not files:
        ui.print_info("No logs.")
    elif len(files) > 20:
        ui.print_dim(f"... and {len(files) - 20} more")

@log.command("show")
@click.argument("name", required=False)
@pass_ctx
def log_show(ctx: DeltaContext, name: str | None) -> None:
    """Show log file."""
    ctx.require_init()
    files = sorted(ctx.storage.logs_dir.glob("*.log"), reverse=True)
    path = ctx.storage.logs_dir / name if name else (files[0] if files else None)
    if not path or not path.exists():
        ui.print_info("No logs.")
        return
    click.echo(path.read_text(encoding="utf-8", errors="replace"))

@log.command("clean")
@click.option("--keep", default=10)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@pass_ctx
def log_clean(ctx: DeltaContext, keep: int, yes: bool) -> None:
    """Remove old logs."""
    ctx.require_init()
    _apply_yes(ctx, yes)
    files = sorted(ctx.storage.logs_dir.glob("*.log"), reverse=True)
    to_rm = files[keep:]
    if not to_rm:
        ui.print_info("Nothing to clean.")
        return
    ui.confirm_or_abort(f"Remove {len(to_rm)} logs?", auto_yes=ctx.auto_yes)
    for f in to_rm:
        f.unlink()
    ui.print_success(f"Removed {len(to_rm)} logs.")


# ======================================================================
# delta export
# ======================================================================

@main.command("export")
@click.argument("name", required=False)
@click.option("--out-dir", "-o", default="", help="Output directory (default: current dir).")
@pass_ctx
def export_patch(ctx: DeltaContext, name: str | None, out_dir: str) -> None:
    """Export a patch as a portable archive.

    \b
    The archive contains everything needed to apply the patch
    on another machine. No baseline or config needed.

    \b
    Examples:
      delta export wifi                # → ./wifi.tar.gz
      delta export wifi -o /tmp        # → /tmp/wifi.tar.gz
      delta export                     # Export active patch
    """
    import tarfile

    ctx.require_init()
    patch_name = name or ctx.get_state().active
    if not patch_name:
        ui.print_error("No patch specified or active.")
        sys.exit(1)
    if ctx.storage.get_entity_type(patch_name) != EntityType.PATCH:
        ui.print_error(f"'{patch_name}' is not a patch.")
        sys.exit(1)

    patch_dir = ctx.storage._patch_dir(patch_name)
    if not patch_dir.exists():
        ui.print_error(f"Patch directory not found: {patch_dir}")
        sys.exit(1)

    out_path = Path(out_dir) if out_dir else Path.cwd()
    out_path.mkdir(parents=True, exist_ok=True)
    archive = out_path / f"{patch_name}.tar.gz"

    if archive.exists():
        ui.print_warning(f"Overwriting {archive}")

    meta = ctx.storage.load_patch(patch_name)
    with tarfile.open(archive, "w:gz") as tar:
        for f in patch_dir.rglob("*"):
            arcname = f"{patch_name}/{f.relative_to(patch_dir)}"
            tar.add(f, arcname=arcname)

    total = len(meta.modified_files) + len(meta.created_files) + len(meta.deleted_files)
    ui.print_success(f"Exported '{patch_name}' → {archive}")
    ui.print_info(f"  {total} files, {ui.format_size(archive.stat().st_size)}")


# ======================================================================
# delta import
# ======================================================================

@main.command("import")
@click.argument("archive_paths", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation.")
@pass_ctx
def import_patch(ctx: DeltaContext, archive_paths: tuple[str, ...], yes: bool) -> None:
    """Import patches from archives.

    \b
    Archives created by 'delta export'. Accepts multiple files.

    \b
    Examples:
      delta import wifi.tar.gz
      delta import patches/*.tar.gz --yes
    """
    import tarfile

    ctx.require_init()
    _apply_yes(ctx, yes)

    imported = 0
    for archive_path in archive_paths:
        archive = Path(archive_path)
        if not tarfile.is_tarfile(archive):
            ui.print_warning(f"Skipping (not a valid archive): {archive}")
            continue

        with tarfile.open(archive, "r:gz") as tar:
            members = tar.getnames()
            if not members:
                ui.print_warning(f"Skipping (empty): {archive}")
                continue
            patch_name = members[0].split("/")[0]
            if not patch_name:
                ui.print_warning(f"Skipping (invalid structure): {archive}")
                continue

        if ctx.storage.name_exists(patch_name):
            ui.confirm_or_abort(
                f"'{patch_name}' already exists. Overwrite?",
                auto_yes=ctx.auto_yes,
            )
            ctx.storage.remove_patch(patch_name)

        patches_dir = ctx.storage.delta_dir / "patches"
        patches_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(path=patches_dir, filter="data")

        meta = ctx.storage.load_patch(patch_name)
        total = len(meta.modified_files) + len(meta.created_files) + len(meta.deleted_files)
        ui.print_success(f"Imported '{patch_name}' ({total} files).")
        if meta.description:
            ui.print_info(f"  Description: {meta.description}")
        imported += 1

    if imported > 1:
        ui.print_success(f"{imported} patches imported.")


# ======================================================================
# Helpers
# ======================================================================

def _require_scan(ctx: DeltaContext):
    scan = ctx.storage.load_scan()
    if not scan:
        ui.print_error("No cached data. Run 'delta fetch' first.")
        sys.exit(1)
    return scan

def _validate_cache(scan, config: DeltaConfig, expected_ref: str) -> None:
    if scan.host and config.ssh.host and scan.host != config.ssh.host:
        ui.print_warning(f"Cache is from {scan.host}, current host is {config.ssh.host}. Run 'delta fetch'.")
    if scan.reference and scan.reference != expected_ref:
        ui.print_warning(f"Cache fetched against '{scan.reference}', comparing against '{expected_ref}'. Run 'delta fetch'.")

def _get_tracked_paths(storage: Storage, ref_name: str, ref_type: EntityType) -> list[str]:
    if ref_type == EntityType.BASELINE:
        return storage.load_baseline(ref_name).tracked_paths
    pm = storage.load_patch(ref_name)
    try:
        return storage.load_baseline(pm.baseline).tracked_paths
    except Exception:
        return []

def _run_entity_cmds(ctx, conn, ref_name, ref_type, phase, skip):
    if skip:
        return
    from delta.remote_cmd import execute_commands
    meta = (ctx.storage.load_baseline(ref_name) if ref_type == EntityType.BASELINE
            else ctx.storage.load_patch(ref_name))
    cmds = meta.on_fetch.pre if phase == "pre" else meta.on_fetch.post
    if cmds:
        all_vars = dict(meta.command_outputs)
        all_vars.update(ctx.var_map)
        execute_commands(conn, cmds, all_vars, phase=f"{phase}-fetch")

def _load_template(storage, name):
    return storage.load_template(name) if name else None

def _load_config_file(path):
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _parse_cmd_args(args):
    result = []
    for arg in args:
        if arg.startswith("save:"):
            parts = arg.split(":", 2)
            if len(parts) >= 3:
                result.append(CommandSpec(cmd=parts[2], save_output=True, output_key=parts[1]))
                continue
        result.append(CommandSpec(cmd=arg))
    return result

def _dict_get(d, key):
    for part in key.split("."):
        if isinstance(d, dict) and part in d:
            d = d[part]
        else:
            return None
    return d

def _dict_set(d, key, value):
    parts = key.split(".")
    for part in parts[:-1]:
        if isinstance(d, dict) and part in d:
            d = d[part]
        else:
            return False
    if isinstance(d, dict):
        d[parts[-1]] = value
        return True
    return False

def _parse_value(value):
    if value.lower() in ("true", "yes"):
        return True
    if value.lower() in ("false", "no"):
        return False
    try:
        return int(value)
    except ValueError:
        return value


def _handle_error(e: Exception) -> None:
    """Print error, but skip if it's already in the collector (avoids duplication)."""
    from delta.exceptions import RemoteCommandError
    collector = ui.get_collector()
    if isinstance(e, RemoteCommandError) and collector and collector.has_issues:
        return  # Will be shown in summary
    ui.print_error(str(e))


def _apply_yes(ctx: DeltaContext, yes: bool) -> None:
    ctx.auto_yes = yes


def _apply_vars(ctx: DeltaContext, var: tuple[str, ...]) -> None:
    from delta.remote_cmd import parse_var_args
    try:
        ctx.var_map = parse_var_args(var)
    except ValidationError as e:
        ui.print_error(str(e))
        sys.exit(1)


def _display_resolved_config(
    ctx: DeltaContext,
    operation: str,
    *,
    extra_ignore: list[str] | None = None,
    template_name: str = "",
) -> None:
    """Display resolved config for --show-config and exit."""
    state = ctx.get_state()
    ref_name = state.active
    if not ref_name:
        ui.print_error("No active reference.")
        sys.exit(1)
    ref_type = ctx.storage.get_entity_type(ref_name)
    config = ctx.get_config()

    from delta.diff_ops import resolve_config
    rc = resolve_config(
        ctx.storage, operation, ref_name, ref_type,
        config=config, template_name=template_name,
        extra_ignore=extra_ignore,
    )
    ui.print_info(rc.display())
