#!/usr/bin/env python3
"""
delta - synchronize changes across identical devices over SSH.

Workflow:
  1. delta config --add-watched-dir /etc/myapp  configure directories to track
  2. delta snapshot [DIR ...] [-b NAME]         save current device state as baseline
  3. (make changes on the device via SSH)
  4. delta diff [-p NAME]                       compare, generate patch
  5. delta apply [-p NAME] [host ...]           deploy patch to devices
  6. delta rollback [host ...]                  revert devices back to baseline

Other commands:
  delta deploy [master] host1 host2 ...         direct rsync, no patch involved
  delta diff-commands [-p NAME]                 sync commands into manifest without re-fetch
  delta patch                                   list all patches
  delta patch add-file LOCAL REMOTE [-p NAME]   add/replace file in patch
  delta patch remove-file REMOTE [-p NAME]      remove file from patch
  delta patch copy SRC DEST                     copy a patch under new name
  delta patch rename OLD NEW                    rename a patch
  delta patch delete NAME [-y]                  delete a patch
  delta patch set-format FORMAT [-p NAME]       set display format for patch list
  delta baseline                                list all baselines
  delta baseline rename OLD NEW                 rename a baseline
  delta baseline delete NAME [-y]               delete a baseline
  delta baseline set-format FORMAT [-b NAME]    set display format for baseline list
  delta pack [-p NAME] [-o file.tar.gz]         pack patch into distributable archive
  delta status [-p NAME]                        show patch summary
  delta logs                                    list log files
  delta logs clear [-y]                         delete all log files
  delta config [--set-* / --add-* ...]          view or edit config

Design principles:
  - Only `delta config` modifies config.json
  - snapshot reads config, writes only to baseline/meta.json
  - diff reads from baseline meta, writes only to patch/manifest.json
  - apply reads from manifest, never modifies config

Host resolution:
  source (snapshot/diff/rollback-read/deploy-master):
    --host → source_host → default_host → error
  targets (apply/rollback-write):
    CLI args → target_hosts (config) → target_hosts (manifest) → default_host → error
  deploy targets: CLI only, no fallback

Config vs meta vs manifest:
  config.json    — persistent settings for this PC (SSH, defaults, watched dirs, commands)
  meta.json      — snapshot of config at snapshot time + ownership + pending sync
  manifest.json  — snapshot of config at diff time + file lists + ownership per file
"""

import argparse
import contextlib
import difflib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path


# ══════════════════════════════════════════════════════════════════════════════
#  Constants
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_WORK_DIR   = Path.cwd() / "delta"
DEFAULT_PATCH_NAME = "default"
DEFAULT_BASELINE   = "default"

DEFAULT_CONFIG: dict = {
    # SSH
    "default_host":     "",   # fallback for both source and target if specific fields empty
    "source_host":      "",   # device to snapshot/diff/rollback from
    "ssh_user":         "root",
    "ssh_port":         22,
    "ssh_key":          "",   # e.g. "~/.ssh/id_ed25519"; empty = SSH agent/default

    # Active patch and baseline names (empty = must specify explicitly)
    "default_patch":    DEFAULT_PATCH_NAME,
    "default_baseline": DEFAULT_BASELINE,

    # What to watch (used as template for new snapshots)
    "watched_dirs":     [],   # e.g. ["/etc/myapp", "/opt/myapp"]

    # Exclusion patterns (Python regex, matched against full absolute path)
    # e.g. ["/device/usr/bin/.*", ".*[.]log"]
    "exclude_patterns": [],

    # Where to store fetched files during diff (empty = {work-dir}/patches/{name}/current)
    "current_dir":      "",

    # Path on remote device for temporary patch archive upload
    "remote_tmp_path":  "/tmp/_delta_patch.tar.gz",

    # Target devices for apply (fallback when no CLI args given)
    "target_hosts":     [],

    # Commands run on device before/after patch (support {VAR} / {VAR=default})
    "pre_commands":     [],
    "post_commands":    [],

    # Fallback file owner when stat fails during diff (e.g. in --skip-fetch mode)
    # If empty and stat fails, diff aborts with an error
    "default_chown":    "",

    # Commands run on source device during snapshot
    # Plain string: run and print. {"cmd": "...", "key": "..."}: store result in meta
    "snapshot_commands": [],

    # Logging
    "log_enabled":  True,
    "log_dir":      "",   # empty = {work-dir}/logs
    "log_filename": "{cmd}_{timestamp}_{result}.log",  # {cmd} {timestamp} {result}
}


# ══════════════════════════════════════════════════════════════════════════════
#  Path layout
# ══════════════════════════════════════════════════════════════════════════════

def get_paths(work_dir: Path,
              patch_name:    str = DEFAULT_PATCH_NAME,
              baseline_name: str = DEFAULT_BASELINE) -> dict:
    patch_dir = work_dir / "patches" / patch_name
    return {
        "work":           work_dir,
        "config":         work_dir / "config.json",
        "baselines_root": work_dir / "baselines",
        "baseline":       work_dir / "baselines" / baseline_name,
        "baseline_meta":  work_dir / "baselines" / baseline_name / "meta.json",
        "patches_root":   work_dir / "patches",
        "patch":          patch_dir,
        "current":        patch_dir / "current",
        "tmp":            work_dir / "tmp",   # for deploy/rollback staging
        "logs":           work_dir / "logs",
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Config
# ══════════════════════════════════════════════════════════════════════════════

def load_config(paths: dict) -> dict:
    f = paths["config"]
    return {**DEFAULT_CONFIG, **json.loads(f.read_text())} if f.exists() \
        else dict(DEFAULT_CONFIG)


def save_config(paths: dict, cfg: dict) -> None:
    paths["work"].mkdir(parents=True, exist_ok=True)
    paths["config"].write_text(json.dumps(cfg, indent=2, ensure_ascii=False))


# ══════════════════════════════════════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════════════════════════════════════

class _Tee:
    """Write to both stdout/stderr and a log file simultaneously."""
    def __init__(self, original, log_file):
        self._orig = original
        self._log  = log_file

    def write(self, data):
        self._orig.write(data)
        self._log.write(data)

    def flush(self):
        self._orig.flush()
        self._log.flush()

    def fileno(self):  return self._orig.fileno()
    def isatty(self):  return self._orig.isatty()


_log_handle:          object       = None
_log_tmp_path: Path | None         = None


def open_log(cfg: dict, paths: dict, cmd: str) -> None:
    global _log_handle, _log_tmp_path
    if not cfg.get("log_enabled"):
        return
    log_dir = Path(cfg["log_dir"]) if cfg.get("log_dir") else paths["logs"]
    log_dir.mkdir(parents=True, exist_ok=True)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    pattern = cfg.get("log_filename", "{cmd}_{timestamp}_{result}.log")
    name    = pattern.replace("{cmd}", cmd).replace("{timestamp}", ts) \
                     .replace("{result}", "__RESULT__")
    _log_tmp_path = log_dir / name
    _log_handle   = open(_log_tmp_path, "w", encoding="utf-8")
    sys.stdout    = _Tee(sys.__stdout__, _log_handle)
    sys.stderr    = _Tee(sys.__stderr__, _log_handle)
    print(f"📝  Logging to: {_log_tmp_path}")


def close_log(result: str = "success") -> None:
    global _log_handle, _log_tmp_path
    if not _log_handle:
        return
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    _log_handle.close()
    _log_handle = None
    if _log_tmp_path and _log_tmp_path.exists():
        final = _log_tmp_path.parent / _log_tmp_path.name.replace("__RESULT__", result)
        _log_tmp_path.rename(final)
        if final != _log_tmp_path:
            print(f"📝  Log saved: {final}")
    _log_tmp_path = None


def check_log_size(cfg: dict, paths: dict) -> None:
    log_dir = Path(cfg["log_dir"]) if cfg.get("log_dir") else paths["logs"]
    if not log_dir.exists():
        return
    files   = list(log_dir.glob("*.log"))
    count   = len(files)
    size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
    if count > 100 or size_mb > 50:
        w = 60
        print()
        print("╔" + "═" * w + "╗")
        print("║  💡  Log cleanup recommendation" + " " * (w - 32) + "║")
        print("╠" + "═" * w + "╣")
        print(f"║   Log files : {count:<{w-15}}║")
        print(f"║   Total size: {size_mb:.1f} MB{' '*(w-16-len(f'{size_mb:.1f} MB'))}║")
        print("║" + " " * w + "║")
        print("║   delta logs clear" + " " * (w - 19) + "║")
        print("╚" + "═" * w + "╝")


# ══════════════════════════════════════════════════════════════════════════════
#  SSH / rsync
# ══════════════════════════════════════════════════════════════════════════════

def _ssh_opts(cfg: dict) -> list:
    opts = ["-p", str(cfg["ssh_port"]),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10"]
    if cfg.get("ssh_key"):
        opts += ["-i", str(Path(cfg["ssh_key"]).expanduser())]
    return opts


def _scp_opts(cfg: dict) -> list:
    opts = ["-P", str(cfg["ssh_port"]),
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10"]
    if cfg.get("ssh_key"):
        opts += ["-i", str(Path(cfg["ssh_key"]).expanduser())]
    return opts


def _with_sock(base: list, sock: str | None) -> list:
    return base + ["-o", f"ControlPath={sock}", "-o", "ControlMaster=no"] \
        if sock else base


@contextlib.contextmanager
def ssh_master(cfg: dict, host: str):
    """Open a persistent SSH ControlMaster — one TCP connection for all operations."""
    sock = f"/tmp/delta_ctl_{host}_{os.getpid()}"
    proc = subprocess.Popen(
        ["ssh", *_ssh_opts(cfg), "-M", "-S", sock,
         "-o", "ControlPersist=yes", "-N", f"{cfg['ssh_user']}@{host}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(30):
        if Path(sock).exists():
            break
        time.sleep(0.5)
    else:
        proc.terminate()
        raise RuntimeError(f"Could not connect to {host}. Check host/credentials.")
    print(f"  🔗  SSH → {host}")
    try:
        yield sock
    finally:
        subprocess.run(["ssh", "-S", sock, "-O", "exit", f"{cfg['ssh_user']}@{host}"],
                       capture_output=True)
        Path(sock).unlink(missing_ok=True)
        proc.wait()
        print(f"  🔌  SSH ← {host}")


def run_cmd(cmd: list, check: bool = True) -> int:
    """Run command, streaming output through sys.stdout (captured by Tee if logging)."""
    print(f"  $ {' '.join(str(c) for c in cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        print(line, end="")
    proc.wait()
    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    return proc.returncode


def ssh_capture(cfg: dict, host: str, cmd_str: str,
                sock: str | None = None) -> str:
    opts = _with_sock(_ssh_opts(cfg), sock)
    r    = subprocess.run(["ssh", *opts, f"{cfg['ssh_user']}@{host}", cmd_str],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return r.stdout.strip()


def rsync_pull(cfg: dict, host: str, remote_dir: str,
               local_dest: Path, excludes: list, sock: str | None = None) -> None:
    """
    Mirror remote_dir to local_dest, respecting regex exclude patterns.

    Strategy:
    1. Get the full file list from the device via `find`.
    2. Filter it locally using is_excluded() (regex, not glob).
    3. Pass the accepted list to rsync --files-from so only those files
       are transferred — excluded files are never downloaded.
    4. Remove from the local mirror any files that are now excluded.
    """
    local_dest.mkdir(parents=True, exist_ok=True)
    user  = cfg["ssh_user"]
    ssh   = _with_sock(_ssh_opts(cfg), sock)

    # Step 1: list all files on the device
    raw = ssh_capture(cfg, host,
                      f'find {remote_dir} -type f -follow 2>/dev/null', sock)
    all_remote = [l.strip() for l in raw.splitlines() if l.strip()]

    # Step 2: filter — keep only files that are NOT excluded
    prefix = remote_dir.rstrip("/")
    accepted = []
    for abs_path in all_remote:
        # rel_path relative to remote_dir
        if abs_path.startswith(prefix + "/"):
            rel = abs_path[len(prefix) + 1:]
        else:
            rel = abs_path.lstrip("/")
        if not is_excluded(rel, excludes, remote_dir):
            accepted.append(rel)

    # Step 3: remove locally any files that are now excluded
    if local_dest.exists():
        for local_file in local_dest.rglob("*"):
            if not local_file.is_file():
                continue
            rel = str(local_file.relative_to(local_dest))
            if is_excluded(rel, excludes, remote_dir):
                local_file.unlink()

    if not accepted:
        print(f"  ℹ️   No files to sync in {remote_dir} (all excluded or empty)")
        return

    # Step 4: rsync only the accepted files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as ff:
        ff.write("\n".join(accepted))
        flist = ff.name

    try:
        run_cmd(["rsync", "-avz", "--links",
                 "--files-from", flist,
                 "-e", "ssh " + " ".join(ssh),
                 f"{user}@{host}:{remote_dir}/",
                 str(local_dest) + "/"])
    finally:
        Path(flist).unlink(missing_ok=True)


def rsync_push(cfg: dict, local_src: Path, host: str, remote_dir: str,
               excludes: list, sock: str | None = None) -> None:
    # Same as rsync_pull — no excludes passed to rsync, filtering is local only.
    run_cmd(["rsync", "-avz", "--delete", "--links",
             "-e", "ssh " + " ".join(_with_sock(_ssh_opts(cfg), sock)),
             str(local_src) + "/",
             f"{cfg['ssh_user']}@{host}:{remote_dir}/"])


# ══════════════════════════════════════════════════════════════════════════════
#  File utilities
# ══════════════════════════════════════════════════════════════════════════════

def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def is_excluded(rel_path: str, patterns: list, watched_dir: str = "") -> bool:
    """
    Match rel_path against exclusion patterns using re.fullmatch.
    Pattern is matched against the full absolute path: {watched_dir}/{rel_path}.

    Examples:
      "/device/usr/bin/wifi"   — exact file (fullmatch, so no partial matches)
      "/device/usr/bin/wifi.*" — wifi and anything starting with wifi
      "/device/usr/bin/.*"     — everything under /device/usr/bin/
      ".*/usr/bin/.*"          — usr/bin/ in any watched dir
      ".*[.]log"               — any .log file in any watched dir
    """
    abs_path = (watched_dir.rstrip("/") + "/" + rel_path) if watched_dir else rel_path
    for pat in patterns:
        try:
            if re.fullmatch(pat, abs_path):
                return True
        except re.error:
            pass
    return False


def validate_patterns(patterns: list) -> list:
    """Return list of error strings for invalid regex patterns."""
    errors = []
    for p in patterns:
        try:
            re.compile(p)
        except re.error as e:
            errors.append(f"  Invalid regex {p!r}: {e}")
    return errors


def collect_files(base: Path, excludes: list, watched_dir: str = "") -> dict:
    """Return {relative_path: sha256}. Dangling/dir symlinks skipped."""
    result = {}
    if not base.exists():
        return result
    for p in base.rglob("*"):
        if p.is_symlink():
            if not p.exists() or p.is_dir():
                continue
        elif not p.is_file():
            continue
        rel = str(p.relative_to(base))
        if not is_excluded(rel, excludes, watched_dir):
            result[rel] = file_hash(p)
    return result


def fetch_owners(cfg: dict, host: str, paths_list: list,
                 sock: str | None = None) -> dict:
    """Return {absolute_path: 'user:group'} via stat on the device."""
    owners = {}
    for i in range(0, len(paths_list), 200):
        chunk  = paths_list[i:i + 200]
        quoted = " ".join(f'"{p}"' for p in chunk)
        out    = ssh_capture(cfg, host, f'stat -c "%U:%G %n" {quoted}', sock)
        for line in out.splitlines():
            parts = line.split(" ", 1)
            if len(parts) == 2:
                owners[parts[1].strip()] = parts[0].strip()
    return owners


def compute_chown(owners: dict) -> tuple[str, dict]:
    """Return (default_chown, overrides) where default is the most common owner."""
    if not owners:
        return "", {}
    default  = Counter(owners.values()).most_common(1)[0][0]
    return default, {p: o for p, o in owners.items() if o != default}


def make_file_entries(paths_list: list, default_chown: str,
                      overrides: dict) -> list:
    """Build [{path}, {path, chown}] list for manifest."""
    result = []
    for p in paths_list:
        entry: dict = {"path": p}
        owner = overrides.get(p, default_chown)
        if owner and owner != default_chown:
            entry["chown"] = owner
        result.append(entry)
    return result


def entry_path(e) -> str:
    return e["path"] if isinstance(e, dict) else e


def entry_chown(e, default_chown: str) -> str:
    return e.get("chown", default_chown) if isinstance(e, dict) else default_chown


def all_paths(entries: list) -> list:
    return [entry_path(e) for e in entries]


def is_text(path: Path) -> bool:
    try:
        return b"\x00" not in path.read_bytes()[:8192]
    except Exception:
        return False


def print_diff(a: Path | None, b: Path | None,
               label_a: str = "baseline", label_b: str = "current") -> None:
    for p in (a, b):
        if p and p.exists() and not is_text(p):
            print("  [binary — diff not shown]")
            return
    la = a.read_text(errors="replace").splitlines() if (a and a.exists()) else []
    lb = b.read_text(errors="replace").splitlines() if (b and b.exists()) else []
    diff = list(difflib.unified_diff(la, lb, fromfile=label_a, tofile=label_b,
                                     lineterm=""))
    if not diff:
        return
    R = "\033[0m"; B = "\033[1m"; RED = "\033[31m"; GRN = "\033[32m"; CYN = "\033[36m"
    for line in diff:
        if line.startswith(("---", "+++")):  print(f"{B}{line}{R}")
        elif line.startswith("-"):           print(f"{RED}{line}{R}")
        elif line.startswith("+"):           print(f"{GRN}{line}{R}")
        elif line.startswith("@@"):          print(f"{CYN}{line}{R}")
        else:                                print(line)


def pack_locally(files_to_pack: list, current: Path, patch_tar: Path) -> None:
    with tarfile.open(patch_tar, "w:gz") as tar:
        for full, d, rel in files_to_pack:
            local = current / d.lstrip("/") / rel
            if local.exists():
                tar.add(local, arcname=full.lstrip("/"))


# ══════════════════════════════════════════════════════════════════════════════
#  Host resolution
# ══════════════════════════════════════════════════════════════════════════════

def resolve_source(cfg: dict, given: str | None) -> str:
    host = given or cfg.get("source_host") or cfg.get("default_host") or ""
    if not host:
        print("❌  No source host.")
        print("    delta config --set-source-host IP  (or --set-host as fallback)")
        sys.exit(1)
    return host


def resolve_targets(cfg: dict, given: list,
                    manifest: dict | None = None) -> list:
    if given:
        return given
    if cfg.get("target_hosts"):
        hosts = cfg["target_hosts"]
        print(f"  ℹ️   target_hosts from config: {hosts}")
        return hosts
    if manifest and manifest.get("target_hosts"):
        hosts = manifest["target_hosts"]
        print(f"  ℹ️   target_hosts from manifest: {hosts}")
        return hosts
    if cfg.get("default_host"):
        host = cfg["default_host"]
        print(f"  ℹ️   Using default_host as target: {host}")
        return [host]
    print("❌  No target hosts.")
    print("    delta config --add-target-host IP  (or --set-host as fallback)")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
#  Variable substitution
# ══════════════════════════════════════════════════════════════════════════════

def parse_placeholders(cmd_str: str) -> list:
    return [(m.group(1), m.group(2))
            for m in re.finditer(r"\{(\w+)(?:=([^}]*))?\}", cmd_str)]


def substitute_vars(cmd_str: str, var_map: dict) -> str:
    return re.sub(r"\{(\w+)(?:=[^}]*)?\}",
                  lambda m: var_map.get(m.group(1), m.group(0)), cmd_str)


def validate_vars(commands: list, var_map: dict) -> list:
    """Check all required placeholders are provided. Fills defaults into var_map."""
    errors = []
    seen   = set()
    for cmd_str in commands:
        for key, default in parse_placeholders(cmd_str):
            if key in seen:
                continue
            seen.add(key)
            if key in var_map:
                continue
            if default is not None:
                var_map[key] = default
            else:
                errors.append(
                    f"  ❌  Missing required variable '{{{key}}}'"
                    f" (in: {cmd_str!r})")
    return errors


# ══════════════════════════════════════════════════════════════════════════════
#  Pending sync
# ══════════════════════════════════════════════════════════════════════════════

def add_pending_sync(paths: dict, description: str) -> None:
    mf = paths["baseline_meta"]
    if not mf.exists():
        return
    m = json.loads(mf.read_text())
    pending = m.get("pending_sync", [])
    if description not in pending:
        pending.append(description)
    m["pending_sync"] = pending
    mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))


def warn_pending_sync(paths: dict, skip_confirm: bool = False) -> None:
    mf = paths["baseline_meta"]
    if not mf.exists():
        return
    m       = json.loads(mf.read_text())
    pending = m.get("pending_sync", [])
    if not pending:
        return
    bname = paths["baseline"].name
    print(f"\n{'═'*60}")
    print(f"⚠️   Baseline '{bname}' has pending sync:")
    for p in pending:
        print(f"    • {p}")
    print(f"\n    Fix: delta snapshot --baseline {bname} -f")
    print(f"{'═'*60}\n")
    if skip_confirm:
        print("  --yes: proceeding with stale data\n")
        return
    try:
        answer = input("Continue with stale data? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        answer = ""
    if answer != "y":
        print("Aborted.")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
#  Command consistency check
# ══════════════════════════════════════════════════════════════════════════════

def check_cmd_consistency(cfg: dict, manifest: dict) -> list:
    """
    Verify manifest commands start with all config commands in order.
    config = minimum required set; manifest may have more (added via diff-commands).
    """
    errors = []
    for key, label in [("pre_commands", "pre"), ("post_commands", "post")]:
        cfg_cmds = cfg.get(key, [])
        man_cmds = manifest.get(key, [])
        if not cfg_cmds:
            continue
        if man_cmds[:len(cfg_cmds)] != cfg_cmds:
            errors.append(
                f"  {label}_commands mismatch:\n"
                f"    config   : {cfg_cmds}\n"
                f"    manifest : {man_cmds}\n"
                f"    Manifest must start with all config commands in the same order.")
    return errors


# ══════════════════════════════════════════════════════════════════════════════
#  Visual helpers
# ══════════════════════════════════════════════════════════════════════════════

def stage_header(n: int, total: int, name: str) -> None:
    label = f"  Stage {n}/{total} — {name}  "
    print(f"\n▶▶  {label}{'─' * max(0, 60 - len(label))}")


def print_summary(results: dict) -> None:
    ok  = [h for h, v in results.items() if v]
    err = [h for h, v in results.items() if not v]
    print(f"\n{'═'*60}")
    if err:
        print(f"✅  OK: {len(ok)}    ❌  Failed: {len(err)}")
        print(f"   Failed: {err}")
    else:
        print(f"✅  All {len(ok)} device(s) updated successfully")


def render_format(fmt: str, data: dict) -> str:
    try:
        return fmt.format_map(data)
    except (KeyError, ValueError):
        return fmt


# ══════════════════════════════════════════════════════════════════════════════
#  Baseline meta helpers
# ══════════════════════════════════════════════════════════════════════════════

def read_baseline_meta(paths: dict) -> dict:
    mf = paths["baseline_meta"]
    return json.loads(mf.read_text()) if mf.exists() else {}


def write_baseline_meta(paths: dict, meta: dict) -> None:
    paths["baseline"].mkdir(parents=True, exist_ok=True)
    paths["baseline_meta"].write_text(
        json.dumps(meta, indent=2, ensure_ascii=False))


# ══════════════════════════════════════════════════════════════════════════════
#  Commands
# ══════════════════════════════════════════════════════════════════════════════

# ── snapshot ──────────────────────────────────────────────────────────────────

def cmd_snapshot(args, paths: dict, cfg: dict) -> None:
    """
    Fetch watched directories from the device and save as a named baseline.
    Only writes to baseline/meta.json — never modifies config.json.

    watched_dirs = config.watched_dirs + CLI dirs (CLI dirs not saved to config).
    exclude_patterns = config.exclude_patterns + --exclude (not saved to config).
    --remove-exclude removes from this baseline's meta only (not from config).

    If only --exclude/--remove-exclude is passed (no -f) and the baseline exists,
    no SSH connection is made for exclude additions (local operation only).
    """
    if not cfg["watched_dirs"] and not args.dirs:
        print("❌  No directories to watch.")
        print("    delta config --add-watched-dir /etc/myapp")
        print("    delta snapshot /etc/myapp /opt/myapp")
        sys.exit(1)

    watched = list(cfg["watched_dirs"])
    for d in (args.dirs or []):
        if d not in watched:
            watched.append(d)

    # Merge exclude_patterns: config (global) union meta (baseline-local).
    # Config patterns are always included — adding to config affects all baselines
    # on next snapshot. Meta may have additional baseline-specific patterns.
    existing_meta = read_baseline_meta(paths)
    cfg_excludes  = cfg.get("exclude_patterns", [])
    meta_only     = [p for p in existing_meta.get("exclude_patterns", [])
                     if p not in cfg_excludes]
    meta_excludes = list(cfg_excludes) + meta_only

    # Validate and process --exclude (adds to meta, not config)
    new_excludes: list = []
    if args.exclude:
        new_excludes = [e for e in args.exclude if e not in meta_excludes]
        if new_excludes:
            errs = validate_patterns(new_excludes)
            if errs:
                for e in errs: print(e)
                sys.exit(1)

    # Validate and process --remove-exclude
    removed_excludes: list = []
    for pat in (getattr(args, "remove_exclude", None) or []):
        for i in range(len(meta_excludes) - 1, -1, -1):
            if meta_excludes[i] == pat:
                meta_excludes.pop(i)
                removed_excludes.append(pat)
                break
        else:
            print(f"❌  Pattern not found in baseline exclude_patterns: {pat!r}")
            sys.exit(1)

    # Apply new excludes
    for pat in new_excludes:
        if pat not in meta_excludes:
            meta_excludes.append(pat)

    excludes      = meta_excludes
    baseline      = paths["baseline"]
    baseline_name = baseline.name

    # ── Exclude-only mode (no SSH needed for additions) ──────────────────────
    if new_excludes and not removed_excludes and not args.force:
        if not baseline.exists():
            print(f"\n❌  Baseline '{baseline_name}' does not exist yet.")
            print(f"    Run a full snapshot first, then add excludes:")
            print(f"    delta snapshot --baseline {baseline_name}")
            print(f"    delta snapshot --baseline {baseline_name}"
                  f" --exclude {' '.join(new_excludes)}")
            sys.exit(1)
        print(f"\n🚫  New exclude pattern(s): {new_excludes}")
        print(f"    Scanning local baseline...\n")
        removed_files = []
        removed_bytes = 0
        for d in watched:
            base_d = baseline / d.lstrip("/")
            for rel in list(collect_files(base_d, [], d)):
                if is_excluded(rel, new_excludes, d):
                    f = base_d / rel
                    removed_bytes += f.stat().st_size if f.exists() else 0
                    removed_files.append(d.rstrip("/") + "/" + rel)
                    f.unlink(missing_ok=True)
        total_files = sum(len(collect_files(baseline / d.lstrip("/"), excludes, d))
                          for d in watched)
        total_bytes = sum(
            (baseline / d.lstrip("/") / rel).stat().st_size
            for d in watched
            for rel in collect_files(baseline / d.lstrip("/"), excludes, d)
            if (baseline / d.lstrip("/") / rel).exists())
        if removed_files:
            print(f"  🗑️   Removed ({len(removed_files)} files,"
                  f" {removed_bytes/1024/1024:.1f} MB):")
            for f in removed_files:
                print(f"    {f}")
        else:
            print("  ✅  No files match the new pattern(s).")
        print(f"\n  📊  Baseline now: {total_files} files,"
              f" {total_bytes/1024/1024:.1f} MB")
        existing_meta["exclude_patterns"] = excludes
        existing_meta.pop("pending_sync", None)
        write_baseline_meta(paths, existing_meta)
        print(f"\n✅  Exclude patterns updated in baseline '{baseline_name}'.")
        return

    # ── Removed-exclude mode (need SSH to fetch newly un-excluded files) ──────
    if removed_excludes and not args.force and baseline.exists():
        host = resolve_source(cfg, args.host)
        print(f"\n♻️   Removed exclude pattern(s): {removed_excludes}")
        print(f"    Need to fetch previously excluded files from device.\n")
        try:
            with ssh_master(cfg, host) as sock:
                for d in watched:
                    rsync_pull(cfg, host, d, baseline / d.lstrip("/"),
                               excludes, sock)
            total_files = sum(len(collect_files(baseline / d.lstrip("/"), excludes, d))
                              for d in watched)
            total_bytes = sum(
                (baseline / d.lstrip("/") / rel).stat().st_size
                for d in watched
                for rel in collect_files(baseline / d.lstrip("/"), excludes, d)
                if (baseline / d.lstrip("/") / rel).exists())
            print(f"\n  📊  Baseline now: {total_files} files,"
                  f" {total_bytes/1024/1024:.1f} MB")
            existing_meta["exclude_patterns"] = excludes
            pending = [p for p in existing_meta.get("pending_sync", [])
                       if not any(ex in p for ex in removed_excludes)]
            if pending:
                existing_meta["pending_sync"] = pending
            else:
                existing_meta.pop("pending_sync", None)
            write_baseline_meta(paths, existing_meta)
            print(f"\n✅  Baseline updated with previously excluded files.")
        except Exception as exc:
            print(f"\n⚠️   Could not connect: {exc}")
            add_pending_sync(paths,
                f"Re-fetch files after removing excludes: {removed_excludes}")
            print(f"⚠️   Pending sync recorded.")
            print(f"    delta snapshot --baseline {baseline_name} -f")
        return

    # ── Full snapshot ─────────────────────────────────────────────────────────
    baseline.mkdir(parents=True, exist_ok=True)
    snap_cmds  = cfg.get("snapshot_commands", [])
    meta_extra: dict = {}

    # If excludes changed in any way — re-fetch all watched dirs.
    # We cannot determine programmatically whether a regex change is more or
    # less restrictive, so we always re-fetch and let the new patterns filter.
    old_excludes     = existing_meta.get("exclude_patterns", [])
    excludes_changed = set(excludes) != set(old_excludes)

    if excludes_changed and not args.force:
        dirs_to_fetch = list(watched)
    else:
        dirs_to_fetch = [d for d in watched
                         if args.force or not (baseline / d.lstrip("/")).exists()]
    needs_ssh = bool(dirs_to_fetch)

    # Only resolve (and require) source host when SSH is actually needed
    host = resolve_source(cfg, args.host) if needs_ssh else (
        args.host or cfg.get("source_host") or cfg.get("default_host") or "local"
    )
    print(f"\n📸  Snapshot  →  baseline '{baseline_name}'")
    if needs_ssh:
        print(f"    Source      : {host}")
    print(f"    Directories : {watched}")
    if excludes:
        print(f"    Excludes    : {excludes}")

    print()
    for d in watched:
        dest = baseline / d.lstrip("/")
        if dest.exists() and not args.force:
            print(f"  ↩️   {d} — already in baseline  (-f to refresh)")

    owners: dict = {}

    if needs_ssh:
        with ssh_master(cfg, host) as sock:
            if snap_cmds:
                print(f"\n  ⚡  Running {len(snap_cmds)} snapshot command(s)...")
                for entry in snap_cmds:
                    if isinstance(entry, dict):
                        cmd_str, key = entry["cmd"], entry.get("key")
                    else:
                        cmd_str, key = entry, None
                    print(f"  ⚡  [snapshot] {cmd_str}")
                    output = ssh_capture(cfg, host, cmd_str, sock)
                    print(f"      → {output}" if output else "      → (no output)")
                    if key:
                        meta_extra[key] = output

            for d in dirs_to_fetch:
                dest = baseline / d.lstrip("/")
                if args.force and dest.exists():
                    print(f"  🔄  {d} — refreshing (-f)")
                rsync_pull(cfg, host, d, dest, excludes, sock)

            # Fetch ownership for all files in baseline
            print(f"\n  🔑  Fetching ownership from {host}...")
            all_remote = [d.rstrip("/") + "/" + rel
                          for d in watched
                          for rel in collect_files(baseline / d.lstrip("/"), excludes, d)]
            owners = fetch_owners(cfg, host, all_remote, sock) if all_remote else {}
    else:
        owners = {}  # no fetch — ownership reused from existing meta below

    if needs_ssh:
        default_chown, owner_overrides = compute_chown(owners)
        if default_chown:
            print(f"  🔑  default_chown={default_chown!r}"
                  f"  overrides={len(owner_overrides)}")
        elif owners == {} and dirs_to_fetch:
            print("  ⚠️   stat unavailable — no ownership info stored")
    else:
        default_chown  = existing_meta.get("default_chown",  "")
        owner_overrides = existing_meta.get("owner_overrides", {})

    # Stats
    total_files = sum(len(collect_files(baseline / d.lstrip("/"), excludes, d))
                      for d in watched)
    total_bytes = sum(
        (baseline / d.lstrip("/") / rel).stat().st_size
        for d in watched
        for rel in collect_files(baseline / d.lstrip("/"), excludes, d)
        if (baseline / d.lstrip("/") / rel).exists())
    print(f"\n  📊  Baseline: {total_files} files,"
          f" {total_bytes/1024/1024:.1f} MB")

    # Preserve display_format if it existed
    display_fmt = existing_meta.get("display_format")
    meta: dict = {
        "host":             host,
        "baseline":         baseline_name,
        "time":             datetime.now().isoformat(),
        "watched_dirs":     watched,
        "exclude_patterns": excludes,
        "default_chown":    default_chown,
        "owner_overrides":  owner_overrides,
        **meta_extra,
    }
    if display_fmt:
        meta["display_format"] = display_fmt

    write_baseline_meta(paths, meta)
    if meta_extra:
        print(f"\n  📋  Stored in meta: {meta_extra}")
    print(f"\n✅  Baseline '{baseline_name}': {paths['baseline']}")


# ── diff ──────────────────────────────────────────────────────────────────────

def cmd_diff(args, paths: dict, cfg: dict) -> None:
    """
    Fetch current device state, compare against baseline, generate patch.

    Reads watched_dirs and exclude_patterns from baseline meta — not from config.
    Ownership of changed files is fetched via stat.
    If stat fails and default_chown is not set in config, diff aborts.

    All file lists live in manifest.json.
    """
    if not paths["baseline"].exists():
        bname = paths["baseline"].name
        print(f"❌  Baseline '{bname}' not found.")
        print(f"    delta snapshot --baseline {bname}")
        sys.exit(1)

    host       = resolve_source(cfg, args.host)
    patch_name = paths["patch"].name

    # Read watched_dirs and excludes from baseline meta
    bl_meta  = read_baseline_meta(paths)
    if not bl_meta:
        print("❌  Baseline has no meta.json. Re-run snapshot.")
        sys.exit(1)
    watched  = bl_meta.get("watched_dirs",     cfg["watched_dirs"])
    excludes = bl_meta.get("exclude_patterns", cfg["exclude_patterns"])

    warn_pending_sync(paths, skip_confirm=getattr(args, "yes", False))

    current = Path(args.dir) if args.dir \
        else Path(cfg["current_dir"]) if cfg.get("current_dir") \
        else paths["current"]

    def _do_diff(sock: str | None) -> None:
        print("\n🔎  Comparing files...\n")

        added:    list = []
        removed:  list = []
        modified: list = []

        for d in watched:
            bl_root   = paths["baseline"] / d.lstrip("/")
            curr_root = current / d.lstrip("/")
            prefix    = d.rstrip("/") + "/"
            bl_files  = collect_files(bl_root,   excludes, d)
            cr_files  = collect_files(curr_root, excludes, d)
            for rel, h in cr_files.items():
                entry = (prefix + rel, d, rel)
                if rel not in bl_files:    added.append(entry)
                elif bl_files[rel] != h:   modified.append(entry)
            for rel in bl_files:
                if rel not in cr_files:
                    removed.append((prefix + rel, d, rel))

        has_cmds = bool(cfg.get("pre_commands") or cfg.get("post_commands"))

        if not added and not removed and not modified:
            if has_cmds:
                print("✅  No file changes — commands-only patch.")
            else:
                print("✅  No changes found.")
                return

        def _section(tag: str, label: str, items: list,
                     show_diff: bool = False) -> None:
            print(f"{tag}  {label} ({len(items)}):")
            for full, *_ in items:
                print(f"    {full}")
            if show_diff:
                for full, d, rel in items:
                    b = paths["baseline"] / d.lstrip("/") / rel
                    c = current / d.lstrip("/") / rel
                    print(f"\n  ── diff: {full}")
                    print_diff(b, c, f"baseline:{full}", f"current:{full}")
                    print()

        _section("➕",    "Added",    added)
        _section("\n✏️ ", "Modified", modified, show_diff=args.show)
        _section("\n🗑️ ", "Removed",  removed)

        if args.dry_run:
            print("\n🔎  Dry run — patch not created.")
            return

        # Fetch ownership of changed files
        files_to_pack = added + modified
        remote_paths  = [x[0] for x in files_to_pack]
        default_chown = ""
        owner_overrides: dict = {}
        cfg_chown = cfg.get("default_chown", "")

        if files_to_pack:
            if sock is not None:
                print(f"\n  🔑  Fetching ownership from {host}...")
                owners = fetch_owners(cfg, host, remote_paths, sock)
                if owners:
                    default_chown, owner_overrides = compute_chown(owners)
                    print(f"  🔑  default_chown={default_chown!r}"
                          f"  overrides={len(owner_overrides)}")
                elif cfg_chown:
                    print(f"  ⚠️   stat unavailable — using config"
                          f" default_chown={cfg_chown!r}")
                    default_chown = cfg_chown
                else:
                    print("\n❌  Cannot fetch ownership and default_chown not set.")
                    print("    delta config --set-default-chown root:root")
                    sys.exit(1)
            else:
                # --skip-fetch mode
                if cfg_chown:
                    print(f"  ℹ️   --skip-fetch: using config"
                          f" default_chown={cfg_chown!r}")
                    default_chown = cfg_chown
                else:
                    print("\n❌  --skip-fetch: cannot fetch ownership and"
                          " default_chown not set.")
                    print("    delta config --set-default-chown root:root")
                    sys.exit(1)

        # Build patch
        print(f"\n📦  Building patch '{patch_name}'...")
        patch = paths["patch"]
        for f in ["changes.tar.gz", "manifest.json"]:
            (patch / f).unlink(missing_ok=True)
        patch.mkdir(parents=True, exist_ok=True)

        patch_tar = patch / "changes.tar.gz"
        if files_to_pack:
            pack_locally(files_to_pack, current, patch_tar)
            print(f"  ✅  {len(files_to_pack)} file(s) packed")

        manifest = {
            "created":         datetime.now().isoformat(),
            "patch_name":      patch_name,
            "baseline_name":   paths["baseline"].name,
            "source_host":     host,
            "watched_dirs":    watched,
            "default_chown":   default_chown,
            "added":           make_file_entries([x[0] for x in added],
                                                 default_chown, owner_overrides),
            "modified":        make_file_entries([x[0] for x in modified],
                                                 default_chown, owner_overrides),
            "removed":         [{"path": x[0]} for x in removed],
            "pre_commands":    cfg.get("pre_commands",  []),
            "post_commands":   cfg.get("post_commands", []),
            "target_hosts":    cfg.get("target_hosts",  []),
            "remote_tmp_path": cfg.get("remote_tmp_path",
                                       "/tmp/_delta_patch.tar.gz"),
        }
        (patch / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False))

        n_del   = len(removed)
        p_flag  = f" --patch {patch_name}" if patch_name != DEFAULT_PATCH_NAME else ""
        print(f"\n✅  Patch '{patch_name}': {patch}")
        print(f"    changes.tar.gz  — {len(files_to_pack)} file(s)")
        print(f"    manifest.json   — {n_del} deletion(s) + ownership")
        print(f"\n    deploy: delta apply{p_flag}")
        print(f"    export: delta pack{p_flag}")

    if not args.skip_fetch:
        print(f"\n🔍  Fetching from {host} → {current}")
        print(f"    Directories: {watched}\n")
        if current.exists():
            shutil.rmtree(current)
        current.mkdir(parents=True)
        with ssh_master(cfg, host) as sock:
            for d in watched:
                rsync_pull(cfg, host, d, current / d.lstrip("/"), excludes, sock)
            _do_diff(sock)
    else:
        if not current.exists():
            has_cmds = bool(cfg.get("pre_commands") or cfg.get("post_commands"))
            if has_cmds:
                print(f"\n📋  --skip-fetch + no current dir — commands-only patch")
                _do_diff(sock=None)
            else:
                print(f"❌  --skip-fetch: {current} does not exist.")
                sys.exit(1)
        else:
            print(f"\n🔍  Using already-fetched files from {current}")
            _do_diff(sock=None)


# ── diff-commands ─────────────────────────────────────────────────────────────

def cmd_diff_commands(args, paths: dict, cfg: dict) -> None:
    """Update commands, targets, and default_chown in manifest from current config."""
    mf    = paths["patch"] / "manifest.json"
    pname = paths["patch"].name
    if not mf.exists():
        print(f"❌  No patch '{pname}'. Run: delta diff --patch {pname}")
        sys.exit(1)
    m = json.loads(mf.read_text())
    m["pre_commands"]    = cfg.get("pre_commands",  [])
    m["post_commands"]   = cfg.get("post_commands", [])
    m["target_hosts"]    = cfg.get("target_hosts",  [])
    m["remote_tmp_path"] = cfg.get("remote_tmp_path", "/tmp/_delta_patch.tar.gz")
    m["default_chown"]   = cfg.get("default_chown", "")
    mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))
    print(f"✅  Manifest updated for patch '{pname}':")
    print(f"    pre_commands  : {m['pre_commands']}")
    print(f"    post_commands : {m['post_commands']}")
    print(f"    target_hosts  : {m['target_hosts']}")
    print(f"    default_chown : {m['default_chown']}")


# ── patch management ──────────────────────────────────────────────────────────

def cmd_patch(args, paths: dict, cfg: dict) -> None:
    """List patches or manage patch contents."""
    sub = getattr(args, "patch_cmd", None)

    if sub is None:
        _list_patches(paths, cfg, verbose=getattr(args, "verbose", False))
        return

    if sub == "copy":
        src = paths["patches_root"] / args.source
        dst = paths["patches_root"] / args.dest
        if not src.exists():
            print(f"❌  Patch '{args.source}' not found.")
            sys.exit(1)
        if dst.exists():
            print(f"❌  Patch '{args.dest}' already exists.")
            sys.exit(1)
        shutil.copytree(src, dst, ignore=shutil.ignore_patterns("current"))
        mf = dst / "manifest.json"
        if mf.exists():
            m = json.loads(mf.read_text())
            m["patch_name"] = args.dest
            m["created"]    = datetime.now().isoformat()
            mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))
        print(f"✅  Patch copied: {args.source} → {args.dest}")
        return

    if sub == "rename":
        src = paths["patches_root"] / args.old_name
        dst = paths["patches_root"] / args.new_name
        if not src.exists():
            print(f"❌  Patch '{args.old_name}' not found.")
            sys.exit(1)
        if dst.exists():
            print(f"❌  Patch '{args.new_name}' already exists.")
            sys.exit(1)
        src.rename(dst)
        mf = dst / "manifest.json"
        if mf.exists():
            m = json.loads(mf.read_text())
            m["patch_name"] = args.new_name
            mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))
        print(f"✅  Patch renamed: {args.old_name} → {args.new_name}")
        return

    if sub == "delete":
        target = paths["patches_root"] / args.name
        if not target.exists():
            print(f"❌  Patch '{args.name}' not found.")
            sys.exit(1)
        if not getattr(args, "yes", False):
            answer = input(f"Delete patch '{args.name}'? [y/N] ").strip().lower()
            if answer != "y":
                print("Cancelled.")
                return
        shutil.rmtree(target)
        print(f"🗑️   Patch '{args.name}' deleted.")
        return

    if sub == "set-format":
        mf = paths["patch"] / "manifest.json"
        pname = paths["patch"].name
        if not mf.exists():
            print(f"❌  No patch '{pname}'.")
            sys.exit(1)
        m = json.loads(mf.read_text())
        m["display_format"] = args.format
        mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))
        print(f"✅  display_format set for patch '{pname}'.")
        return

    # add-file, remove-file — require patch to exist
    pname = paths["patch"].name
    mf    = paths["patch"] / "manifest.json"
    if not mf.exists():
        print(f"❌  No patch '{pname}'. Run: delta diff")
        sys.exit(1)
    m         = json.loads(mf.read_text())
    patch_tar = paths["patch"] / "changes.tar.gz"

    if sub == "add-file":
        local_path  = Path(args.local_path)
        remote_path = args.remote_path
        full        = remote_path if remote_path.startswith("/") else "/" + remote_path
        chown_arg   = getattr(args, "chown", None)

        if not local_path.exists():
            print(f"❌  Local file not found: {local_path}")
            sys.exit(1)
        if not chown_arg and not m.get("default_chown"):
            print("⚠️   No --chown and manifest default_chown is empty.")
            print("    File will be extracted without chown.")

        # Rebuild tar, preserve existing chown if not overridden
        existing_entry = next((e for lst in (m["added"], m["modified"])
                                for e in lst if entry_path(e) == full), None)
        tmp = paths["patch"] / "changes_tmp.tar.gz"
        with tarfile.open(tmp, "w:gz") as new_tar:
            if patch_tar.exists():
                with tarfile.open(patch_tar, "r:gz") as old_tar:
                    for member in old_tar.getmembers():
                        if member.name != full.lstrip("/"):
                            new_tar.addfile(member, old_tar.extractfile(member))
            new_tar.add(local_path, arcname=full.lstrip("/"))
        tmp.replace(patch_tar)

        entry: dict = {"path": full}
        if chown_arg:
            entry["chown"] = chown_arg
        elif isinstance(existing_entry, dict) and "chown" in existing_entry:
            entry["chown"] = existing_entry["chown"]
            print(f"  ℹ️   Preserving chown: {entry['chown']}")

        for key in ("added", "modified"):
            m[key] = [e for e in m[key] if entry_path(e) != full]
        m["added"].append(entry)
        is_update = existing_entry is not None
        print(f"{'✏️ ' if is_update else '➕'}  {'Updated' if is_update else 'Added'}"
              f" in patch '{pname}': {full}")

    elif sub == "remove-file":
        full = args.remote_path
        if not full.startswith("/"):
            full = "/" + full
        if patch_tar.exists():
            tmp = paths["patch"] / "changes_tmp.tar.gz"
            removed_from_tar = False
            with tarfile.open(tmp, "w:gz") as new_tar:
                with tarfile.open(patch_tar, "r:gz") as old_tar:
                    for member in old_tar.getmembers():
                        if member.name == full.lstrip("/"):
                            removed_from_tar = True
                        else:
                            new_tar.addfile(member, old_tar.extractfile(member))
            if removed_from_tar:
                tmp.replace(patch_tar)
                print(f"🗑️   Removed from tar: {full}")
            else:
                tmp.unlink()
                print(f"ℹ️   File not found in tar: {full}")
        for key in ("added", "modified"):
            before = len(m[key])
            m[key] = [e for e in m[key] if entry_path(e) != full]
            if len(m[key]) < before:
                print(f"🗑️   Removed from manifest[{key!r}]: {full}")

    mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))


def _list_patches(paths: dict, cfg: dict, verbose: bool = False) -> None:
    root    = paths["patches_root"]
    default = cfg.get("default_patch", DEFAULT_PATCH_NAME) or None
    if not root.exists() or not any(
            p for p in root.iterdir()
            if p.is_dir() and (p / "manifest.json").exists()):
        print("  No patches found.")
        return
    print(f"\n📦  Patches in {root}:\n")
    for p in sorted(root.iterdir()):
        if not p.is_dir() or not (p / "manifest.json").exists():
            continue
        m      = json.loads((p / "manifest.json").read_text())
        ts     = m.get("created", "?")[:19]
        is_def = default and p.name == default
        custom = ("  " + render_format(m["display_format"], m)
                  if m.get("display_format") else "")
        flag   = "  ← default" if is_def else ""
        print(f"  {p.name:<20}  {ts}{custom}{flag}")
        if verbose:
            print(f"    source   : {m.get('source_host', '?')}")
            print(f"    baseline : {m.get('baseline_name', '?')}")
            a = len(m.get("added", [])); mo = len(m.get("modified", []))
            r = len(m.get("removed", []))
            print(f"    changes  : +{a} ~{mo} -{r}")
            print(f"    chown    : {m.get('default_chown', '—') or '—'}")
            if m.get("target_hosts"):
                print(f"    targets  : {m['target_hosts']}")
            if m.get("pre_commands"):
                print(f"    pre-cmds : {m['pre_commands']}")
            if m.get("post_commands"):
                print(f"    post-cmds: {m['post_commands']}")
            print()


# ── baseline management ───────────────────────────────────────────────────────

def cmd_baseline(args, paths: dict, cfg: dict) -> None:
    """List baselines or manage baseline metadata."""
    sub = getattr(args, "baseline_cmd", None)

    if sub is None:
        _list_baselines(paths, cfg, verbose=getattr(args, "verbose", False))
        return

    if sub == "rename":
        src = paths["baselines_root"] / args.old_name
        dst = paths["baselines_root"] / args.new_name
        if not src.exists():
            print(f"❌  Baseline '{args.old_name}' not found.")
            sys.exit(1)
        if dst.exists():
            print(f"❌  Baseline '{args.new_name}' already exists.")
            sys.exit(1)
        src.rename(dst)
        mf = dst / "meta.json"
        if mf.exists():
            m = json.loads(mf.read_text())
            m["baseline"] = args.new_name
            mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))
        print(f"✅  Baseline renamed: {args.old_name} → {args.new_name}")
        return

    if sub == "delete":
        target = paths["baselines_root"] / args.name
        if not target.exists():
            print(f"❌  Baseline '{args.name}' not found.")
            sys.exit(1)
        if not getattr(args, "yes", False):
            answer = input(f"Delete baseline '{args.name}'? [y/N] ").strip().lower()
            if answer != "y":
                print("Cancelled.")
                return
        shutil.rmtree(target)
        print(f"🗑️   Baseline '{args.name}' deleted.")
        return

    if sub == "set-format":
        mf    = paths["baseline_meta"]
        bname = paths["baseline"].name
        if not mf.exists():
            print(f"❌  No baseline '{bname}' or missing meta.json.")
            sys.exit(1)
        m = json.loads(mf.read_text())
        m["display_format"] = args.format
        mf.write_text(json.dumps(m, indent=2, ensure_ascii=False))
        print(f"✅  display_format set for baseline '{bname}'.")
        return


def _list_baselines(paths: dict, cfg: dict, verbose: bool = False) -> None:
    root    = paths["baselines_root"]
    default = cfg.get("default_baseline", DEFAULT_BASELINE) or None
    if not root.exists() or not any(b for b in root.iterdir() if b.is_dir()):
        print("  No baselines found.")
        return
    print(f"\n🗂️   Baselines in {root}:\n")
    for b in sorted(root.iterdir()):
        if not b.is_dir():
            continue
        mf     = b / "meta.json"
        is_def = default and b.name == default
        flag   = "  ← default" if is_def else ""
        if mf.exists():
            m       = json.loads(mf.read_text())
            ts      = m.get("time", "?")[:19]
            custom  = ("  " + render_format(m["display_format"], m)
                       if m.get("display_format") else "")
            pending = "  ⚠️  pending sync" if m.get("pending_sync") else ""
            print(f"  {b.name:<20}  {ts}{custom}{pending}{flag}")
            if verbose:
                print(f"    host    : {m.get('host', '?')}")
                print(f"    dirs    : {m.get('watched_dirs', m.get('dirs', []))}")
                print(f"    chown   : {m.get('default_chown', '—') or '—'}")
                exc = m.get("exclude_patterns", [])
                if exc:
                    print(f"    excludes: {exc}")
                std = {"host","dirs","baseline","time","watched_dirs",
                       "exclude_patterns","default_chown","owner_overrides",
                       "display_format","pending_sync"}
                for k, v in m.items():
                    if k not in std:
                        print(f"    {k:<10}: {v}")
                print()
        else:
            print(f"  {b.name:<20}  (no meta.json){flag}")


# ── apply ─────────────────────────────────────────────────────────────────────

def _apply_to_host(cfg: dict, host: str, patch: Path,
                   sock:      str | None = None,
                   skip_pre:  bool = False,
                   skip_files:bool = False,
                   skip_post: bool = False,
                   var_map:   dict | None = None,
                   is_rollback: bool = False) -> bool:
    mf    = patch / "manifest.json"
    ptar  = patch / "changes.tar.gz"
    label = "rollback" if is_rollback else "patch"
    vmap  = var_map or {}

    if not mf.exists():
        print(f"❌  manifest.json not found: {patch}")
        return False

    m             = json.loads(mf.read_text())
    added_entries = m.get("added",    [])
    mod_entries   = m.get("modified", [])
    rem_entries   = m.get("removed",  [])
    added_mod     = all_paths(added_entries + mod_entries)
    all_del       = [entry_path(e) for e in rem_entries]
    default_chown = m.get("default_chown", "")
    pre_cmds      = [substitute_vars(c, vmap) for c in m.get("pre_commands",  [])]
    post_cmds     = [substitute_vars(c, vmap) for c in m.get("post_commands", [])]

    print(f"\n── {host} {'─'*40}")
    print(f"   {label}: +/~ {len(added_mod)} files,  🗑 {len(all_del)} deletions")
    if vmap:
        print(f"   vars: {vmap}")

    ssh  = _with_sock(_ssh_opts(cfg), sock)
    scp  = _with_sock(_scp_opts(cfg), sock)
    user = cfg["ssh_user"]
    ok   = True

    def _remote(cmd_str: str, stage: str) -> bool:
        print(f"  ⚡  [{stage}] {cmd_str}")
        try:
            run_cmd(["ssh", *ssh, f"{user}@{host}", cmd_str])
            return True
        except subprocess.CalledProcessError:
            print(f"  ❌  Failed: {cmd_str}")
            return False

    # Stage 1: Pre-commands
    stage_header(1, 3, "Pre-commands")
    if skip_pre:
        print("  ⏭️   Skipped")
    elif not pre_cmds:
        print("  ✔   Nothing to run")
    else:
        for c in pre_cmds:
            if not _remote(c, "pre"):
                print(f"  ⛔  Aborting for {host}")
                return False

    # Stage 2: File transfer
    stage_header(2, 3, "File transfer")
    if skip_files:
        print("  ⏭️   Skipped")
    else:
        remote_tmp = m.get("remote_tmp_path", "/tmp/_delta_patch.tar.gz")
        if not added_mod and not all_del:
            print("  ✔   Nothing to transfer")
        else:
            if ptar.exists() and added_mod:
                try:
                    run_cmd(["scp", *scp, str(ptar), f"{user}@{host}:{remote_tmp}"])
                    run_cmd(["ssh", *ssh, f"{user}@{host}",
                             f"tar -xzf {remote_tmp} -C / && rm -f {remote_tmp}"])
                except subprocess.CalledProcessError:
                    print("  ❌  Error uploading/extracting files")
                    ok = False

            if ok and added_mod and default_chown:
                by_owner: dict = {}
                for e in (added_entries + mod_entries):
                    fp    = entry_path(e)
                    owner = entry_chown(e, default_chown)
                    by_owner.setdefault(owner, []).append(fp)
                for owner, fpaths in by_owner.items():
                    files_str = " ".join(f'"{p}"' for p in fpaths)
                    print(f"  🔑  chown {owner}  ({len(fpaths)} file(s))")
                    try:
                        run_cmd(["ssh", *ssh, f"{user}@{host}",
                                 f"chown {owner} {files_str}"])
                    except subprocess.CalledProcessError:
                        print("  ⚠️   chown failed (non-fatal)")

            if all_del and ok:
                files_str = " ".join(f'"{p}"' for p in all_del)
                try:
                    run_cmd(["ssh", *ssh, f"{user}@{host}", f"rm -rf {files_str}"])
                except subprocess.CalledProcessError:
                    print("  ❌  Error removing files")
                    ok = False

    # Stage 3: Post-commands
    stage_header(3, 3, "Post-commands")
    if skip_post:
        print("  ⏭️   Skipped")
    elif not post_cmds:
        print("  ✔   Nothing to run")
    elif not ok:
        print("  ⏭️   Skipped (previous stage failed)")
    else:
        for c in post_cmds:
            if not _remote(c, "post"):
                print(f"  ⛔  Post-command failed on {host}")
                ok = False
                break

    print()
    if ok:
        print(f"  ✅  {host} — done")
    return ok


def cmd_apply(args, paths: dict, cfg: dict) -> None:
    """Apply current patch to one or more devices."""
    patch = paths["patch"]
    pname = patch.name
    mf    = patch / "manifest.json"
    if not mf.exists():
        p_flag = f" --patch {pname}" if pname != DEFAULT_PATCH_NAME else ""
        print(f"❌  No patch '{pname}'. Run: delta diff{p_flag}")
        sys.exit(1)

    m = json.loads(mf.read_text())

    # CLI SSH overrides
    if args.ssh_user: cfg["ssh_user"] = args.ssh_user
    if args.ssh_port: cfg["ssh_port"] = int(args.ssh_port)
    if args.ssh_key:  cfg["ssh_key"]  = str(Path(args.ssh_key).expanduser())

    # Command consistency check
    if not getattr(args, "skip_config_check", False):
        errs = check_cmd_consistency(cfg, m)
        if errs:
            print(f"\n❌  Manifest commands don't satisfy config requirements:")
            for e in errs:
                print(e)
            p_flag = f" --patch {pname}" if pname != DEFAULT_PATCH_NAME else ""
            print(f"    Fix : delta diff-commands{p_flag}")
            print(f"    Skip: delta apply --skip-config-check")
            sys.exit(1)

    hosts = resolve_targets(cfg, args.hosts, m)

    # Variable substitution validation (before any SSH)
    var_map: dict = {}
    for item in (args.var or []):
        if "=" not in item:
            print(f"❌  Invalid --var: '{item}'  (expected KEY=VALUE)")
            sys.exit(1)
        k, v = item.split("=", 1)
        var_map[k.strip()] = v.strip()

    all_cmds = (
        ([] if args.skip_pre  else m.get("pre_commands",  [])) +
        ([] if args.skip_post else m.get("post_commands", []))
    )
    errs = validate_vars(all_cmds, var_map)
    if errs:
        print("\n❌  Missing required variables:")
        for e in errs: print(e)
        print("\n    delta apply --var KEY=VALUE")
        sys.exit(1)

    if var_map:
        print(f"  🔑  Vars: { {k: v or '(empty)' for k, v in var_map.items()} }")

    # Dry run — no SSH
    if args.dry_run:
        print(f"\n🔎  Dry run for patch '{pname}':\n")
        added_mod = all_paths(m.get("added", []) + m.get("modified", []))
        rem       = [entry_path(e) for e in m.get("removed", [])]
        pre       = [substitute_vars(c, var_map) for c in m.get("pre_commands",  [])]
        post      = [substitute_vars(c, var_map) for c in m.get("post_commands", [])]
        dc        = m.get("default_chown", "")
        for host in hosts:
            print(f"── {host} {'─'*40}")
            stage_header(1, 3, "Pre-commands")
            if not pre: print("  ✔   Nothing to run")
            for c in pre: print(f"  ⚡  [pre] {c}")
            stage_header(2, 3, "File transfer")
            if not added_mod and not rem:
                print("  ✔   Nothing to transfer")
            if added_mod:
                print(f"  📤  Would upload/extract {len(added_mod)} file(s)")
            if dc and added_mod:
                by: dict = {}
                for e in m.get("added", []) + m.get("modified", []):
                    by.setdefault(entry_chown(e, dc), []).append(entry_path(e))
                for owner, fps in by.items():
                    print(f"  🔑  Would chown {owner} ({len(fps)} file(s))")
            if rem:
                print(f"  🗑️   Would delete {len(rem)} file(s)")
            stage_header(3, 3, "Post-commands")
            if not post: print("  ✔   Nothing to run")
            for c in post: print(f"  ⚡  [post] {c}")
            print()
        print(f"{'═'*60}")
        print(f"🔎  Dry run — {len(hosts)} device(s) would be updated")
        return

    skipped = [n for f, n in [(args.skip_pre, "pre"), (args.skip_files, "files"),
                               (args.skip_post, "post")] if f]
    if skipped:
        print(f"  ⏭️   Skipping: {', '.join(skipped)}")

    print(f"\n🚀  Applying patch '{pname}' to {len(hosts)} device(s)...")
    results = {}
    for host in hosts:
        with ssh_master(cfg, host) as sock:
            results[host] = _apply_to_host(
                cfg, host, patch, sock=sock,
                skip_pre=args.skip_pre, skip_files=args.skip_files,
                skip_post=args.skip_post, var_map=var_map)
    print_summary(results)


# ── rollback ──────────────────────────────────────────────────────────────────

def cmd_rollback(args, paths: dict, cfg: dict) -> None:
    """Revert device(s) to the named baseline state."""
    baseline = paths["baseline"]
    bname    = baseline.name
    if not baseline.exists():
        print(f"❌  Baseline '{bname}' not found.")
        sys.exit(1)

    bl_meta = read_baseline_meta(paths)
    if not bl_meta:
        print(f"❌  Baseline '{bname}' has no meta.json.")
        print(f"    Re-run: delta snapshot --baseline {bname} -f")
        sys.exit(1)

    bl_default_chown   = bl_meta.get("default_chown",   "")
    bl_owner_overrides = bl_meta.get("owner_overrides",  {})
    cfg_chown          = cfg.get("default_chown", "")

    if not bl_default_chown:
        if cfg_chown:
            print(f"  ⚠️   Baseline has no ownership info — using config"
                  f" default_chown={cfg_chown!r}")
            bl_default_chown = cfg_chown
        else:
            print(f"❌  Baseline '{bname}' has no ownership info and"
                  f" default_chown is not set.")
            print(f"    Re-run: delta snapshot --baseline {bname} -f")
            print(f"    Or set: delta config --set-default-chown root:root")
            sys.exit(1)

    warn_pending_sync(paths, skip_confirm=getattr(args, "yes", False))

    host     = resolve_source(cfg, args.host)
    watched  = bl_meta.get("watched_dirs",     cfg["watched_dirs"])
    excludes = bl_meta.get("exclude_patterns", cfg["exclude_patterns"])

    print(f"\n⏪  Rollback to '{bname}': fetching current state from {host}...")

    current = paths["tmp"] / "rollback_current"
    if current.exists():
        shutil.rmtree(current)
    current.mkdir(parents=True)

    with ssh_master(cfg, host) as sock:
        for d in watched:
            rsync_pull(cfg, host, d, current / d.lstrip("/"), excludes, sock)

    rb_added:    list = []
    rb_modified: list = []
    rb_removed:  list = []

    for d in watched:
        bl_root   = baseline / d.lstrip("/")
        curr_root = current  / d.lstrip("/")
        prefix    = d.rstrip("/") + "/"
        bl_files  = collect_files(bl_root,   excludes, d)
        cr_files  = collect_files(curr_root, excludes, d)
        for rel, h in bl_files.items():
            full = prefix + rel
            if rel not in cr_files:      rb_added.append((full, d, rel))
            elif cr_files[rel] != h:     rb_modified.append((full, d, rel))
        for rel in cr_files:
            if rel not in bl_files:
                rb_removed.append((prefix + rel, d, rel))

    if not rb_added and not rb_modified and not rb_removed:
        print("✅  Device already at baseline — nothing to roll back.")
        return

    print(f"\n  Changes to revert:")
    if rb_added:    print(f"    🔁  Restore deleted  ({len(rb_added)})")
    if rb_modified: print(f"    🔁  Restore modified ({len(rb_modified)})")
    if rb_removed:  print(f"    🗑️   Remove new       ({len(rb_removed)})")

    tmp_patch = paths["tmp"] / "rollback_patch"
    if tmp_patch.exists():
        shutil.rmtree(tmp_patch)
    tmp_patch.mkdir(parents=True)

    files_to_restore = rb_added + rb_modified
    rb_tar = tmp_patch / "changes.tar.gz"
    if files_to_restore:
        with tarfile.open(rb_tar, "w:gz") as tar:
            for full, d, rel in files_to_restore:
                local = baseline / d.lstrip("/") / rel
                if local.exists():
                    tar.add(local, arcname=full.lstrip("/"))

    (tmp_patch / "manifest.json").write_text(json.dumps({
        "created":         datetime.now().isoformat(),
        "type":            "rollback",
        "added":           make_file_entries([x[0] for x in files_to_restore],
                                             bl_default_chown, bl_owner_overrides),
        "modified":        [],
        "removed":         [{"path": x[0]} for x in rb_removed],
        "pre_commands":    [],
        "post_commands":   [],
        "remote_tmp_path": cfg.get("remote_tmp_path", "/tmp/_delta_patch.tar.gz"),
        "default_chown":   bl_default_chown,
    }, indent=2))

    hosts = resolve_targets(cfg, args.hosts)
    print(f"\n⏪  Rolling back {len(hosts)} device(s)...")
    results = {}
    try:
        for h in hosts:
            with ssh_master(cfg, h) as sock:
                results[h] = _apply_to_host(cfg, h, tmp_patch,
                                            sock=sock, is_rollback=True)
        print_summary(results)
    finally:
        shutil.rmtree(tmp_patch, ignore_errors=True)


# ── deploy ────────────────────────────────────────────────────────────────────

def cmd_deploy(args, paths: dict, cfg: dict) -> None:
    """Direct rsync from master to targets. No patch. Targets must be explicit."""
    master   = resolve_source(cfg, args.master)
    watched  = cfg["watched_dirs"]
    excludes = cfg["exclude_patterns"]
    if not watched:
        print("❌  No watched directories. delta config --add-watched-dir DIR")
        sys.exit(1)
    if not args.hosts:
        print("❌  deploy requires explicit target hosts — no fallback.")
        print("    delta deploy [master] host1 host2 ...")
        sys.exit(1)

    print(f"\n🚀  Deploy {master} → {args.hosts}\n")
    current = paths["tmp"] / "deploy_current"
    if current.exists():
        shutil.rmtree(current)
    current.mkdir(parents=True)

    with ssh_master(cfg, master) as sock:
        for d in watched:
            rsync_pull(cfg, master, d, current / d.lstrip("/"), excludes, sock)

    results = {}
    for host in args.hosts:
        print(f"\n── {host} {'─'*40}")
        ok = True
        with ssh_master(cfg, host) as sock:
            for d in watched:
                try:
                    rsync_push(cfg, current / d.lstrip("/"), host, d, excludes, sock)
                except subprocess.CalledProcessError:
                    ok = False
        results[host] = ok
        print(f"  {'✅' if ok else '❌'}  {host}")
    print_summary(results)


# ── pack ──────────────────────────────────────────────────────────────────────

def cmd_pack(args, paths: dict, _cfg: dict) -> None:
    """Pack patch into a distributable archive."""
    patch = paths["patch"]
    pname = patch.name
    if not (patch / "manifest.json").exists():
        print(f"❌  No patch '{pname}'.")
        sys.exit(1)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix   = f"_{pname}" if pname != DEFAULT_PATCH_NAME else ""
    out_path = Path(args.output) if args.output \
        else Path(f"delta_patch{suffix}_{ts}.tar.gz")
    with tarfile.open(out_path, "w:gz") as tar:
        for f in patch.iterdir():
            if f.name != "current":
                tar.add(f, arcname=f"patch/{f.name}")
        tar.add(Path(__file__).resolve(), arcname="delta.py")
    p_flag = f" --patch {pname}" if pname != DEFAULT_PATCH_NAME else ""
    print(f"\n📦  Packed: {out_path}  ({out_path.stat().st_size // 1024} KB)")
    print(f"\n    On the target PC:")
    print(f"      tar -xzf {out_path.name}")
    print(f"      python3 delta.py --work-dir . apply{p_flag}"
          f" [--ssh-user U] [--ssh-port P] [--ssh-key K]")


# ── status ────────────────────────────────────────────────────────────────────

def cmd_status(args, paths: dict, _cfg: dict) -> None:
    """Show current patch summary."""
    mf    = paths["patch"] / "manifest.json"
    pname = paths["patch"].name
    if not mf.exists():
        print(f"❌  No patch '{pname}'.")
        return
    m  = json.loads(mf.read_text())
    dc = m.get("default_chown", "") or "—"
    print(f"\n📦  Patch '{pname}'")
    print(f"    Created        : {m['created']}")
    print(f"    Baseline       : {m.get('baseline_name', '—')}")
    print(f"    Source host    : {m.get('source_host',   '—')}")
    print(f"    Directories    : {m.get('watched_dirs',  [])}")
    print(f"    ➕  Added       : {len(m.get('added',    []))}")
    print(f"    ✏️   Modified    : {len(m.get('modified', []))}")
    print(f"    🗑️   Removed     : {len(m.get('removed',  []))}")
    print(f"    ⚡  Pre-cmds    : {m.get('pre_commands',  [])}")
    print(f"    ⚡  Post-cmds   : {m.get('post_commands', [])}")
    print(f"    🎯  Targets     : {m.get('target_hosts',  [])}")
    print(f"    🔑  default_chown: {dc}")
    for tag, key in [("➕", "added"), ("✏️ ", "modified"), ("🗑️ ", "removed")]:
        entries = m.get(key, [])
        if not entries:
            continue
        print(f"\n  {tag}")
        for e in entries:
            path  = entry_path(e)
            owner = entry_chown(e, m.get("default_chown", "")) if key != "removed" else ""
            suffix = f"  [{owner}]" if owner else ""
            print(f"    {path}{suffix}")


# ── logs ──────────────────────────────────────────────────────────────────────

def cmd_logs(args, paths: dict, cfg: dict) -> None:
    """List or manage log files."""
    log_dir  = Path(cfg["log_dir"]) if cfg.get("log_dir") else paths["logs"]
    logs_cmd = getattr(args, "logs_cmd", None)

    if logs_cmd == "clear":
        if not log_dir.exists() or not any(log_dir.glob("*.log")):
            print("✅  No log files — nothing to delete.")
            return
        logs = list(log_dir.glob("*.log"))
        if not getattr(args, "yes", False):
            answer = input(f"Delete {len(logs)} log file(s)? [y/N] ").strip().lower()
            if answer != "y":
                print("Cancelled.")
                return
        shutil.rmtree(log_dir)
        print(f"🗑️   Deleted {len(logs)} log file(s) from {log_dir}")
        return

    # Default: list
    if not log_dir.exists() or not any(log_dir.glob("*.log")):
        print(f"  No log files in {log_dir}")
        return
    for lg in sorted(log_dir.glob("*.log"), reverse=True):
        size_kb = lg.stat().st_size // 1024
        mtime   = datetime.fromtimestamp(lg.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(f"    {mtime}  {lg.name}  ({size_kb} KB)")


# ── config ────────────────────────────────────────────────────────────────────

def cmd_config(args, paths: dict, cfg: dict) -> None:
    """View or edit config. Only this command modifies config.json."""
    changed = False

    def _set(key: str, val, transform=None) -> None:
        nonlocal changed
        if val is None:
            return
        cfg[key] = transform(val) if transform else val
        print(f"✅  {key} = {cfg[key]}")
        changed = True

    _set("default_host",     getattr(args, "set_host",             None))
    _set("source_host",      getattr(args, "set_source_host",      None))
    _set("ssh_user",         getattr(args, "set_ssh_user",         None))
    _set("ssh_port",         getattr(args, "set_ssh_port",         None), int)
    _set("ssh_key",          getattr(args, "set_ssh_key",          None),
         lambda v: str(Path(v).expanduser()))
    _set("default_patch",    getattr(args, "set_default_patch",    None))
    _set("default_baseline", getattr(args, "set_default_baseline", None))
    _set("default_chown",    getattr(args, "set_default_chown",    None))
    _set("current_dir",      getattr(args, "set_current_dir",      None))
    _set("remote_tmp_path",  getattr(args, "set_remote_tmp_path",  None))
    _set("log_enabled",      getattr(args, "log_enable",           None), lambda _: True)
    _set("log_enabled",      getattr(args, "log_disable",          None), lambda _: False)
    _set("log_dir",          getattr(args, "set_log_dir",          None))
    _set("log_filename",     getattr(args, "set_log_filename",     None))

    LIST_OPS = [
        ("add_watched_dir",     "watched_dirs",      True),
        ("remove_watched_dir",  "watched_dirs",      False),
        ("add_target_host",     "target_hosts",      True),
        ("remove_target_host",  "target_hosts",      False),
        ("add_exclude",         "exclude_patterns",  True),
        ("remove_exclude",      "exclude_patterns",  False),
        ("add_pre_command",     "pre_commands",      True),
        ("remove_pre_command",  "pre_commands",      False),
        ("add_post_command",    "post_commands",     True),
        ("remove_post_command", "post_commands",     False),
    ]
    for attr, key, is_add in LIST_OPS:
        val = getattr(args, attr, None)
        if not val:
            continue
        if is_add:
            new_vals = [v for v in val if v not in cfg[key]]
            if key == "exclude_patterns" and new_vals:
                errs = validate_patterns(new_vals)
                if errs:
                    for e in errs: print(e)
                    sys.exit(1)
            cfg[key] += new_vals
        else:
            for v in val:
                for i in range(len(cfg[key]) - 1, -1, -1):
                    if cfg[key][i] == v:
                        cfg[key].pop(i)
                        break
                else:
                    print(f"❌  Not found in {key}: {v!r}")
                    sys.exit(1)
        print(f"✅  {key}: {cfg[key]}")
        changed = True

    for entry in (getattr(args, "add_snapshot_command", None) or []):
        cfg["snapshot_commands"].append(entry)
        print(f"✅  snapshot_commands += {entry!r}")
        changed = True
    for entry in (getattr(args, "add_snapshot_command_key", None) or []):
        if "=" not in entry:
            print(f"❌  Expected KEY=command, got: {entry!r}")
            continue
        k, cmd_str = entry.split("=", 1)
        cfg["snapshot_commands"].append({"cmd": cmd_str.strip(), "key": k.strip()})
        print(f"✅  snapshot_commands += {{cmd: {cmd_str.strip()!r},"
              f" key: {k.strip()!r}}}")
        changed = True
    if getattr(args, "clear_snapshot_commands", False):
        cfg["snapshot_commands"] = []
        print("✅  snapshot_commands cleared")
        changed = True

    if changed:
        save_config(paths, cfg)
    else:
        print(f"\n📄  Config: {paths['config']}\n")
        print(json.dumps(cfg, indent=2, ensure_ascii=False))


# ══════════════════════════════════════════════════════════════════════════════
#  Argparse helpers
# ══════════════════════════════════════════════════════════════════════════════

def _add_patch_arg(p) -> None:
    p.add_argument("-p", "--patch", metavar="NAME", default=None,
                   help=f"Patch name (default from config or '{DEFAULT_PATCH_NAME}')")


def _add_baseline_arg(p) -> None:
    p.add_argument("-b", "--baseline", metavar="NAME", default=None,
                   help=f"Baseline name (default from config or '{DEFAULT_BASELINE}')")


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="delta — synchronize changes across identical devices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--work-dir", default=str(DEFAULT_WORK_DIR),
                        help=f"Working directory (default: {DEFAULT_WORK_DIR})")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ── snapshot ──────────────────────────────────────────────────────────────
    p = sub.add_parser("snapshot",
                       help="Fetch dirs from device and save as baseline")
    p.add_argument("dirs", nargs="*", metavar="DIR",
                   help="Extra directories to include (combined with config"
                        " watched_dirs; not saved to config)")
    p.add_argument("--host")
    p.add_argument("--exclude",        nargs="+", metavar="PATTERN",
                   help="Add regex exclude patterns (to baseline meta, not config)")
    p.add_argument("--remove-exclude", nargs="+", metavar="PATTERN",
                   help="Remove exclude patterns from baseline meta")
    p.add_argument("-f", "--force", action="store_true",
                   help="Re-fetch existing dirs (overwrites them)")
    _add_baseline_arg(p)

    # ── diff ──────────────────────────────────────────────────────────────────
    p = sub.add_parser("diff",
                       help="Compare device state against baseline, generate patch")
    p.add_argument("--host"); _add_patch_arg(p); _add_baseline_arg(p)
    p.add_argument("-n", "--skip-fetch", action="store_true",
                   help="Skip downloading — compare already-fetched files")
    p.add_argument("--dir", metavar="PATH",
                   help="Custom directory for fetched files")
    p.add_argument("-s", "--show", action="store_true",
                   help="Print coloured unified diff for modified files")
    p.add_argument("-d", "--dry-run", action="store_true",
                   help="Show changes but do not create a patch")
    p.add_argument("-y", "--yes", action="store_true",
                   help="Skip confirmation when baseline has pending sync")

    # ── diff-commands ─────────────────────────────────────────────────────────
    p = sub.add_parser("diff-commands",
                       help="Sync commands/targets from config into manifest"
                            " (no re-fetch)")
    _add_patch_arg(p)

    # ── patch ─────────────────────────────────────────────────────────────────
    p    = sub.add_parser("patch", help="List patches or manage patch contents")
    _add_patch_arg(p)
    p.add_argument("-v", "--verbose", action="store_true")
    psub = p.add_subparsers(dest="patch_cmd", required=False)

    pa = psub.add_parser("add-file", help="Add/replace a file in the patch")
    pa.add_argument("local_path")
    pa.add_argument("remote_path")
    pa.add_argument("--chown", metavar="USER:GROUP")

    pr = psub.add_parser("remove-file", help="Remove a file from the patch")
    pr.add_argument("remote_path")

    pcp = psub.add_parser("copy", help="Copy a patch under a new name")
    pcp.add_argument("source", help="Source patch name")
    pcp.add_argument("dest",   help="New patch name")

    prn = psub.add_parser("rename", help="Rename a patch")
    prn.add_argument("old_name", help="Current patch name")
    prn.add_argument("new_name", help="New patch name")

    pd = psub.add_parser("delete", help="Delete a patch")
    pd.add_argument("name", help="Patch name to delete")
    pd.add_argument("-y", "--yes", action="store_true")

    psf = psub.add_parser("set-format",
                           help="Set display format for patch list")
    psf.add_argument("format", metavar="FORMAT")

    # ── baseline ──────────────────────────────────────────────────────────────
    p    = sub.add_parser("baseline",
                          help="List baselines or manage baseline metadata")
    _add_baseline_arg(p)
    p.add_argument("-v", "--verbose", action="store_true")
    bsub = p.add_subparsers(dest="baseline_cmd", required=False)

    brn = bsub.add_parser("rename", help="Rename a baseline")
    brn.add_argument("old_name", help="Current baseline name")
    brn.add_argument("new_name", help="New baseline name")

    bd = bsub.add_parser("delete", help="Delete a baseline")
    bd.add_argument("name", help="Baseline name to delete")
    bd.add_argument("-y", "--yes", action="store_true")

    bsf = bsub.add_parser("set-format",
                           help="Set display format for baseline list")
    bsf.add_argument("format", metavar="FORMAT")

    # ── apply ─────────────────────────────────────────────────────────────────
    p = sub.add_parser("apply", help="Apply patch to one or more devices")
    p.add_argument("hosts", nargs="*", metavar="HOST"); _add_patch_arg(p)
    p.add_argument("--ssh-user", metavar="USER")
    p.add_argument("--ssh-port", metavar="PORT")
    p.add_argument("--ssh-key",  metavar="PATH")
    p.add_argument("--skip-pre",          action="store_true")
    p.add_argument("--skip-files",        action="store_true")
    p.add_argument("--skip-post",         action="store_true")
    p.add_argument("--skip-config-check", action="store_true",
                   help="Skip config/manifest command consistency check")
    p.add_argument("-d", "--dry-run",     action="store_true",
                   help="Show what would happen — no SSH connections made")
    p.add_argument("-e", "--var", action="append", metavar="KEY=VALUE",
                   help="Variable substitution in commands (repeatable)")

    # ── rollback ──────────────────────────────────────────────────────────────
    p = sub.add_parser("rollback", help="Revert device(s) to baseline state")
    p.add_argument("hosts", nargs="*", metavar="HOST")
    p.add_argument("--host"); _add_baseline_arg(p)
    p.add_argument("-y", "--yes", action="store_true",
                   help="Skip confirmation when baseline has pending sync")

    # ── deploy ────────────────────────────────────────────────────────────────
    p = sub.add_parser("deploy",
                       help="Direct rsync from master to targets (no patch)")
    p.add_argument("master", nargs="?", default=None)
    p.add_argument("hosts",  nargs="*", metavar="HOST")

    # ── pack ──────────────────────────────────────────────────────────────────
    p = sub.add_parser("pack", help="Pack patch into a distributable archive")
    _add_patch_arg(p)
    p.add_argument("-o", "--output", metavar="FILE")

    # ── status ────────────────────────────────────────────────────────────────
    p = sub.add_parser("status", help="Show patch summary")
    _add_patch_arg(p)

    # ── logs ──────────────────────────────────────────────────────────────────
    p    = sub.add_parser("logs", help="List or manage log files")
    lsub = p.add_subparsers(dest="logs_cmd", required=False)
    lc   = lsub.add_parser("clear", help="Delete all log files")
    lc.add_argument("-y", "--yes", action="store_true")

    # ── config ────────────────────────────────────────────────────────────────
    p = sub.add_parser("config", help="View or edit config")
    p.add_argument("--set-host",             metavar="IP",
                   help="Set default_host (fallback for source and targets)")
    p.add_argument("--set-source-host",      metavar="IP")
    p.add_argument("--set-ssh-user",         metavar="USER")
    p.add_argument("--set-ssh-port",         metavar="PORT")
    p.add_argument("--set-ssh-key",          metavar="PATH")
    p.add_argument("--set-default-patch",    metavar="NAME")
    p.add_argument("--set-default-baseline", metavar="NAME")
    p.add_argument("--set-default-chown",    metavar="USER:GROUP")
    p.add_argument("--set-current-dir",      metavar="PATH")
    p.add_argument("--set-remote-tmp-path",  metavar="PATH")
    p.add_argument("--add-watched-dir",      nargs="+", metavar="DIR")
    p.add_argument("--remove-watched-dir",   nargs="+", metavar="DIR")
    p.add_argument("--add-target-host",      nargs="+", metavar="HOST")
    p.add_argument("--remove-target-host",   nargs="+", metavar="HOST")
    p.add_argument("--add-exclude",          nargs="+", metavar="PATTERN")
    p.add_argument("--remove-exclude",       nargs="+", metavar="PATTERN")
    p.add_argument("--add-pre-command",      nargs="+", metavar="CMD")
    p.add_argument("--remove-pre-command",   nargs="+", metavar="CMD")
    p.add_argument("--add-post-command",     nargs="+", metavar="CMD")
    p.add_argument("--remove-post-command",  nargs="+", metavar="CMD")
    p.add_argument("--log-enable",   action="store_true", default=None)
    p.add_argument("--log-disable",  action="store_true", default=None)
    p.add_argument("--set-log-dir",      metavar="PATH")
    p.add_argument("--set-log-filename", metavar="PATTERN",
                   help="Log filename pattern: {cmd} {timestamp} {result}")
    p.add_argument("--add-snapshot-command",     nargs="+", metavar="CMD")
    p.add_argument("--add-snapshot-command-key", nargs="+", metavar="KEY=CMD")
    p.add_argument("--clear-snapshot-commands",  action="store_true")

    # ── Parse and resolve names ───────────────────────────────────────────────
    args = parser.parse_args()

    # Temp paths to load config
    tmp_paths = get_paths(Path(args.work_dir))
    cfg       = load_config(tmp_paths)

    given_patch    = getattr(args, "patch",    None)
    given_baseline = getattr(args, "baseline", None)

    # Commands that don't need a specific patch/baseline
    NO_PATCH_NEEDED    = {"snapshot", "deploy", "config", "status",
                          "logs", "diff-commands"}
    NO_BASELINE_NEEDED = {"deploy", "config", "pack", "diff-commands",
                          "status", "logs"}

    # Resolve patch name
    if args.cmd in NO_PATCH_NEEDED and args.cmd != "status":
        patch_name = given_patch or cfg.get("default_patch",
                                             DEFAULT_PATCH_NAME) or DEFAULT_PATCH_NAME
    else:
        patch_name = given_patch or cfg.get("default_patch", DEFAULT_PATCH_NAME) or ""
        if not patch_name and args.cmd in {"diff", "apply", "pack", "patch",
                                            "diff-commands", "status"}:
            print("❌  No --patch and default_patch is disabled in config.")
            print("    delta diff --patch NAME")
            sys.exit(1)
        patch_name = patch_name or DEFAULT_PATCH_NAME

    # Resolve baseline name
    if args.cmd in NO_BASELINE_NEEDED:
        baseline_name = given_baseline or cfg.get("default_baseline",
                                                   DEFAULT_BASELINE) or DEFAULT_BASELINE
    else:
        baseline_name = given_baseline or cfg.get("default_baseline",
                                                   DEFAULT_BASELINE) or ""
        if not baseline_name and args.cmd in {"snapshot", "diff", "rollback",
                                               "baseline"}:
            print("❌  No --baseline and default_baseline is disabled in config.")
            print("    delta snapshot --baseline NAME")
            sys.exit(1)
        baseline_name = baseline_name or DEFAULT_BASELINE

    paths = get_paths(Path(args.work_dir), patch_name, baseline_name)
    cfg   = load_config(paths)   # reload with final paths

    # Logging: skip for read-only / UI commands
    _patch_cmd    = getattr(args, "patch_cmd",    None)
    _baseline_cmd = getattr(args, "baseline_cmd", None)
    _logs_cmd     = getattr(args, "logs_cmd",     None)
    _NO_LOG = {"config", "status", "logs"}
    _read_only = (
        (args.cmd == "patch"    and _patch_cmd    in (None, "set-format")) or
        (args.cmd == "baseline" and _baseline_cmd in (None, "set-format")) or
        (args.cmd == "logs"     and _logs_cmd      in (None, "clear"))
    )
    _should_log = args.cmd not in _NO_LOG and not _read_only

    if _should_log:
        open_log(cfg, paths, args.cmd)

    dispatch = {
        "snapshot":      cmd_snapshot,
        "diff":          cmd_diff,
        "diff-commands": cmd_diff_commands,
        "patch":         cmd_patch,
        "baseline":      cmd_baseline,
        "apply":         cmd_apply,
        "rollback":      cmd_rollback,
        "deploy":        cmd_deploy,
        "pack":          cmd_pack,
        "status":        cmd_status,
        "logs":          cmd_logs,
        "config":        cmd_config,
    }

    _result = "success"
    try:
        dispatch[args.cmd](args, paths, cfg)
    except KeyboardInterrupt:
        _result = "interrupted"
        print("\n\u26a0\ufe0f   Interrupted (Ctrl+C)")
        sys.exit(130)
    except SystemExit as e:
        _result = "success" if e.code == 0 else "failure"
        raise
    except Exception:
        _result = "failure"
        raise
    finally:
        if _should_log:
            check_log_size(cfg, paths)
        close_log(_result)


if __name__ == "__main__":
    main()
