# Delta — Device Filesystem Version Control

A git-like CLI tool for tracking, snapshotting, and deploying filesystem changes across identical devices over SSH.

## Installation

```bash
cd delta-project
pip install -e .
```

Or install directly:
```bash
pip install .
```

## Quick Start

### 1. Initialize and create a baseline

```bash
# Simple: track /etc and /opt/app on the device
delta init factory --host 192.168.1.100 -p /etc -p /opt/app

# With options
delta init factory \
  --host 192.168.1.100 \
  --user root \
  --key-file ~/.ssh/id_rsa \
  -p /etc \
  -p /opt/app \
  --description "Factory state" \
  --ignore '.*\.log$' \
  --ignore '.*\.tmp$'
```

### 2. Make changes on the device, then capture them

```bash
# Compare device with baseline and create a patch
delta diff wifi-setup

# Preview changes first (no patch created)
delta diff --dry-run

# Preview with line-by-line diffs
delta diff --dry-run --detailed
```

### 3. Apply the patch to another device

```bash
# Apply to the configured device
delta apply wifi-setup

# Apply to a different device
delta apply wifi-setup --host 10.0.0.6

# Preview what would be applied
delta apply --dry-run
```

## Commands

### `delta init <name>`

Create a baseline by downloading the current device state.

```bash
delta init <name> --host <host> -p <path> [-p <path> ...]
  --port <n>            SSH port (default: 22)
  --user <u>            SSH user (default: root)
  --key-file <f>        SSH private key file
  --description <d>     Baseline description
  --ignore <pattern>    Regex ignore pattern (repeatable)
  --list-files / -l     Show file list with sizes before downloading
  --pre-cmd <cmd>       Command to run before capture
  --post-cmd <cmd>      Command to run after capture
  --config-file <f>     YAML config file for complex setups
```

### `delta baseline <subcommand>`

Manage baselines.

```bash
delta baseline ls                     # List baselines
delta baseline use factory            # Set 'factory' as active
delta baseline copy factory backup    # Copy baseline
delta baseline info                   # Show active baseline details
delta baseline info factory           # Show specific baseline
delta baseline ignore '\.log$'        # Add ignore pattern
delta baseline ignore -r '\.log$'     # Remove ignore pattern
delta baseline track /opt/newapp      # Add tracked path
delta baseline track -r /opt/oldapp   # Remove tracked path
delta baseline refresh                # Re-download active baseline from device
delta baseline refresh factory        # Re-download specific baseline
delta baseline rm old-factory         # Remove a baseline
```

When removing a baseline, any patches that depend on it become "orphaned."
Orphaned patches remain fully functional for `apply` (they store all their
changed files locally), but lose the ability to diff against their parent baseline.

### `delta diff [name]`

Compare device state and create/update a patch.

```bash
delta diff <patch-name>                   # Create/update patch
delta diff                                # Update active patch
delta diff wifi --against factory         # Compare vs 'factory'
delta diff wifi --against-base            # Compare vs patch's parent baseline
delta diff --dry-run                      # Preview only
delta diff --dry-run -l                   # Preview with full file list + sizes
delta diff --dry-run --detailed           # Preview with line-by-line diffs
delta diff wifi -i '\.bak$'              # Ignore extra patterns
delta diff wifi --config-file patch.yaml  # Complex setup via YAML
```

**Comparison logic:**
- Default: compares device vs active patch (or active baseline if no patch)
- `--against <name>`: compares against a specific baseline or patch
- `--against-base`: compares against the parent baseline of the current patch

### `delta apply [name]`

Apply a patch to a device.

```bash
delta apply <patch-name>                  # Apply patch
delta apply                               # Apply active patch
delta apply wifi --host 10.0.0.6          # Apply to different device
delta apply --dry-run                     # Preview only
```

### `delta patch <subcommand>`

Manage patches.

```bash
delta patch ls                 # List patches
delta patch use wifi-setup     # Set as active
delta patch info               # Show active patch details
delta patch info wifi-setup    # Show specific patch
delta patch copy wifi v2       # Copy a patch
delta patch rm wifi-setup      # Remove a patch
```

### `delta compare <entity-a> <entity-b>`

Compare two local entities without SSH. Works with any combination of baselines and patches.

```bash
delta compare factory v2-baseline     # Baseline vs baseline
delta compare factory wifi-setup      # Baseline vs patch
delta compare wifi-setup new-feature  # Patch vs patch
delta compare factory wifi -d         # With line-by-line unified diffs
```

### `delta status`

Show current state: active baseline, patch, SSH target.

### `delta log <subcommand>`

Manage log files.

```bash
delta log ls              # List logs
delta log show <file>     # Display log contents
delta log clean           # Remove all logs
delta log clean --keep 10 # Keep 10 most recent
```

## Global Options

```bash
delta --yes/-y ...     # Skip all confirmation prompts
delta --var/-V KEY=VALUE ...  # Set variables for commands
```

## Unified Namespace

Baseline and patch names share the same namespace — names must be unique across both. This simplifies the CLI: `--against <name>` works with any entity without needing to specify whether it's a baseline or patch.

## Remote Commands

Commands can be executed on the device at various stages. Use the `--pre-cmd` and `--post-cmd` flags:

```bash
# Simple command
delta init factory --host 10.0.0.1 -p /etc \
  --pre-cmd "systemctl stop myservice" \
  --post-cmd "systemctl start myservice"

# Save command output to metadata
delta init factory --host 10.0.0.1 -p /etc \
  --pre-cmd "save:os_info:cat /etc/os-release"
```

### Required Commands

All commands must succeed by default — a non-zero exit code aborts the operation. Mark specific commands as `optional: true` to allow failures:

```yaml
pre_capture_commands:
  - cmd: "systemctl stop myservice"    # fail → abort (default)
  - cmd: "cat /etc/optional.conf"
    save_output: true
    output_key: opt_conf
    optional: true                     # fail → warning, continue
```

For conditional logic, use bash directly:

```yaml
  - cmd: "pgrep myservice && systemctl stop myservice || echo 'not running'"
  - cmd: "[ -f /etc/custom.conf ] && cat /etc/custom.conf || echo 'none'"
    save_output: true
    output_key: custom_conf
```

### Dynamic Description

Description templates can reference `${VAR}` placeholders from three sources (each overrides the previous):

1. **Metadata fields** — `${name}`, `${file_count}`, `${total_size}`, `${created_at}`, `${tracked_paths}` (baselines) or `${baseline}`, `${modified_count}`, `${created_count}`, `${deleted_count}`, `${total_changes}` (patches)
2. **Variables** — defaults from config + `--var KEY=VALUE` overrides
3. **Command outputs** — saved with `save_output: true` + `output_key`

```yaml
description: "${name}: ${DEVICE_ID}, kernel ${kernel_info}, ${file_count} files (${total_size})"
variables:
  - name: DEVICE_ID
    required: true
pre_capture_commands:
  - cmd: "uname -r"
    save_output: true
    output_key: kernel_info
```

Result: `"factory: dev-001, kernel 5.15.0, 1523 files (45.2 MB)"`

After all commands run, `${kernel_info}` is replaced with the actual output. This works for both `init` and `diff`.

### Variables

Variables use `${VAR_NAME}` syntax in commands and descriptions, passed via `--var`:

```bash
delta init factory --host 10.0.0.1 -p /etc \
  --pre-cmd 'echo "Device: ${DEVICE_ID}"' \
  --var DEVICE_ID=dev-001
```

### Config File

For complex setups, use `--config-file` (works with both `init` and `diff`):

```yaml
# config.yaml
description: "Build ${BUILD_ID}, OS: ${os_info}"
tracked_paths:
  - /etc
  - /opt/app
ignore_patterns:
  - '.*\.log$'
variables:
  - name: BUILD_ID
    required: true
  - name: ENV
    required: false
    default: production
pre_capture_commands:
  - cmd: "systemctl stop myservice"
  - cmd: "cat /etc/os-release"
    save_output: true
    output_key: os_info
  - cmd: "cat /etc/optional.conf"
    optional: true
post_capture_commands:
  - cmd: "systemctl start myservice"
```

```bash
delta init factory --host 10.0.0.1 --config-file config.yaml --var BUILD_ID=42
delta diff wifi-setup --config-file patch-config.yaml --var BUILD_ID=43
```

## Detailed Diffs

Both `delta diff --detailed` and `delta compare --detailed` show unified line-by-line diffs for modified text files. When diffing against a device, modified files are temporarily downloaded to `.delta/tmp/` for comparison and automatically cleaned up afterward. All temporary data stays within the `.delta/` directory.

## File Ownership

Delta automatically tracks file ownership (user, group, permissions). It optimizes storage by finding the most common owner/group/mode and storing them as defaults, only recording exceptions for files that differ. Permissions (chmod) are stored because SFTP upload does not preserve them — without explicit restore, a `755` script would become `644` after apply.

## Symlinks

Delta tracks symlinks as metadata (path and target) rather than downloading their content. During `apply`, symlinks are recreated on the target device using `ln -sf`. The files that symlinks point to are only downloaded if they fall within tracked paths.

## Transfer Backends

Delta supports three transfer backends, configurable in `.delta/config.yaml`:

```yaml
transfer:
  method: auto       # auto | sftp | tar | rsync
```

**sftp** — per-file transfer via scp. Slowest, but always available.

**tar** — tar streaming over SSH pipes. Both directions stream directly — no temp files on the device. 10-50x faster than sftp for many small files.

**rsync** — `rsync --files-from` with our pre-filtered file list. Fastest for incremental updates. Requires rsync on the device. Uses the same SSH connection via ControlMaster socket.

**auto** (default) — probes the device on first connection: rsync → tar → sftp.

If `rsync` is explicitly selected but unavailable, delta shows an error suggesting to switch to `tar` or `sftp`.

## Directory Structure

```
.delta/
├── config.yaml          # SSH settings, log config
├── state.yaml           # Active entity (baseline or patch)
├── tmp/                 # Temporary files (auto-cleaned)
├── logs/                # Per-run log files
├── baselines/
│   └── <name>/
│       ├── metadata.yaml
│       └── files/       # Full file tree snapshot
└── patches/
    └── <name>/
        ├── metadata.yaml
        └── files/       # Only modified/created files
```

## Logging

Each modifying operation creates a log file. Filename format: `{datetime}_{command}_{result}.log`. When log count exceeds 50 or total size exceeds 100 MB, a warning is displayed.
