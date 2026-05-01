"""Data models for Delta."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class EntityType(str, Enum):
    BASELINE = "baseline"
    PATCH = "patch"


# ---------------------------------------------------------------------------
# Variable & command specs
# ---------------------------------------------------------------------------

@dataclass
class VariableSpec:
    name: str
    required: bool = True
    default: str | None = None
    description: str = ""

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"name": self.name, "required": self.required}
        if self.default is not None:
            d["default"] = self.default
        if self.description:
            d["description"] = self.description
        return d

    @classmethod
    def from_dict(cls, d: dict) -> VariableSpec:
        return cls(name=d["name"], required=d.get("required", True),
                   default=d.get("default"), description=d.get("description", ""))


@dataclass
class CommandSpec:
    cmd: str
    save_output: bool = False
    output_key: str = ""
    optional: bool = False
    run_once: bool = False

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"cmd": self.cmd}
        if self.save_output:
            d["save_output"] = True
            d["output_key"] = self.output_key
        if self.optional:
            d["optional"] = True
        if self.run_once:
            d["run_once"] = True
        return d

    @classmethod
    def from_dict(cls, d: dict) -> CommandSpec:
        return cls(cmd=d["cmd"], save_output=d.get("save_output", False),
                   output_key=d.get("output_key", ""), optional=d.get("optional", False),
                   run_once=d.get("run_once", False))


@dataclass
class CommandBlock:
    pre: list[CommandSpec] = field(default_factory=list)
    post: list[CommandSpec] = field(default_factory=list)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {}
        if self.pre:
            d["pre"] = [c.to_dict() for c in self.pre]
        if self.post:
            d["post"] = [c.to_dict() for c in self.post]
        return d

    @classmethod
    def from_dict(cls, d: dict) -> CommandBlock:
        return cls(pre=[CommandSpec.from_dict(c) for c in d.get("pre", [])],
                   post=[CommandSpec.from_dict(c) for c in d.get("post", [])])

    @property
    def is_empty(self) -> bool:
        return not self.pre and not self.post


# ---------------------------------------------------------------------------
# File information (regular files AND symlinks — unified)
# ---------------------------------------------------------------------------

@dataclass
class FileInfo:
    """A tracked file or symlink. Symlinks have is_symlink=True + symlink_target."""
    path: str
    md5: str = ""
    owner: str = ""
    group: str = ""
    mode: str = ""
    size: int = 0
    is_symlink: bool = False
    symlink_target: str = ""

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"path": self.path}
        if self.md5:
            d["md5"] = self.md5
        if self.owner:
            d["owner"] = self.owner
        if self.group:
            d["group"] = self.group
        if self.mode:
            d["mode"] = self.mode
        if self.size:
            d["size"] = self.size
        if self.is_symlink:
            d["is_symlink"] = True
            d["symlink_target"] = self.symlink_target
        return d

    @classmethod
    def from_dict(cls, d: dict) -> FileInfo:
        return cls(
            path=d["path"], md5=d.get("md5", ""), owner=d.get("owner", ""),
            group=d.get("group", ""), mode=d.get("mode", ""), size=d.get("size", 0),
            is_symlink=d.get("is_symlink", False),
            symlink_target=d.get("symlink_target", ""),
        )


# ---------------------------------------------------------------------------
# Scan result (cached device state)
# ---------------------------------------------------------------------------

@dataclass
class ScanResult:
    """Cached result of scanning a device."""
    timestamp: str = ""
    host: str = ""
    reference: str = ""  # entity name this was fetched against
    files: list[FileInfo] = field(default_factory=list)  # includes symlinks

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp or datetime.now().isoformat(),
            "host": self.host,
            "reference": self.reference,
            "files": [f.to_dict() for f in self.files],
        }

    @classmethod
    def from_dict(cls, d: dict) -> ScanResult:
        return cls(
            timestamp=d.get("timestamp", ""), host=d.get("host", ""),
            reference=d.get("reference", ""),
            files=[FileInfo.from_dict(f) for f in d.get("files", [])],
        )

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def total_size(self) -> int:
        return sum(f.size for f in self.files if not f.is_symlink)


# ---------------------------------------------------------------------------
# Staging manifest
# ---------------------------------------------------------------------------

@dataclass
class StagingManifest:
    reference: str = ""
    modified: list[str] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    # path → "cache" (file lives in .delta/cache/files/) or "local" (in .delta/staging/files/)
    sources: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"reference": self.reference}
        if self.modified:
            d["modified"] = self.modified
        if self.created:
            d["created"] = self.created
        if self.deleted:
            d["deleted"] = self.deleted
        if self.sources:
            d["sources"] = self.sources
        return d

    @classmethod
    def from_dict(cls, d: dict) -> StagingManifest:
        return cls(
            reference=d.get("reference", ""),
            modified=d.get("modified", []),
            created=d.get("created", []),
            deleted=d.get("deleted", []),
            sources=d.get("sources", {}),
        )

    @property
    def is_empty(self) -> bool:
        return not self.modified and not self.created and not self.deleted

    @property
    def total_files(self) -> int:
        return len(self.modified) + len(self.created) + len(self.deleted)

    def has_file(self, path: str) -> bool:
        return path in self.modified or path in self.created or path in self.deleted

    def remove_file(self, path: str) -> bool:
        for lst in (self.modified, self.created, self.deleted):
            if path in lst:
                lst.remove(path)
                self.sources.pop(path, None)
                return True
        return False


# ---------------------------------------------------------------------------
# Diff result
# ---------------------------------------------------------------------------

@dataclass
class DiffResult:
    """Result of comparing device state with a reference."""
    reference_name: str
    reference_type: EntityType
    modified: list[FileInfo] = field(default_factory=list)
    created: list[FileInfo] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.modified or self.created or self.deleted)

    @property
    def total_changes(self) -> int:
        return len(self.modified) + len(self.created) + len(self.deleted)


# ---------------------------------------------------------------------------
# SSH config
# ---------------------------------------------------------------------------

@dataclass
class SSHConfig:
    host: str = ""
    port: int = 22
    user: str = "root"
    key_file: str = ""
    password: str = ""
    connect_timeout: int = 30

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"host": self.host, "port": self.port,
                              "user": self.user, "connect_timeout": self.connect_timeout}
        if self.key_file:
            d["key_file"] = self.key_file
        return d

    @classmethod
    def from_dict(cls, d: dict) -> SSHConfig:
        return cls(host=d.get("host", ""), port=d.get("port", 22),
                   user=d.get("user", "root"), key_file=d.get("key_file", ""),
                   password=d.get("password", ""), connect_timeout=d.get("connect_timeout", 30))


# ---------------------------------------------------------------------------
# Ownership
# ---------------------------------------------------------------------------

@dataclass
class OwnershipData:
    default_owner: str = "root"
    default_group: str = "root"
    default_mode: str = "0644"
    exceptions: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict:
        # Sort exceptions by path for stable serialization
        sorted_exc = sorted(self.exceptions, key=lambda e: e.get("path", ""))
        return {"default_owner": self.default_owner, "default_group": self.default_group,
                "default_mode": self.default_mode, "exceptions": sorted_exc}

    @classmethod
    def from_dict(cls, d: dict) -> OwnershipData:
        return cls(default_owner=d.get("default_owner", "root"),
                   default_group=d.get("default_group", "root"),
                   default_mode=d.get("default_mode", "0644"),
                   exceptions=d.get("exceptions", []))


# ---------------------------------------------------------------------------
# Transfer config
# ---------------------------------------------------------------------------

class TransferMethod(str, Enum):
    AUTO = "auto"
    SFTP = "sftp"
    TAR = "tar"
    RSYNC = "rsync"


@dataclass
class TransferConfig:
    method: TransferMethod = TransferMethod.AUTO
    compress: bool = False

    def to_dict(self) -> dict:
        return {"method": self.method.value, "compress": self.compress}

    @classmethod
    def from_dict(cls, d: dict) -> TransferConfig:
        try:
            method = TransferMethod(d.get("method", "auto"))
        except ValueError:
            method = TransferMethod.AUTO
        return cls(method=method, compress=d.get("compress", False))


# ---------------------------------------------------------------------------
# Baseline metadata
# ---------------------------------------------------------------------------

@dataclass
class BaselineMetadata:
    name: str
    description: str = ""
    created_at: str = ""
    tracked_paths: list[str] = field(default_factory=list)
    ignore_patterns: list[str] = field(default_factory=list)
    variables: list[VariableSpec] = field(default_factory=list)
    on_fetch: CommandBlock = field(default_factory=CommandBlock)
    ownership: OwnershipData = field(default_factory=OwnershipData)
    command_outputs: dict[str, str] = field(default_factory=dict)
    symlink_targets: dict[str, str] = field(default_factory=dict)  # path→target
    file_count: int = 0
    total_size: int = 0

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "name": self.name, "type": EntityType.BASELINE.value,
            "description": self.description,
            "created_at": self.created_at or datetime.now().isoformat(),
            "tracked_paths": self.tracked_paths,
            "ignore_patterns": self.ignore_patterns,
            "variables": [v.to_dict() for v in self.variables],
            "ownership": self.ownership.to_dict(),
            "file_count": self.file_count, "total_size": self.total_size,
        }
        if not self.on_fetch.is_empty:
            d["on_fetch"] = self.on_fetch.to_dict()
        if self.command_outputs:
            d["command_outputs"] = self.command_outputs
        if self.symlink_targets:
            d["symlink_targets"] = self.symlink_targets
        return d

    @classmethod
    def from_dict(cls, d: dict) -> BaselineMetadata:
        return cls(
            name=d["name"], description=d.get("description", ""),
            created_at=d.get("created_at", ""),
            tracked_paths=d.get("tracked_paths", []),
            ignore_patterns=d.get("ignore_patterns", []),
            variables=[VariableSpec.from_dict(v) for v in d.get("variables", [])],
            on_fetch=CommandBlock.from_dict(d.get("on_fetch", {})),
            ownership=OwnershipData.from_dict(d.get("ownership", {})),
            command_outputs=d.get("command_outputs", {}),
            symlink_targets=d.get("symlink_targets", {}),
            file_count=d.get("file_count", 0), total_size=d.get("total_size", 0),
        )


# ---------------------------------------------------------------------------
# Patch metadata
# ---------------------------------------------------------------------------

@dataclass
class PatchMetadata:
    name: str
    baseline: str
    description: str = ""
    created_at: str = ""
    updated_at: str = ""
    ignore_patterns: list[str] = field(default_factory=list)
    variables: list[VariableSpec] = field(default_factory=list)
    on_fetch: CommandBlock = field(default_factory=CommandBlock)
    on_apply: CommandBlock = field(default_factory=CommandBlock)
    ownership: OwnershipData = field(default_factory=OwnershipData)
    command_outputs: dict[str, str] = field(default_factory=dict)
    modified_files: list[str] = field(default_factory=list)
    created_files: list[str] = field(default_factory=list)
    deleted_files: list[str] = field(default_factory=list)
    symlink_targets: dict[str, str] = field(default_factory=dict)  # path→target for symlinks
    hash: str = ""  # Cached content hash, updated on commit

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "name": self.name, "type": EntityType.PATCH.value,
            "baseline": self.baseline, "description": self.description,
            "created_at": self.created_at or datetime.now().isoformat(),
            "updated_at": self.updated_at or datetime.now().isoformat(),
            "ignore_patterns": self.ignore_patterns,
            "variables": [v.to_dict() for v in self.variables],
            "ownership": self.ownership.to_dict(),
            "modified_files": self.modified_files,
            "created_files": self.created_files,
            "deleted_files": self.deleted_files,
        }
        if self.hash:
            d["hash"] = self.hash
        if self.symlink_targets:
            d["symlink_targets"] = self.symlink_targets
        if not self.on_fetch.is_empty:
            d["on_fetch"] = self.on_fetch.to_dict()
        if not self.on_apply.is_empty:
            d["on_apply"] = self.on_apply.to_dict()
        if self.command_outputs:
            d["command_outputs"] = self.command_outputs
        return d

    @classmethod
    def from_dict(cls, d: dict) -> PatchMetadata:
        return cls(
            name=d["name"], baseline=d["baseline"],
            description=d.get("description", ""),
            created_at=d.get("created_at", ""), updated_at=d.get("updated_at", ""),
            ignore_patterns=d.get("ignore_patterns", []),
            variables=[VariableSpec.from_dict(v) for v in d.get("variables", [])],
            on_fetch=CommandBlock.from_dict(d.get("on_fetch", {})),
            on_apply=CommandBlock.from_dict(d.get("on_apply", {})),
            ownership=OwnershipData.from_dict(d.get("ownership", {})),
            command_outputs=d.get("command_outputs", {}),
            modified_files=d.get("modified_files", []),
            created_files=d.get("created_files", []),
            deleted_files=d.get("deleted_files", []),
            symlink_targets=d.get("symlink_targets", {}),
            hash=d.get("hash", ""),
        )


# ---------------------------------------------------------------------------
# Delta config & state
# ---------------------------------------------------------------------------

@dataclass
class ConfigDefaults:
    """Default settings copied into new baselines/patches at creation time."""
    ignore_patterns: list[str] = field(default_factory=list)
    variables: list[VariableSpec] = field(default_factory=list)
    on_fetch: CommandBlock = field(default_factory=CommandBlock)
    on_apply: CommandBlock = field(default_factory=CommandBlock)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {}
        if self.ignore_patterns:
            d["ignore_patterns"] = self.ignore_patterns
        if self.variables:
            d["variables"] = [v.to_dict() for v in self.variables]
        if not self.on_fetch.is_empty:
            d["on_fetch"] = self.on_fetch.to_dict()
        if not self.on_apply.is_empty:
            d["on_apply"] = self.on_apply.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> ConfigDefaults:
        return cls(
            ignore_patterns=d.get("ignore_patterns", []),
            variables=[VariableSpec.from_dict(v) for v in d.get("variables", [])],
            on_fetch=CommandBlock.from_dict(d.get("on_fetch", {})),
            on_apply=CommandBlock.from_dict(d.get("on_apply", {})),
        )


@dataclass
class DeltaConfig:
    ssh: SSHConfig = field(default_factory=SSHConfig)
    transfer: TransferConfig = field(default_factory=TransferConfig)
    editor: str = ""
    default_patch_template: str = ""
    default_baseline_template: str = ""
    defaults: ConfigDefaults = field(default_factory=ConfigDefaults)
    log_filename_pattern: str = "{datetime}_{command}_{result}"
    log_max_count: int = 50
    log_max_size_mb: int = 100
    log_enabled: bool = True

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "ssh": self.ssh.to_dict(), "transfer": self.transfer.to_dict(),
            "log": {"enabled": self.log_enabled,
                    "filename_pattern": self.log_filename_pattern,
                    "max_count": self.log_max_count, "max_size_mb": self.log_max_size_mb},
        }
        if self.editor:
            d["editor"] = self.editor
        if self.default_patch_template:
            d.setdefault("templates", {})["default_patch"] = self.default_patch_template
        if self.default_baseline_template:
            d.setdefault("templates", {})["default_baseline"] = self.default_baseline_template
        defaults_d = self.defaults.to_dict()
        if defaults_d:
            d["defaults"] = defaults_d
        return d

    @classmethod
    def from_dict(cls, d: dict) -> DeltaConfig:
        log = d.get("log", {})
        templates = d.get("templates", {})
        # Backward compat: on_fetch/on_apply at top level → move to defaults
        defaults_d = d.get("defaults", {})
        if "on_fetch" in d and "on_fetch" not in defaults_d:
            defaults_d["on_fetch"] = d["on_fetch"]
        if "on_apply" in d and "on_apply" not in defaults_d:
            defaults_d["on_apply"] = d["on_apply"]
        return cls(
            ssh=SSHConfig.from_dict(d.get("ssh", {})),
            transfer=TransferConfig.from_dict(d.get("transfer", {})),
            editor=d.get("editor", ""),
            default_patch_template=templates.get("default_patch", ""),
            default_baseline_template=templates.get("default_baseline", ""),
            defaults=ConfigDefaults.from_dict(defaults_d),
            log_filename_pattern=log.get("filename_pattern", "{datetime}_{command}_{result}"),
            log_max_count=log.get("max_count", 50), log_max_size_mb=log.get("max_size_mb", 100),
            log_enabled=log.get("enabled", True),
        )


@dataclass
class DeltaState:
    active: str = ""

    def to_dict(self) -> dict:
        return {"active": self.active}

    @classmethod
    def from_dict(cls, d: dict) -> DeltaState:
        return cls(active=d.get("active", "") or d.get("active_patch", "") or d.get("active_baseline", ""))


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------

@dataclass
class Template:
    description: str = ""
    tracked_paths: list[str] = field(default_factory=list)
    ignore_patterns: list[str] = field(default_factory=list)
    variables: list[VariableSpec] = field(default_factory=list)
    on_fetch: CommandBlock = field(default_factory=CommandBlock)
    on_apply: CommandBlock = field(default_factory=CommandBlock)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {}
        if self.description:
            d["description"] = self.description
        if self.tracked_paths:
            d["tracked_paths"] = self.tracked_paths
        if self.ignore_patterns:
            d["ignore_patterns"] = self.ignore_patterns
        if self.variables:
            d["variables"] = [v.to_dict() for v in self.variables]
        if not self.on_fetch.is_empty:
            d["on_fetch"] = self.on_fetch.to_dict()
        if not self.on_apply.is_empty:
            d["on_apply"] = self.on_apply.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> Template:
        return cls(
            description=d.get("description", ""),
            tracked_paths=d.get("tracked_paths", []),
            ignore_patterns=d.get("ignore_patterns", []),
            variables=[VariableSpec.from_dict(v) for v in d.get("variables", [])],
            on_fetch=CommandBlock.from_dict(d.get("on_fetch", {})),
            on_apply=CommandBlock.from_dict(d.get("on_apply", {})),
        )


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------

def merge_settings(
    *, template: Template | None = None,
    metadata_on_fetch: CommandBlock | None = None,
    metadata_on_apply: CommandBlock | None = None,
    metadata_variables: list[VariableSpec] | None = None,
    metadata_ignore: list[str] | None = None,
    metadata_description: str = "",
    config_file: dict | None = None,
    cli_pre_cmd: list[CommandSpec] | None = None,
    cli_post_cmd: list[CommandSpec] | None = None,
    cli_ignore: list[str] | None = None,
    cli_description: str = "",
) -> dict:
    description = ""
    variables: list[VariableSpec] = []
    ignore_patterns: list[str] = []
    on_fetch = CommandBlock()
    on_apply = CommandBlock()

    if template:
        description, variables = template.description, list(template.variables)
        ignore_patterns = list(template.ignore_patterns)
        on_fetch = CommandBlock(pre=list(template.on_fetch.pre), post=list(template.on_fetch.post))
        on_apply = CommandBlock(pre=list(template.on_apply.pre), post=list(template.on_apply.post))

    if metadata_description:
        description = metadata_description
    if metadata_variables:
        variables = list(metadata_variables)
    if metadata_ignore:
        ignore_patterns = list(metadata_ignore)
    if metadata_on_fetch and not metadata_on_fetch.is_empty:
        on_fetch = CommandBlock(pre=list(metadata_on_fetch.pre), post=list(metadata_on_fetch.post))
    if metadata_on_apply and not metadata_on_apply.is_empty:
        on_apply = CommandBlock(pre=list(metadata_on_apply.pre), post=list(metadata_on_apply.post))

    if config_file:
        if "description" in config_file:
            description = config_file["description"]
        if "variables" in config_file:
            variables = [VariableSpec.from_dict(v) for v in config_file["variables"]]
        if "ignore_patterns" in config_file:
            ignore_patterns = list(config_file["ignore_patterns"])
        if "on_fetch" in config_file:
            on_fetch = CommandBlock.from_dict(config_file["on_fetch"])
        if "on_apply" in config_file:
            on_apply = CommandBlock.from_dict(config_file["on_apply"])

    if cli_description:
        description = cli_description
    if cli_ignore:
        ignore_patterns = list(cli_ignore)
    if cli_pre_cmd:
        on_fetch = CommandBlock(pre=list(cli_pre_cmd), post=on_fetch.post)
    if cli_post_cmd:
        on_fetch = CommandBlock(pre=on_fetch.pre, post=list(cli_post_cmd))

    return {"description": description, "variables": variables,
            "ignore_patterns": ignore_patterns, "on_fetch": on_fetch, "on_apply": on_apply}


def matches_any_pattern(path: str, patterns: list[str]) -> bool:
    for p in patterns:
        try:
            if re.search(p, path):
                return True
        except re.error:
            continue
    return False


# ---------------------------------------------------------------------------
# Resolved config with source tracking (for --show-config)
# ---------------------------------------------------------------------------

@dataclass
class TrackedValue:
    """A value annotated with its source."""
    value: Any
    source: str  # e.g. "baseline/factory", "patch/wifi", "template/prod", "--ignore CLI"

    def __str__(self) -> str:
        return f"[{self.source}] {self.value}"


@dataclass
class ResolvedConfig:
    """Full resolved configuration with source tracking for display."""
    operation: str = ""
    reference: str = ""
    reference_type: str = ""
    host: str = ""
    tracked_paths: list[str] = field(default_factory=list)
    ignore_patterns: list[TrackedValue] = field(default_factory=list)
    description: TrackedValue | None = None
    variables: list[TrackedValue] = field(default_factory=list)
    on_fetch_pre: list[TrackedValue] = field(default_factory=list)
    on_fetch_post: list[TrackedValue] = field(default_factory=list)
    on_apply_pre: list[TrackedValue] = field(default_factory=list)
    on_apply_post: list[TrackedValue] = field(default_factory=list)
    transfer_method: str = ""
    transfer_compress: bool = False
    extra: dict[str, str] = field(default_factory=dict)

    def display(self) -> str:
        """Format as readable text for terminal output."""
        lines = [f"Resolved configuration for '{self.operation}':"]
        lines.append(f"  reference: {self.reference} ({self.reference_type})")
        if self.host:
            lines.append(f"  host: {self.host}")
        if self.tracked_paths:
            lines.append(f"  tracked_paths: {', '.join(self.tracked_paths)}")
        if self.transfer_method:
            lines.append(f"  transfer: {self.transfer_method}, compress={self.transfer_compress}")

        if self.description:
            lines.append(f"  description: {self.description}")

        if self.ignore_patterns:
            lines.append("  ignore_patterns:")
            for tv in self.ignore_patterns:
                lines.append(f"    [{tv.source}] {tv.value}")

        if self.variables:
            lines.append("  variables:")
            for tv in self.variables:
                lines.append(f"    [{tv.source}] {tv.value}")

        if self.on_fetch_pre or self.on_fetch_post:
            lines.append("  on_fetch:")
            for tv in self.on_fetch_pre:
                lines.append(f"    pre: [{tv.source}] {tv.value}")
            for tv in self.on_fetch_post:
                lines.append(f"    post: [{tv.source}] {tv.value}")

        if self.on_apply_pre or self.on_apply_post:
            lines.append("  on_apply:")
            for tv in self.on_apply_pre:
                lines.append(f"    pre: [{tv.source}] {tv.value}")
            for tv in self.on_apply_post:
                lines.append(f"    post: [{tv.source}] {tv.value}")

        for k, v in self.extra.items():
            lines.append(f"  {k}: {v}")

        return "\n".join(lines)

