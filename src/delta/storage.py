"""Manage the .delta directory: config, state, baselines, patches, cache, staging."""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from delta.exceptions import ConfigError, NameConflictError, NotFoundError, StorageError
from delta.models import (
    BaselineMetadata,
    DeltaConfig,
    DeltaState,
    EntityType,
    PatchMetadata,
    ScanResult,
    StagingManifest,
    Template,
)

DELTA_DIR = ".delta"
CONFIG_FILE = "config.yaml"
STATE_FILE = "state.yaml"
BASELINES_DIR = "baselines"
PATCHES_DIR = "patches"
TEMPLATES_DIR = "templates"
CACHE_DIR = "cache"
STAGING_DIR = "staging"
LOGS_DIR = "logs"
TMP_DIR = "tmp"


class Storage:
    """Manages the .delta directory on the local machine."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Path.cwd()
        self.delta_dir = self.root / DELTA_DIR

    @property
    def is_initialized(self) -> bool:
        return (self.delta_dir / CONFIG_FILE).exists()

    def require_initialized(self) -> None:
        if not self.is_initialized:
            raise ConfigError(
                "Delta is not initialized in this directory. Run 'delta init' first."
            )

    def init(self, config: DeltaConfig) -> None:
        self.delta_dir.mkdir(exist_ok=True)
        for subdir in (BASELINES_DIR, PATCHES_DIR, TEMPLATES_DIR,
                       CACHE_DIR, STAGING_DIR, LOGS_DIR, TMP_DIR):
            (self.delta_dir / subdir).mkdir(exist_ok=True)
        # staging/files
        (self.delta_dir / STAGING_DIR / "files").mkdir(exist_ok=True)
        self.save_config(config)
        self.save_state(DeltaState())

    # ------------------------------------------------------------------
    # Config & state
    # ------------------------------------------------------------------

    def load_config(self) -> DeltaConfig:
        path = self.delta_dir / CONFIG_FILE
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return DeltaConfig.from_dict(data)

    def save_config(self, config: DeltaConfig) -> None:
        path = self.delta_dir / CONFIG_FILE
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(config.to_dict(), f, default_flow_style=False, sort_keys=False)

    def load_state(self) -> DeltaState:
        path = self.delta_dir / STATE_FILE
        if not path.exists():
            return DeltaState()
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return DeltaState.from_dict(data)

    def save_state(self, state: DeltaState) -> None:
        path = self.delta_dir / STATE_FILE
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(state.to_dict(), f, default_flow_style=False, sort_keys=False)

    # ------------------------------------------------------------------
    # Entity namespace (baselines + patches share one namespace)
    # ------------------------------------------------------------------

    def name_exists(self, name: str) -> bool:
        return self._baseline_dir(name).exists() or self._patch_dir(name).exists()

    def get_entity_type(self, name: str) -> EntityType:
        if self._baseline_dir(name).exists():
            return EntityType.BASELINE
        if self._patch_dir(name).exists():
            return EntityType.PATCH
        raise NotFoundError(f"Entity '{name}' not found.")

    def require_name_exists(self, name: str) -> EntityType:
        return self.get_entity_type(name)

    # ------------------------------------------------------------------
    # Baselines
    # ------------------------------------------------------------------

    def _baseline_dir(self, name: str) -> Path:
        return self.delta_dir / BASELINES_DIR / name

    def baseline_files_dir(self, name: str) -> Path:
        d = self._baseline_dir(name) / "files"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save_baseline(self, meta: BaselineMetadata) -> None:
        import json as _json
        d = self._baseline_dir(meta.name)
        d.mkdir(parents=True, exist_ok=True)
        data = meta.to_dict()
        with open(d / "metadata.yaml", "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        # JSON cache for fast loading
        with open(d / ".metadata_cache.json", "w", encoding="utf-8") as f:
            _json.dump(data, f, separators=(",", ":"))

    def load_baseline(self, name: str) -> BaselineMetadata:
        import json as _json
        d = self._baseline_dir(name)
        yaml_path = d / "metadata.yaml"
        json_path = d / ".metadata_cache.json"
        if not yaml_path.exists():
            raise NotFoundError(f"Baseline '{name}' not found.")
        # Use JSON cache if valid (newer than YAML)
        if json_path.exists() and json_path.stat().st_mtime >= yaml_path.stat().st_mtime:
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    return BaselineMetadata.from_dict(_json.load(f))
            except Exception:
                pass
        # Fallback: parse YAML and rebuild cache
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        meta = BaselineMetadata.from_dict(data)
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                _json.dump(data, f, separators=(",", ":"))
        except Exception:
            pass
        return meta

    def get_baseline_file(self, name: str, remote_path: str) -> Path:
        return self.baseline_files_dir(name) / remote_path.lstrip("/")

    def list_baselines(self) -> list[str]:
        d = self.delta_dir / BASELINES_DIR
        if not d.exists():
            return []
        return sorted(
            p.name for p in d.iterdir()
            if p.is_dir() and not p.name.startswith(".") and (p / "metadata.yaml").exists()
        )

    def remove_baseline(self, name: str) -> None:
        d = self._baseline_dir(name)
        if d.exists():
            shutil.rmtree(d)

    def copy_baseline(self, src: str, dst: str) -> None:
        if self.name_exists(dst):
            raise NameConflictError(f"Name '{dst}' already exists.")
        shutil.copytree(self._baseline_dir(src), self._baseline_dir(dst))
        meta = self.load_baseline(dst)
        meta.name = dst
        self.save_baseline(meta)

    # ------------------------------------------------------------------
    # Patches
    # ------------------------------------------------------------------

    def _patch_dir(self, name: str) -> Path:
        return self.delta_dir / PATCHES_DIR / name

    def patch_files_dir(self, name: str) -> Path:
        d = self._patch_dir(name) / "files"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save_patch(self, meta: PatchMetadata) -> None:
        import json as _json
        d = self._patch_dir(meta.name)
        d.mkdir(parents=True, exist_ok=True)
        data = meta.to_dict()
        with open(d / "metadata.yaml", "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        with open(d / ".metadata_cache.json", "w", encoding="utf-8") as f:
            _json.dump(data, f, separators=(",", ":"))

    def load_patch(self, name: str) -> PatchMetadata:
        import json as _json
        d = self._patch_dir(name)
        yaml_path = d / "metadata.yaml"
        json_path = d / ".metadata_cache.json"
        if not yaml_path.exists():
            raise NotFoundError(f"Patch '{name}' not found.")
        if json_path.exists() and json_path.stat().st_mtime >= yaml_path.stat().st_mtime:
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    return PatchMetadata.from_dict(_json.load(f))
            except Exception:
                pass
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        meta = PatchMetadata.from_dict(data)
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                _json.dump(data, f, separators=(",", ":"))
        except Exception:
            pass
        return meta

    def get_patch_file(self, name: str, remote_path: str) -> Path:
        return self.patch_files_dir(name) / remote_path.lstrip("/")

    def list_patches(self) -> list[str]:
        d = self.delta_dir / PATCHES_DIR
        if not d.exists():
            return []
        return sorted(
            p.name for p in d.iterdir()
            if p.is_dir() and not p.name.startswith(".") and (p / "metadata.yaml").exists()
        )

    def remove_patch(self, name: str) -> None:
        d = self._patch_dir(name)
        if d.exists():
            shutil.rmtree(d)

    def copy_patch(self, src: str, dst: str) -> None:
        if self.name_exists(dst):
            raise NameConflictError(f"Name '{dst}' already exists.")
        shutil.copytree(self._patch_dir(src), self._patch_dir(dst))
        meta = self.load_patch(dst)
        meta.name = dst
        self.save_patch(meta)

    # ------------------------------------------------------------------
    # Cache (scan results)
    # ------------------------------------------------------------------

    @property
    def cache_dir(self) -> Path:
        return self.delta_dir / CACHE_DIR

    @property
    def cache_files_dir(self) -> Path:
        d = self.cache_dir / "files"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save_scan(self, scan: ScanResult) -> None:
        import json
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        # JSON is ~50x faster to parse than YAML for large scan results
        with open(self.cache_dir / "scan.json", "w", encoding="utf-8") as f:
            json.dump(scan.to_dict(), f, separators=(",", ":"))
        # Remove old YAML scan if exists
        old = self.cache_dir / "scan.yaml"
        if old.exists():
            old.unlink()

    def load_scan(self) -> ScanResult | None:
        import json
        path = self.cache_dir / "scan.json"
        if not path.exists():
            # Backward compat: try old YAML format
            yaml_path = self.cache_dir / "scan.yaml"
            if yaml_path.exists():
                with open(yaml_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                # Auto-migrate to JSON
                scan = ScanResult.from_dict(data)
                self.save_scan(scan)
                return scan
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return ScanResult.from_dict(data)

    def clean_cache(self) -> None:
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True)

    # ------------------------------------------------------------------
    # Staging
    # ------------------------------------------------------------------

    @property
    def staging_dir(self) -> Path:
        return self.delta_dir / STAGING_DIR

    @property
    def staging_files_dir(self) -> Path:
        d = self.staging_dir / "files"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def load_staging(self) -> StagingManifest:
        path = self.staging_dir / "manifest.yaml"
        if not path.exists():
            return StagingManifest()
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return StagingManifest.from_dict(data)

    def save_staging(self, manifest: StagingManifest) -> None:
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        with open(self.staging_dir / "manifest.yaml", "w", encoding="utf-8") as f:
            yaml.dump(manifest.to_dict(), f, default_flow_style=False, sort_keys=False)

    def clear_staging(self) -> None:
        """Remove all staged files and manifest."""
        files_dir = self.staging_dir / "files"
        if files_dir.exists():
            shutil.rmtree(files_dir)
            files_dir.mkdir(parents=True)
        manifest = self.staging_dir / "manifest.yaml"
        if manifest.exists():
            manifest.unlink()

    def get_staging_file(self, remote_path: str) -> Path:
        """Get the path where local staged files live (for --file, --force, edit)."""
        return self.staging_files_dir / remote_path.lstrip("/")

    def resolve_staged_file(self, manifest: StagingManifest, remote_path: str) -> Path | None:
        """Resolve a staged file's actual location based on its source.

        Priority: explicit source → fallback to both locations.
        """
        source = manifest.sources.get(remote_path)
        if source == "local":
            p = self.get_staging_file(remote_path)
            return p if p.exists() else None
        if source == "cache":
            p = self.cache_files_dir / remote_path.lstrip("/")
            return p if p.exists() else None
        # No source recorded — try cache first, then staging (backward compat)
        cache_p = self.cache_files_dir / remote_path.lstrip("/")
        if cache_p.exists():
            return cache_p
        staging_p = self.get_staging_file(remote_path)
        if staging_p.exists():
            return staging_p
        return None

    # ------------------------------------------------------------------
    # Templates
    # ------------------------------------------------------------------

    @property
    def templates_dir(self) -> Path:
        return self.delta_dir / TEMPLATES_DIR

    def list_templates(self) -> list[str]:
        d = self.templates_dir
        if not d.exists():
            return []
        return sorted(p.stem for p in d.iterdir() if p.suffix in (".yaml", ".yml"))

    def load_template(self, name: str) -> Template:
        for ext in (".yaml", ".yml"):
            path = self.templates_dir / f"{name}{ext}"
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                return Template.from_dict(data)
        raise NotFoundError(f"Template '{name}' not found.")

    def save_template(self, name: str, template: Template) -> None:
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        with open(self.templates_dir / f"{name}.yaml", "w", encoding="utf-8") as f:
            yaml.dump(template.to_dict(), f, default_flow_style=False, sort_keys=False)

    def remove_template(self, name: str) -> None:
        for ext in (".yaml", ".yml"):
            path = self.templates_dir / f"{name}{ext}"
            if path.exists():
                path.unlink()
                return

    def template_path(self, name: str) -> Path | None:
        for ext in (".yaml", ".yml"):
            path = self.templates_dir / f"{name}{ext}"
            if path.exists():
                return path
        return None

    # ------------------------------------------------------------------
    # Logs & tmp
    # ------------------------------------------------------------------

    @property
    def logs_dir(self) -> Path:
        return self.delta_dir / LOGS_DIR

    @property
    def tmp_dir(self) -> Path:
        d = self.delta_dir / TMP_DIR
        d.mkdir(parents=True, exist_ok=True)
        return d

    def clean_tmp(self) -> None:
        d = self.delta_dir / TMP_DIR
        if d.exists():
            shutil.rmtree(d)
            d.mkdir(parents=True)

    # ------------------------------------------------------------------
    # Listing & editing
    # ------------------------------------------------------------------

    def list_all_entities(self) -> list[dict]:
        """Fast listing — reads only top-level fields, not full metadata."""
        result = []
        for name in self.list_baselines():
            d = self._read_metadata_dict(self._baseline_dir(name) / "metadata.yaml")
            result.append({
                "name": name, "type": EntityType.BASELINE.value,
                "description": d.get("description", ""),
                "created_at": d.get("created_at", ""),
            })
        for name in self.list_patches():
            d = self._read_metadata_dict(self._patch_dir(name) / "metadata.yaml")
            result.append({
                "name": name, "type": EntityType.PATCH.value,
                "description": d.get("description", ""),
                "created_at": d.get("created_at", ""),
                "baseline": d.get("baseline", ""),
            })
        return result

    @staticmethod
    def _read_metadata_dict(path: Path) -> dict:
        """Read only top-level scalar fields from YAML — fast for large files.

        Skips list/dict values (modified_files, ownership, etc.) which we don't
        need for listing.
        """
        if not path.exists():
            return {}
        result: dict = {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line or line[0] in (" ", "\t", "#", "\n"):
                        continue
                    if ":" not in line:
                        continue
                    key, _, value = line.partition(":")
                    value = value.strip()
                    # Skip lines where value is empty (nested structure follows)
                    if not value or value in ("{}", "[]"):
                        continue
                    # Strip surrounding quotes
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                        value = value[1:-1]
                    result[key.strip()] = value
        except Exception:
            pass
        return result

    def get_edit_path(self, target: str, name: str = "") -> Path:
        if target == "config":
            return self.delta_dir / CONFIG_FILE
        elif target == "template":
            if not name:
                raise NotFoundError("Template name required.")
            path = self.template_path(name)
            return path or (self.templates_dir / f"{name}.yaml")
        elif target == "baseline":
            if not name:
                raise NotFoundError("Baseline name required.")
            p = self._baseline_dir(name) / "metadata.yaml"
            if not p.exists():
                raise NotFoundError(f"Baseline '{name}' not found.")
            return p
        elif target == "patch":
            if not name:
                raise NotFoundError("Patch name required.")
            p = self._patch_dir(name) / "metadata.yaml"
            if not p.exists():
                raise NotFoundError(f"Patch '{name}' not found.")
            return p
        else:
            raise NotFoundError(f"Unknown edit target: '{target}'")

    def compute_patch_hash(self, name: str) -> str:
        """Compute short hash of patch content (files + metadata)."""
        import hashlib
        h = hashlib.md5()
        patch_dir = self._patch_dir(name)
        if not patch_dir.exists():
            return ""
        for f in sorted(patch_dir.rglob("*")):
            if f.is_file():
                h.update(str(f.relative_to(patch_dir)).encode())
                h.update(f.read_bytes())
        return h.hexdigest()[:12]
