"""Tests for Delta — device filesystem version control."""

import logging
from pathlib import Path

import pytest
import yaml

from delta import ui
from delta.models import (
    BaselineMetadata, CommandBlock, CommandSpec, DeltaConfig, DeltaState,
    DiffResult, EntityType, FileInfo, OwnershipData, PatchMetadata,
    ScanResult, SSHConfig, StagingManifest, Template, TransferConfig,
    TransferMethod, VariableSpec, merge_settings, matches_any_pattern,
)
from delta.storage import Storage


@pytest.fixture
def storage(tmp_path):
    s = Storage(root=tmp_path)
    s.init(DeltaConfig(ssh=SSHConfig(host="10.0.0.1")))
    return s


# ======================================================================
# Models
# ======================================================================

class TestModels:
    def test_variable_spec(self):
        v = VariableSpec(name="X", default="y")
        assert VariableSpec.from_dict(v.to_dict()).default == "y"

    def test_command_spec(self):
        c = CommandSpec(cmd="echo", optional=True)
        assert CommandSpec.from_dict(c.to_dict()).optional

    def test_command_block(self):
        cb = CommandBlock(pre=[CommandSpec(cmd="a")], post=[CommandSpec(cmd="b")])
        assert not cb.is_empty
        assert CommandBlock().is_empty

    def test_file_info(self):
        f = FileInfo(path="/a", md5="abc", size=100)
        assert FileInfo.from_dict(f.to_dict()).size == 100

    def test_file_info_symlink(self):
        f = FileInfo(path="/l", is_symlink=True, symlink_target="/t")
        r = FileInfo.from_dict(f.to_dict())
        assert r.is_symlink and r.symlink_target == "/t"

    def test_scan_result(self):
        s = ScanResult(host="h", reference="ref", files=[
            FileInfo(path="/a", size=10),
            FileInfo(path="/l", is_symlink=True, symlink_target="/t"),
        ])
        assert s.file_count == 2
        assert s.total_size == 10  # Symlinks don't count for size
        r = ScanResult.from_dict(s.to_dict())
        assert r.reference == "ref"
        assert r.file_count == 2

    def test_staging_manifest(self):
        m = StagingManifest(reference="bl", modified=["/a"], deleted=["/b"])
        assert m.total_files == 2
        assert m.has_file("/a")
        assert m.remove_file("/a")
        assert m.total_files == 1

    def test_diff_result(self):
        dr = DiffResult("x", EntityType.BASELINE,
                         modified=[FileInfo(path="/a")], deleted=["/b"])
        assert dr.has_changes and dr.total_changes == 2
        assert not DiffResult("x", EntityType.BASELINE).has_changes

    def test_baseline_metadata(self):
        m = BaselineMetadata(name="bl", tracked_paths=["/etc"], file_count=5)
        assert BaselineMetadata.from_dict(m.to_dict()).file_count == 5

    def test_patch_metadata(self):
        m = PatchMetadata(name="p", baseline="bl", modified_files=["/a"],
                           on_apply=CommandBlock(pre=[CommandSpec(cmd="stop")]))
        r = PatchMetadata.from_dict(m.to_dict())
        assert r.baseline == "bl" and len(r.on_apply.pre) == 1

    def test_delta_config(self):
        c = DeltaConfig(ssh=SSHConfig(host="h"), editor="vim",
                         transfer=TransferConfig(compress=True))
        r = DeltaConfig.from_dict(c.to_dict())
        assert r.editor == "vim" and r.transfer.compress

    def test_template(self):
        t = Template(description="d", on_fetch=CommandBlock(pre=[CommandSpec(cmd="x")]))
        assert Template.from_dict(t.to_dict()).on_fetch.pre[0].cmd == "x"

    def test_merge_settings(self):
        tmpl = Template(description="T", ignore_patterns=["*.log"])
        r = merge_settings(template=tmpl, cli_description="CLI")
        assert r["description"] == "CLI" and r["ignore_patterns"] == ["*.log"]

    def test_matches_pattern(self):
        assert matches_any_pattern("/etc/a.log", [".*\\.log$"])
        assert not matches_any_pattern("/etc/a", [".*\\.log$"])
        assert not matches_any_pattern("/etc/a", ["[invalid"])


# ======================================================================
# Storage
# ======================================================================

class TestStorage:
    def test_init(self, storage):
        assert storage.is_initialized
        for d in ("baselines", "patches", "templates", "cache", "staging"):
            assert (storage.delta_dir / d).is_dir()

    def test_config(self, storage):
        assert storage.load_config().ssh.host == "10.0.0.1"

    def test_state(self, storage):
        storage.save_state(DeltaState(active="x"))
        assert storage.load_state().active == "x"

    def test_baseline_crud(self, storage):
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        assert storage.name_exists("bl")
        assert storage.get_entity_type("bl") == EntityType.BASELINE
        assert storage.load_baseline("bl").name == "bl"

    def test_patch_crud(self, storage):
        storage.save_patch(PatchMetadata(name="p", baseline="bl"))
        assert storage.get_entity_type("p") == EntityType.PATCH

    def test_scan_cache(self, storage):
        storage.save_scan(ScanResult(host="h", reference="bl", files=[FileInfo(path="/a")]))
        s = storage.load_scan()
        assert s and s.file_count == 1 and s.reference == "bl"

    def test_staging(self, storage):
        storage.save_staging(StagingManifest(reference="bl", modified=["/a"]))
        assert storage.load_staging().total_files == 1
        storage.clear_staging()
        assert storage.load_staging().is_empty

    def test_template_crud(self, storage):
        storage.save_template("t", Template(description="d"))
        assert "t" in storage.list_templates()
        storage.remove_template("t")
        assert "t" not in storage.list_templates()

    def test_copy_baseline(self, storage):
        storage.save_baseline(BaselineMetadata(name="a", tracked_paths=["/etc"]))
        storage.copy_baseline("a", "b")
        assert storage.load_baseline("b").name == "b"

    def test_copy_patch(self, storage):
        storage.save_patch(PatchMetadata(name="p1", baseline="bl"))
        storage.copy_patch("p1", "p2")
        assert storage.load_patch("p2").name == "p2"


# ======================================================================
# Staging ops
# ======================================================================

class TestStagingOps:
    def test_add_all(self, storage):
        from delta.staging_ops import stage_add
        dr = DiffResult("bl", EntityType.BASELINE,
                         modified=[FileInfo(path="/a")], created=[FileInfo(path="/b")],
                         deleted=["/c"])
        m = stage_add(storage, dr, None)
        assert len(m.modified) == 1 and len(m.created) == 1 and len(m.deleted) == 1

    def test_add_specific(self, storage):
        from delta.staging_ops import stage_add
        dr = DiffResult("bl", EntityType.BASELINE,
                         modified=[FileInfo(path="/etc/a"), FileInfo(path="/opt/b")])
        m = stage_add(storage, dr, ["/etc/a"])
        assert m.modified == ["/etc/a"]

    def test_add_prefix(self, storage):
        from delta.staging_ops import stage_add
        dr = DiffResult("bl", EntityType.BASELINE,
                         modified=[FileInfo(path="/etc/a"), FileInfo(path="/etc/b"),
                                   FileInfo(path="/opt/c")])
        m = stage_add(storage, dr, ["/etc/"])
        assert sorted(m.modified) == ["/etc/a", "/etc/b"]

    def test_add_idempotent(self, storage):
        from delta.staging_ops import stage_add
        dr = DiffResult("bl", EntityType.BASELINE, modified=[FileInfo(path="/a")])
        stage_add(storage, dr, None)
        m = stage_add(storage, dr, None)
        assert m.modified == ["/a"]

    def test_remove(self, storage):
        from delta.staging_ops import stage_add, stage_remove
        dr = DiffResult("bl", EntityType.BASELINE,
                         modified=[FileInfo(path="/a"), FileInfo(path="/b")])
        stage_add(storage, dr, None)
        m = stage_remove(storage, ["/a"])
        assert m.modified == ["/b"]

    def test_commit(self, storage):
        from delta.staging_ops import commit_to_patch, create_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        create_patch(storage, "p1")

        storage.save_staging(StagingManifest(reference="bl", modified=["/etc/a"], deleted=["/etc/old"]))
        staged = storage.get_staging_file("/etc/a")
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_text("content")

        meta = commit_to_patch(storage, "p1")
        assert meta.baseline == "bl"
        assert "/etc/a" in meta.modified_files
        assert "/etc/old" in meta.deleted_files
        assert storage.load_staging().is_empty

    def test_commit_empty(self, storage):
        from delta.staging_ops import commit_to_patch, create_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        create_patch(storage, "p1")
        with pytest.raises(ValueError):
            commit_to_patch(storage, "p1")

    def test_commit_merge(self, storage):
        """Second commit merges into existing patch, not overwrites."""
        from delta.staging_ops import commit_to_patch, create_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        create_patch(storage, "p1")

        # First commit
        storage.save_staging(StagingManifest(reference="bl", modified=["/etc/a"]))
        storage.get_staging_file("/etc/a").parent.mkdir(parents=True, exist_ok=True)
        storage.get_staging_file("/etc/a").write_text("v1")
        commit_to_patch(storage, "p1")

        # Second commit — adds /etc/b, keeps /etc/a
        storage.save_staging(StagingManifest(reference="bl", modified=["/etc/b"]))
        storage.get_staging_file("/etc/b").parent.mkdir(parents=True, exist_ok=True)
        storage.get_staging_file("/etc/b").write_text("v2")
        meta = commit_to_patch(storage, "p1")

        assert "/etc/a" in meta.modified_files  # Still there
        assert "/etc/b" in meta.modified_files  # Added
        assert meta.created_at != meta.updated_at


# ======================================================================
# Diff ops
# ======================================================================

class TestDiffOps:
    def test_compare_entities(self, storage):
        from delta.diff_ops import compare_entities
        for name in ("a", "b"):
            d = storage.baseline_files_dir(name)
            (d / "etc").mkdir(parents=True)
        (storage.baseline_files_dir("a") / "etc" / "f1").write_text("v1")
        (storage.baseline_files_dir("b") / "etc" / "f1").write_text("v2")
        (storage.baseline_files_dir("b") / "etc" / "f2").write_text("new")
        storage.save_baseline(BaselineMetadata(name="a", tracked_paths=["/etc"]))
        storage.save_baseline(BaselineMetadata(name="b", tracked_paths=["/etc"]))
        r = compare_entities(storage, "a", EntityType.BASELINE, "b", EntityType.BASELINE)
        assert len(r.modified) == 1 and len(r.created) == 1

    def test_ignore_patterns_from_entity(self, storage):
        """Ignore patterns come from entity metadata (self-contained)."""
        from delta.diff_ops import collect_ignore_patterns
        storage.save_baseline(BaselineMetadata(
            name="bl", tracked_paths=["/etc"],
            ignore_patterns=[".*\\.log$", ".*\\.tmp$"]))
        patterns = collect_ignore_patterns(storage, "bl", EntityType.BASELINE)
        assert patterns == [".*\\.log$", ".*\\.tmp$"]


# ======================================================================
# CLI smoke tests
# ======================================================================

class TestCLI:
    def test_help(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["--help"])
        assert r.exit_code == 0
        for cmd in ("init", "fetch", "diff", "apply", "checkout", "use", "status",
                    "add", "commit", "reset", "edit", "patch", "baseline"):
            assert cmd in r.output

    def test_fetch_help(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["fetch", "--help"])
        assert "--scan" in r.output
        assert "--detailed" in r.output

    def test_diff_help(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["diff", "--help"])
        assert "--fetch" in r.output
        assert "--full" in r.output
        assert "PATHS" in r.output

    def test_patch_subcommands(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["patch", "--help"])
        for sub in ("create", "info", "rm"):
            assert sub in r.output

    def test_use(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            r = CliRunner().invoke(main, ["use", "bl"])
            assert r.exit_code == 0
            assert s.load_state().active == "bl"

    def test_status(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            r = CliRunner().invoke(main, ["status"])
            assert r.exit_code == 0

    def test_diff_no_scan(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            r = CliRunner().invoke(main, ["diff"])
            assert r.exit_code == 1  # No scan

    def test_config_set_get(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            CliRunner().invoke(main, ["config", "set", "ssh.host", "1.2.3.4"])
            assert s.load_config().ssh.host == "1.2.3.4"
            CliRunner().invoke(main, ["config", "set", "transfer.compress", "true"])
            assert s.load_config().transfer.compress is True

    def test_checkout_help(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["checkout", "--help"])
        assert "--from" in r.output and "--dry-run" in r.output

    def test_checkout_dry_run(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            d = s.baseline_files_dir("bl")
            (d / "etc").mkdir(parents=True)
            (d / "etc" / "cfg").write_text("orig")
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            r = CliRunner().invoke(main, ["checkout", "/etc/cfg", "--dry-run"])
            assert r.exit_code == 0

    def test_add_requires_args(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["add"])
        assert r.exit_code == 2  # Missing required arg


# ======================================================================
# UI formatting
# ======================================================================

class TestUI:
    def test_format_time_ago(self):
        from datetime import datetime, timedelta
        assert ui.format_time_ago(datetime.now().isoformat()) == "just now"
        assert "5 minutes" in ui.format_time_ago((datetime.now() - timedelta(minutes=5)).isoformat())
        assert "2024" in ui.format_time_ago("2024-01-15T10:30:00")
        assert ui.format_time_ago("bad") == "bad"

    def test_format_size(self):
        assert "KB" in ui.format_size(2048)
        assert "MB" in ui.format_size(2 * 1024 * 1024)

    def test_format_duration(self):
        assert ui.format_duration(65) == "1:05"
        assert ui.format_duration(3661) == "1:01:01"


# ======================================================================
# Cache validation
# ======================================================================

class TestCacheValidation:
    def test_validate_host_mismatch(self, tmp_path, capsys):
        from delta.cli import _validate_cache
        scan = ScanResult(host="10.0.0.1", reference="bl")
        config = DeltaConfig(ssh=SSHConfig(host="10.0.0.2"))
        _validate_cache(scan, config, "bl")
        # Should print warning (through ui module)

    def test_validate_reference_mismatch(self, tmp_path, capsys):
        from delta.cli import _validate_cache
        scan = ScanResult(host="h", reference="factory")
        config = DeltaConfig(ssh=SSHConfig(host="h"))
        _validate_cache(scan, config, "wifi")

    def test_validate_ok(self, tmp_path, capsys):
        from delta.cli import _validate_cache
        scan = ScanResult(host="h", reference="bl")
        config = DeltaConfig(ssh=SSHConfig(host="h"))
        _validate_cache(scan, config, "bl")


# ======================================================================
# Helpers
# ======================================================================

class TestHelpers:
    def test_path_matches(self):
        from delta.diff_ops import _path_matches
        assert _path_matches("/etc/cfg", ["/etc/cfg"])
        assert _path_matches("/etc/cfg", ["/etc/"])
        assert _path_matches("/etc/sub/f", ["/etc"])
        assert not _path_matches("/opt/f", ["/etc/"])

    def test_parse_value(self):
        from delta.cli import _parse_value
        assert _parse_value("true") is True
        assert _parse_value("22") == 22
        assert _parse_value("vim") == "vim"

    def test_dict_get_set(self):
        from delta.cli import _dict_get, _dict_set
        d = {"ssh": {"host": "old"}}
        assert _dict_get(d, "ssh.host") == "old"
        assert _dict_set(d, "ssh.host", "new")
        assert d["ssh"]["host"] == "new"
        assert _dict_get(d, "bad.key") is None


# ======================================================================
# Edge cases
# ======================================================================

class TestEdgeCases:
    def test_empty_scan(self, storage):
        assert storage.load_scan() is None

    def test_clear_staging_idempotent(self, storage):
        storage.clear_staging()
        storage.clear_staging()
        assert storage.load_staging().is_empty

    def test_staging_remove_not_found(self, storage):
        from delta.staging_ops import stage_remove
        m = stage_remove(storage, ["/nonexistent"])
        assert m.is_empty

    def test_log_manager_interrupted(self, tmp_path):
        from delta.log_manager import LogManager
        lm = LogManager(logs_dir=tmp_path, filename_pattern="{datetime}_{command}_{result}",
                         max_count=50, max_size_mb=100)
        lm.start("test", log_to_file=True)
        lm.finish_interrupted()
        assert lm._log_path.name.endswith("_interrupted.log")

    def test_ownership(self):
        from delta.ownership import compute_ownership, get_file_ownership
        files = [FileInfo(path="/a", owner="root", group="root", mode="644"),
                 FileInfo(path="/b", owner="app", group="app", mode="755")]
        od = compute_ownership(files)
        o, g, m = get_file_ownership("/b", od)
        assert o == "app"
        o2, _, _ = get_file_ownership("/x", od)
        assert o2 == od.default_owner

    def test_remote_cmd(self):
        from delta.remote_cmd import parse_var_args, validate_variables, substitute_variables
        assert parse_var_args(("A=1",)) == {"A": "1"}
        assert validate_variables([VariableSpec(name="A")], {"A": "v"})["A"] == "v"
        assert substitute_variables("echo ${X}", {"X": "hi"}) == "echo hi"
        # $${} escape for bash variables
        assert substitute_variables("echo $${HOME}", {"HOME": "override"}) == "echo ${HOME}"
        # Mixed: delta var + bash var
        assert substitute_variables("echo ${X} $${HOME}", {"X": "val"}) == "echo val ${HOME}"


# ======================================================================
# --show-config tests
# ======================================================================

class TestShowConfig:
    def test_resolve_config_baseline(self, storage):
        from delta.diff_ops import resolve_config
        storage.save_baseline(BaselineMetadata(
            name="bl", tracked_paths=["/etc"],
            ignore_patterns=[".*\\.log$"],
            on_fetch=CommandBlock(pre=[CommandSpec(cmd="echo pre")]),
        ))
        rc = resolve_config(storage, "fetch", "bl", EntityType.BASELINE)
        assert rc.reference == "bl"
        assert rc.tracked_paths == ["/etc"]
        assert any("log" in tv.value for tv in rc.ignore_patterns)
        assert any("echo pre" in tv.value for tv in rc.on_fetch_pre)

    def test_resolve_config_patch(self, storage):
        from delta.diff_ops import resolve_config
        storage.save_baseline(BaselineMetadata(
            name="bl", tracked_paths=["/etc"],
            ignore_patterns=[".*\\.log$"],
        ))
        storage.save_patch(PatchMetadata(
            name="p1", baseline="bl",
            ignore_patterns=[".*\\.tmp$"],
            on_apply=CommandBlock(pre=[CommandSpec(cmd="stop")]),
        ))
        rc = resolve_config(storage, "apply p1", "p1", EntityType.PATCH)
        # Both baseline and patch ignore patterns
        sources = [tv.source for tv in rc.ignore_patterns]
        assert any("baseline" in s for s in sources)
        assert any("patch" in s for s in sources)
        assert any("stop" in tv.value for tv in rc.on_apply_pre)

    def test_resolve_config_with_entity_ignore(self, storage):
        from delta.diff_ops import resolve_config
        storage.save_baseline(BaselineMetadata(
            name="bl", tracked_paths=["/etc"],
            ignore_patterns=[".*\\.cache$"]))
        rc = resolve_config(storage, "diff", "bl", EntityType.BASELINE)
        assert any("baseline" in tv.source for tv in rc.ignore_patterns)

    def test_resolve_config_extra_ignore(self, storage):
        from delta.diff_ops import resolve_config
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        rc = resolve_config(storage, "diff", "bl", EntityType.BASELINE,
                            extra_ignore=[".*\\.bak$"])
        assert any("CLI" in tv.source for tv in rc.ignore_patterns)

    def test_display_format(self, storage):
        from delta.diff_ops import resolve_config
        from delta.models import DeltaConfig, SSHConfig
        storage.save_baseline(BaselineMetadata(
            name="bl", tracked_paths=["/etc"],
            description="Factory baseline",
            ignore_patterns=[".*\\.log$"],
        ))
        config = DeltaConfig(ssh=SSHConfig(host="10.0.0.1"))
        rc = resolve_config(storage, "fetch", "bl", EntityType.BASELINE, config=config)
        text = rc.display()
        assert "fetch" in text
        assert "10.0.0.1" in text
        assert "baseline" in text
        assert ".log" in text

    def test_show_config_flag_help(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["--help"])
        assert "--show-config" in r.output

    def test_show_config_on_diff(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="10.0.0.1")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            r = CliRunner().invoke(main, ["--show-config", "diff"])
            assert r.exit_code == 0


# ======================================================================
# delta schema tests
# ======================================================================

class TestSchema:
    def test_schema_patch(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["schema", "patch"])
        assert r.exit_code == 0
        assert "on_apply" in r.output
        assert "on_fetch" in r.output
        assert "variables" in r.output
        assert "cmd" in r.output

    def test_schema_baseline(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["schema", "baseline"])
        assert r.exit_code == 0
        assert "tracked_paths" in r.output
        assert "on_fetch" in r.output
        assert "on_apply" not in r.output  # baselines don't have on_apply

    def test_schema_config(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["schema", "config"])
        assert r.exit_code == 0
        assert "ssh" in r.output
        assert "transfer" in r.output
        assert "editor" in r.output

    def test_schema_template(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["schema", "template"])
        assert r.exit_code == 0
        assert "on_fetch" in r.output
        assert "on_apply" in r.output

    def test_schema_config_defaults(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["schema", "config"])
        assert r.exit_code == 0
        assert "defaults" in r.output


# ======================================================================
# delta edit --scaffold tests
# ======================================================================

class TestScaffold:
    def test_inject_scaffold_patch(self, storage):
        from delta.cli import _inject_scaffold
        storage.save_patch(PatchMetadata(name="p1", baseline="bl"))
        path = storage._patch_dir("p1") / "metadata.yaml"
        original = path.read_text()
        _inject_scaffold(path, "patch")
        content = path.read_text()
        assert "Available fields" in content
        assert "on_apply" in content
        assert original in content  # Original preserved

    def test_inject_scaffold_idempotent(self, storage):
        from delta.cli import _inject_scaffold
        storage.save_patch(PatchMetadata(name="p1", baseline="bl"))
        path = storage._patch_dir("p1") / "metadata.yaml"
        _inject_scaffold(path, "patch")
        content1 = path.read_text()
        _inject_scaffold(path, "patch")  # Again
        content2 = path.read_text()
        assert content1 == content2  # No duplication

    def test_inject_scaffold_baseline(self, storage):
        from delta.cli import _inject_scaffold
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        path = storage._baseline_dir("bl") / "metadata.yaml"
        _inject_scaffold(path, "baseline")
        content = path.read_text()
        assert "on_fetch" in content
        assert "ownership" in content


# ======================================================================
# patch create tests
# ======================================================================

class TestPatchCreate:
    def test_create_from_baseline(self, storage):
        from delta.staging_ops import create_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        meta = create_patch(storage, "p1")
        assert meta.baseline == "bl"
        assert meta.modified_files == []
        assert meta.created_files == []

    def test_create_from_patch(self, storage):
        from delta.staging_ops import create_patch, commit_to_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        create_patch(storage, "p1", description="Original")
        # Add a file to p1
        storage.save_staging(StagingManifest(reference="bl", modified=["/etc/a"]))
        storage.get_staging_file("/etc/a").parent.mkdir(parents=True, exist_ok=True)
        storage.get_staging_file("/etc/a").write_text("content")
        commit_to_patch(storage, "p1")

        # Create p2 from p1
        meta = create_patch(storage, "p2", from_entity="p1")
        assert meta.baseline == "bl"
        assert "/etc/a" in meta.modified_files
        assert meta.description == "Original"
        # Verify file was copied
        assert storage.get_patch_file("p2", "/etc/a").exists()

    def test_create_explicit_from_baseline(self, storage):
        from delta.staging_ops import create_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        meta = create_patch(storage, "p1", from_entity="bl")
        assert meta.baseline == "bl"

    def test_create_with_description(self, storage):
        from delta.staging_ops import create_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        meta = create_patch(storage, "p1", description="WiFi config")
        assert meta.description == "WiFi config"

    def test_create_no_active(self, storage):
        from delta.staging_ops import create_patch
        with pytest.raises(ValueError, match="No active"):
            create_patch(storage, "p1")

    def test_create_cli(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            r = CliRunner().invoke(main, ["patch", "create", "wifi", "-m", "Test"])
            assert r.exit_code == 0
            assert s.load_state().active == "wifi"
            assert s.load_patch("wifi").description == "Test"

    def test_create_from_cli(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            CliRunner().invoke(main, ["patch", "create", "p1"])
            r = CliRunner().invoke(main, ["patch", "create", "p2", "--from", "p1"])
            assert r.exit_code == 0
            assert s.load_state().active == "p2"

    def test_create_duplicate_name(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            CliRunner().invoke(main, ["patch", "create", "wifi"])
            r = CliRunner().invoke(main, ["patch", "create", "wifi"])
            assert r.exit_code == 1  # Already exists


class TestPatchCommitCLI:
    def test_commit_no_active_patch(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            r = CliRunner().invoke(main, ["commit"])
            assert r.exit_code == 1  # Active is baseline, not patch

    def test_commit_help(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["commit", "--help"])
        assert "commit" in r.output.lower()


class TestVariableValidation:
    def test_check_undefined(self):
        from delta.remote_cmd import check_undefined_variables
        cmds = [CommandSpec(cmd="echo ${A} ${B}")]
        assert check_undefined_variables(cmds, {"A"}) == ["B"]

    def test_all_defined(self):
        from delta.remote_cmd import check_undefined_variables
        cmds = [CommandSpec(cmd="echo ${A} ${B}")]
        assert check_undefined_variables(cmds, {"A", "B"}) == []

    def test_escaped_ignored(self):
        from delta.remote_cmd import check_undefined_variables
        cmds = [CommandSpec(cmd="echo $${HOME} ${A}")]
        assert check_undefined_variables(cmds, {"A"}) == []

    def test_no_vars(self):
        from delta.remote_cmd import check_undefined_variables
        cmds = [CommandSpec(cmd="echo hello")]
        assert check_undefined_variables(cmds, set()) == []


class TestCreatePatchInheritance:
    def test_inherit_from_baseline(self, storage):
        from delta.staging_ops import create_patch
        storage.save_baseline(BaselineMetadata(
            name="bl", tracked_paths=["/etc"],
            ignore_patterns=[".*\\.log$"],
            variables=[VariableSpec(name="DEV", required=True)],
            on_fetch=CommandBlock(pre=[CommandSpec(cmd="echo pre")]),
            command_outputs={"kernel": "5.15"},
        ))
        storage.save_state(DeltaState(active="bl"))
        meta = create_patch(storage, "p1")
        assert meta.ignore_patterns == [".*\\.log$"]
        assert len(meta.variables) == 1
        assert meta.variables[0].name == "DEV"
        assert meta.command_outputs == {"kernel": "5.15"}
        assert len(meta.on_fetch.pre) == 1


class TestExportImport:
    def test_export(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path) as td:
            s = Storage(root=Path(td))
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            from delta.staging_ops import create_patch, commit_to_patch
            create_patch(s, "wifi", description="WiFi config")
            s.save_staging(StagingManifest(reference="bl", modified=["/etc/a"]))
            s.get_staging_file("/etc/a").parent.mkdir(parents=True, exist_ok=True)
            s.get_staging_file("/etc/a").write_text("content")
            commit_to_patch(s, "wifi")

            out = Path(td) / "out"
            out.mkdir()
            r = CliRunner().invoke(main, ["export", "wifi", "-o", str(out)])
            assert r.exit_code == 0
            archive = out / "wifi.tar.gz"
            assert archive.exists()
            assert archive.stat().st_size > 0

    def test_import(self, tmp_path):
        import tarfile
        from click.testing import CliRunner
        from delta.cli import main

        # Create a fake patch archive
        build = tmp_path / "build"
        build.mkdir()
        patch_dir = build / "test-patch"
        patch_dir.mkdir()
        files_dir = patch_dir / "files" / "etc"
        files_dir.mkdir(parents=True)
        (files_dir / "config").write_text("data")
        meta = PatchMetadata(name="test-patch", baseline="bl", description="Test",
                             modified_files=["/etc/config"])
        (patch_dir / "metadata.yaml").write_text(yaml.dump(meta.to_dict()))
        archive = build / "test-patch.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for f in patch_dir.rglob("*"):
                tar.add(f, arcname=f"test-patch/{f.relative_to(patch_dir)}")

        with CliRunner().isolated_filesystem(temp_dir=tmp_path) as td:
            s = Storage(root=Path(td))
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            r = CliRunner().invoke(main, ["import", str(archive)])
            assert r.exit_code == 0
            loaded = s.load_patch("test-patch")
            assert loaded.baseline == "bl"
            assert "/etc/config" in loaded.modified_files

    def test_import_conflict(self, tmp_path):
        import tarfile
        from click.testing import CliRunner
        from delta.cli import main

        build = tmp_path / "build"
        build.mkdir()
        patch_dir = build / "p1"
        patch_dir.mkdir()
        meta = PatchMetadata(name="p1", baseline="bl")
        (patch_dir / "metadata.yaml").write_text(yaml.dump(meta.to_dict()))
        archive = build / "p1.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for f in patch_dir.rglob("*"):
                tar.add(f, arcname=f"p1/{f.relative_to(patch_dir)}")

        with CliRunner().isolated_filesystem(temp_dir=tmp_path) as td:
            s = Storage(root=Path(td))
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_patch(PatchMetadata(name="p1", baseline="bl"))
            r = CliRunner().invoke(main, ["import", str(archive), "--yes"])
            assert r.exit_code == 0


class TestConfigWithoutInit:
    def test_config_set_creates_delta(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            r = CliRunner().invoke(main, ["config", "set", "ssh.host", "10.0.0.1"])
            assert r.exit_code == 0
            assert Path(".delta/config.yaml").exists()

    def test_config_show_creates_delta(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            r = CliRunner().invoke(main, ["config", "show"])
            assert r.exit_code == 0
            assert Path(".delta/config.yaml").exists()

    def test_log_enabled_default(self):
        c = DeltaConfig()
        assert c.log_enabled is True
        d = c.to_dict()
        assert d["log"]["enabled"] is True

    def test_log_disabled(self):
        c = DeltaConfig(log_enabled=False)
        d = c.to_dict()
        assert d["log"]["enabled"] is False
        c2 = DeltaConfig.from_dict(d)
        assert c2.log_enabled is False

    def test_config_set_log_enabled(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            CliRunner().invoke(main, ["config", "set", "log.enabled", "false"])
            s = Storage(root=Path("."))
            c = s.load_config()
            assert c.log_enabled is False


class TestPatchAddModes:
    def test_add_local_file(self, storage):
        from delta.staging_ops import stage_add_local
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        bf = storage.baseline_files_dir("bl") / "etc" / "existing"
        bf.parent.mkdir(parents=True, exist_ok=True)
        bf.write_text("old")
        storage.save_state(DeltaState(active="bl"))

        # Add local file to existing path (modify)
        local = storage.delta_dir.parent / "myfile.txt"
        local.write_text("new content")
        stage_add_local(storage, "/etc/existing", local, "bl", EntityType.BASELINE)
        m = storage.load_staging()
        assert "/etc/existing" in m.modified

    def test_add_local_file_new(self, storage):
        from delta.staging_ops import stage_add_local
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))

        local = storage.delta_dir.parent / "newfile.txt"
        local.write_text("brand new")
        stage_add_local(storage, "/etc/brand-new.conf", local, "bl", EntityType.BASELINE)
        m = storage.load_staging()
        assert "/etc/brand-new.conf" in m.created

    def test_add_delete(self, storage):
        from delta.staging_ops import stage_add_delete
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))

        stage_add_delete(storage, ["/etc/old.conf", "/etc/deprecated.conf"])
        m = storage.load_staging()
        assert "/etc/old.conf" in m.deleted
        assert "/etc/deprecated.conf" in m.deleted

    def test_add_force(self, storage):
        from delta.staging_ops import stage_add_force
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        bf = storage.baseline_files_dir("bl") / "etc" / "config"
        bf.parent.mkdir(parents=True, exist_ok=True)
        bf.write_text("baseline content")

        stage_add_force(storage, ["/etc/config"], "bl", EntityType.BASELINE)
        m = storage.load_staging()
        assert "/etc/config" in m.modified
        staged = storage.get_staging_file("/etc/config")
        assert staged.read_text() == "baseline content"

    def test_add_force_missing(self, storage):
        from delta.staging_ops import stage_add_force
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        stage_add_force(storage, ["/etc/nonexistent"], "bl", EntityType.BASELINE)
        m = storage.load_staging()
        assert m.is_empty  # Not found, not added

    def test_import_multiple(self, tmp_path):
        import tarfile
        from click.testing import CliRunner
        from delta.cli import main

        build = tmp_path / "build"
        build.mkdir()
        # Create two archives
        for name in ("p1", "p2"):
            pd = build / name
            pd.mkdir()
            meta = PatchMetadata(name=name, baseline="bl")
            (pd / "metadata.yaml").write_text(yaml.dump(meta.to_dict()))
            archive = build / f"{name}.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                for f in pd.rglob("*"):
                    tar.add(f, arcname=f"{name}/{f.relative_to(pd)}")

        with CliRunner().isolated_filesystem(temp_dir=tmp_path) as td:
            s = Storage(root=Path(td))
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            r = CliRunner().invoke(main, ["import",
                                          str(build / "p1.tar.gz"),
                                          str(build / "p2.tar.gz")])
            assert r.exit_code == 0
            assert s.load_patch("p1").baseline == "bl"
            assert s.load_patch("p2").baseline == "bl"


class TestNewFeatures:
    def test_patch_hash(self, storage):
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_patch(PatchMetadata(name="p1", baseline="bl"))
        h1 = storage.compute_patch_hash("p1")
        assert len(h1) == 12

        # Hash changes when files change
        pf = storage.patch_files_dir("p1") / "etc" / "a"
        pf.parent.mkdir(parents=True, exist_ok=True)
        pf.write_text("content")
        h2 = storage.compute_patch_hash("p1")
        assert h2 != h1

    def test_patch_list_bare(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            s.save_patch(PatchMetadata(name="p1", baseline="bl"))
            r = CliRunner().invoke(main, ["patch"])
            assert r.exit_code == 0

    def test_baseline_list_bare(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            r = CliRunner().invoke(main, ["baseline"])
            assert r.exit_code == 0

    def test_dir_flag(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        s = Storage(root=tmp_path)
        s.init(DeltaConfig(ssh=SSHConfig(host="h")))
        s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        s.save_state(DeltaState(active="bl"))
        # Run from a different directory
        r = CliRunner().invoke(main, ["-C", str(tmp_path), "baseline"])
        assert r.exit_code == 0

    def test_config_defaults(self):
        from delta.models import ConfigDefaults
        c = DeltaConfig(
            defaults=ConfigDefaults(
                on_fetch=CommandBlock(pre=[CommandSpec(cmd="echo fetch")]),
                on_apply=CommandBlock(post=[CommandSpec(cmd="echo apply")]),
                ignore_patterns=[".*\\.log$"],
            ),
        )
        d = c.to_dict()
        assert "defaults" in d
        assert "on_fetch" in d["defaults"]
        assert "on_apply" in d["defaults"]
        c2 = DeltaConfig.from_dict(d)
        assert len(c2.defaults.on_fetch.pre) == 1
        assert len(c2.defaults.on_apply.post) == 1
        assert c2.defaults.ignore_patterns == [".*\\.log$"]

    def test_config_defaults_backward_compat(self):
        """Old top-level on_fetch/on_apply migrates to defaults."""
        d = {"on_fetch": {"pre": [{"cmd": "echo old"}]}, "ssh": {"host": "h"}}
        c = DeltaConfig.from_dict(d)
        assert len(c.defaults.on_fetch.pre) == 1

    def test_config_inherits_to_patch(self, storage):
        from delta.staging_ops import create_patch
        from delta.models import ConfigDefaults
        config = DeltaConfig(
            ssh=SSHConfig(host="h"),
            defaults=ConfigDefaults(
                ignore_patterns=[".*\\.tmp$"],
            ),
        )
        storage.save_config(config)
        storage.save_baseline(BaselineMetadata(
            name="bl", tracked_paths=["/etc"],
            ignore_patterns=[".*\\.tmp$"],  # Already inherited at baseline create
        ))
        storage.save_state(DeltaState(active="bl"))
        meta = create_patch(storage, "p1")
        assert ".*\\.tmp$" in meta.ignore_patterns

    def test_cache_clean(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            # Create some cache
            cf = s.cache_files_dir / "etc" / "a"
            cf.parent.mkdir(parents=True, exist_ok=True)
            cf.write_text("cached")
            r = CliRunner().invoke(main, ["cache", "clean", "--yes"])
            assert r.exit_code == 0
            assert not cf.exists()


class TestPathPatterns:
    def test_exact_match(self):
        from delta.staging_ops import _matches_paths
        assert _matches_paths("/etc/config.conf", ["/etc/config.conf"])
        assert not _matches_paths("/etc/other.conf", ["/etc/config.conf"])

    def test_directory_prefix(self):
        from delta.staging_ops import _matches_paths
        assert _matches_paths("/etc/config.conf", ["/etc/"])
        assert _matches_paths("/etc/sub/file", ["/etc/"])
        assert _matches_paths("/etc/config.conf", ["/etc"])  # without trailing /
        assert not _matches_paths("/opt/file", ["/etc/"])

    def test_glob_star(self):
        from delta.staging_ops import _matches_paths
        assert _matches_paths("/etc/config.conf", ["/etc/*.conf"])
        assert _matches_paths("/etc/app.conf", ["/etc/*.conf"])
        assert not _matches_paths("/etc/config.json", ["/etc/*.conf"])
        assert not _matches_paths("/etc/sub/config.conf", ["/etc/*.conf"])  # single * doesn't cross /

    def test_glob_recursive(self):
        from delta.staging_ops import _matches_paths
        assert _matches_paths("/var/log/app.log", ["/var/**/*.log"])
        assert _matches_paths("/var/a/b/app.log", ["/var/**/*.log"])
        assert not _matches_paths("/var/log/app.json", ["/var/**/*.log"])

    def test_question_mark(self):
        from delta.staging_ops import _matches_paths
        assert _matches_paths("/etc/a.conf", ["/etc/?.conf"])
        assert not _matches_paths("/etc/ab.conf", ["/etc/?.conf"])

    def test_multiple_patterns(self):
        from delta.staging_ops import _matches_paths
        assert _matches_paths("/etc/a.conf", ["/etc/*.conf", "/var/*.log"])
        assert _matches_paths("/var/x.log", ["/etc/*.conf", "/var/*.log"])
        assert not _matches_paths("/opt/x", ["/etc/*.conf", "/var/*.log"])

    def test_stage_remove_pattern(self, storage):
        from delta.staging_ops import stage_remove
        storage.save_staging(StagingManifest(
            reference="bl",
            modified=["/etc/a.conf", "/etc/b.conf", "/etc/c.json"],
        ))
        # Create staged files
        for name in ("a.conf", "b.conf", "c.json"):
            f = storage.get_staging_file(f"/etc/{name}")
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text("x")

        stage_remove(storage, ["/etc/*.conf"])
        m = storage.load_staging()
        assert "/etc/a.conf" not in m.modified
        assert "/etc/b.conf" not in m.modified
        assert "/etc/c.json" in m.modified  # .json not affected


class TestStatusCompression:
    def test_print_compressed_many_files(self, capsys):
        from delta.cli import _print_compressed
        # 5 files in same dir → should collapse
        paths = [f"/etc/conf/a{i}.conf" for i in range(5)]
        _print_compressed(paths, "M", threshold=3)
        captured = capsys.readouterr()
        assert "/etc/conf/" in captured.err or "/etc/conf/" in captured.out
        assert "5 files" in (captured.err + captured.out)

    def test_print_compressed_few_files(self, capsys):
        from delta.cli import _print_compressed
        # 2 files → show individually
        paths = ["/etc/a.conf", "/etc/b.conf"]
        _print_compressed(paths, "M", threshold=3)
        out = capsys.readouterr().err + capsys.readouterr().out
        # Neither "2 files" nor collapse
        assert "2 files" not in out

    def test_diff_staged_flag(self):
        from click.testing import CliRunner
        from delta.cli import main
        r = CliRunner().invoke(main, ["diff", "--help"])
        assert "--staged" in r.output or "--cached" in r.output

    def test_diff_staged_empty(self, tmp_path):
        from click.testing import CliRunner
        from delta.cli import main
        with CliRunner().isolated_filesystem(temp_dir=tmp_path):
            s = Storage(root=Path.cwd())
            s.init(DeltaConfig(ssh=SSHConfig(host="h")))
            s.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
            s.save_state(DeltaState(active="bl"))
            r = CliRunner().invoke(main, ["diff", "--staged"])
            assert r.exit_code == 0


class TestStagingManifestSources:
    def test_stage_add_marks_cache(self, storage):
        from delta.staging_ops import stage_add
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        dr = DiffResult(reference_name="bl", reference_type=EntityType.BASELINE,
                        modified=[FileInfo(path="/etc/a", md5="abc")])
        stage_add(storage, dr)
        m = storage.load_staging()
        assert m.sources.get("/etc/a") == "cache"

    def test_stage_add_local_marks_local(self, storage):
        from delta.staging_ops import stage_add_local
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        local = storage.delta_dir.parent / "myfile"
        local.write_text("data")
        stage_add_local(storage, "/etc/new", local, "bl", EntityType.BASELINE)
        m = storage.load_staging()
        assert m.sources.get("/etc/new") == "local"

    def test_resolve_cache(self, storage):
        m = StagingManifest(reference="bl", modified=["/etc/a"], sources={"/etc/a": "cache"})
        cf = storage.cache_files_dir / "etc" / "a"
        cf.parent.mkdir(parents=True, exist_ok=True)
        cf.write_text("cached")
        resolved = storage.resolve_staged_file(m, "/etc/a")
        assert resolved == cf

    def test_resolve_local(self, storage):
        m = StagingManifest(reference="bl", modified=["/etc/a"], sources={"/etc/a": "local"})
        sf = storage.get_staging_file("/etc/a")
        sf.parent.mkdir(parents=True, exist_ok=True)
        sf.write_text("local")
        resolved = storage.resolve_staged_file(m, "/etc/a")
        assert resolved == sf

    def test_resolve_fallback(self, storage):
        m = StagingManifest(reference="bl", modified=["/etc/a"])  # No sources
        cf = storage.cache_files_dir / "etc" / "a"
        cf.parent.mkdir(parents=True, exist_ok=True)
        cf.write_text("cached")
        resolved = storage.resolve_staged_file(m, "/etc/a")
        assert resolved == cf

    def test_commit_updated_count(self, storage):
        """Re-committing same file shows 'updated', not '0 new'."""
        from delta.staging_ops import create_patch, commit_to_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        create_patch(storage, "p1")

        # First commit
        storage.save_staging(StagingManifest(reference="bl", modified=["/etc/a"]))
        cf = storage.cache_files_dir / "etc" / "a"
        cf.parent.mkdir(parents=True, exist_ok=True)
        cf.write_text("v1")
        commit_to_patch(storage, "p1")

        # Second commit — same file, updated
        storage.save_staging(StagingManifest(reference="bl", modified=["/etc/a"],
                                            sources={"/etc/a": "cache"}))
        cf.write_text("v2")
        meta = commit_to_patch(storage, "p1")
        assert "/etc/a" in meta.modified_files
        # File content should be v2
        pf = storage.get_patch_file("p1", "/etc/a")
        assert pf.read_text() == "v2"

    def test_no_file_duplication(self, storage):
        """stage_add should NOT copy files from cache to staging."""
        from delta.staging_ops import stage_add
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        # Put file in cache
        cf = storage.cache_files_dir / "etc" / "a"
        cf.parent.mkdir(parents=True, exist_ok=True)
        cf.write_text("cached")

        dr = DiffResult(reference_name="bl", reference_type=EntityType.BASELINE,
                        modified=[FileInfo(path="/etc/a", md5="abc")])
        stage_add(storage, dr)

        # Staging files dir should NOT have the file
        sf = storage.get_staging_file("/etc/a")
        assert not sf.exists()


class TestBinaryAndFiltering:
    def test_is_binary(self, tmp_path):
        from delta.diff_ops import _is_binary
        text = tmp_path / "a.txt"
        text.write_text("hello world")
        assert not _is_binary(text)

        binary = tmp_path / "b.bin"
        binary.write_bytes(b"\x00\x01\x02\x03")
        assert _is_binary(binary)

    def test_print_unified_diff_binary(self, tmp_path, capsys):
        from delta.diff_ops import _print_unified_diff
        old = tmp_path / "old.bin"
        new = tmp_path / "new.bin"
        old.write_bytes(b"\x00" * 100)
        new.write_bytes(b"\x00" * 200)
        _print_unified_diff("/etc/binary", old, new, "ref", "dev")
        out = capsys.readouterr().out
        assert "Binary files differ" in out

    def test_print_unified_diff_binary_forced(self, tmp_path, capsys):
        from delta.diff_ops import _print_unified_diff
        old = tmp_path / "old.bin"
        new = tmp_path / "new.bin"
        old.write_bytes(b"\x00" * 10)
        new.write_bytes(b"\x00" * 20)
        _print_unified_diff("/etc/binary", old, new, "ref", "dev", show_binary=True)
        out = capsys.readouterr().out
        assert "Binary files differ" not in out

    def test_parse_name_and_paths(self, storage):
        from delta.cli import _parse_name_and_paths
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))

        class FakeCtx:
            def __init__(self, s): self.storage = s
        ctx = FakeCtx(storage)

        # Entity name
        name, paths = _parse_name_and_paths(ctx, ("bl",))
        assert name == "bl"
        assert paths is None

        # Glob pattern
        name, paths = _parse_name_and_paths(ctx, ("*.conf",))
        assert name is None
        assert paths == ["*.conf"]

        # Entity + paths
        name, paths = _parse_name_and_paths(ctx, ("bl", "*.conf", "*.py"))
        assert name == "bl"
        assert paths == ["*.conf", "*.py"]

        # Path starting with /
        name, paths = _parse_name_and_paths(ctx, ("/etc/",))
        assert name is None
        assert paths == ["/etc/"]

        # Empty
        name, paths = _parse_name_and_paths(ctx, ())
        assert name is None
        assert paths is None

    def test_diff_summary_filtered(self, storage):
        from delta.diff_ops import print_diff_summary
        dr = DiffResult(
            reference_name="bl", reference_type=EntityType.BASELINE,
            modified=[FileInfo(path="/etc/a.conf", md5="x"),
                      FileInfo(path="/etc/b.py", md5="y")],
        )
        # With filter — only .conf
        # Just verify no crash
        print_diff_summary(dr, filter_paths=["*.conf"])


class TestRunOnce:
    def test_command_spec_run_once(self):
        cs = CommandSpec(cmd="cat /etc/id", save_output=True, output_key="ID", run_once=True)
        d = cs.to_dict()
        assert d["run_once"] is True
        cs2 = CommandSpec.from_dict(d)
        assert cs2.run_once is True

    def test_run_once_default_false(self):
        cs = CommandSpec(cmd="echo hi")
        assert cs.run_once is False
        d = cs.to_dict()
        assert "run_once" not in d


class TestDeletedFilesBug:
    def test_delete_created_file_no_phantom(self, storage):
        """Deleting a file that was only created by this patch should not leave it in deleted_files."""
        from delta.staging_ops import create_patch, commit_to_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        storage.save_state(DeltaState(active="bl"))
        create_patch(storage, "p1")

        # Commit 1: create a new file
        storage.save_staging(StagingManifest(
            reference="bl",
            created=["/opt/new.conf"],
            sources={"/opt/new.conf": "local"},
        ))
        sf = storage.get_staging_file("/opt/new.conf")
        sf.parent.mkdir(parents=True, exist_ok=True)
        sf.write_text("new content")
        commit_to_patch(storage, "p1")
        m = storage.load_patch("p1")
        assert "/opt/new.conf" in m.created_files

        # Commit 2: delete the same file
        storage.save_staging(StagingManifest(
            reference="bl",
            deleted=["/opt/new.conf"],
        ))
        commit_to_patch(storage, "p1")
        m = storage.load_patch("p1")
        # File should NOT be in deleted_files (it doesn't exist in baseline)
        assert "/opt/new.conf" not in m.deleted_files
        # File should also be removed from created_files
        assert "/opt/new.conf" not in m.created_files

    def test_delete_baseline_file_stays_in_deleted(self, storage):
        """Deleting a file that exists in baseline should stay in deleted_files."""
        from delta.staging_ops import create_patch, commit_to_patch
        storage.save_baseline(BaselineMetadata(name="bl", tracked_paths=["/etc"]))
        # Put a file in baseline
        bf = storage.baseline_files_dir("bl") / "etc" / "old.conf"
        bf.parent.mkdir(parents=True, exist_ok=True)
        bf.write_text("baseline content")

        storage.save_state(DeltaState(active="bl"))
        create_patch(storage, "p1")

        # First stage the file as modified so it's in modified_files
        storage.save_staging(StagingManifest(
            reference="bl",
            modified=["/etc/old.conf"],
            sources={"/etc/old.conf": "cache"},
        ))
        cf = storage.cache_files_dir / "etc" / "old.conf"
        cf.parent.mkdir(parents=True, exist_ok=True)
        cf.write_text("modified")
        commit_to_patch(storage, "p1")

        # Now delete it
        storage.save_staging(StagingManifest(
            reference="bl",
            deleted=["/etc/old.conf"],
        ))
        commit_to_patch(storage, "p1")
        m = storage.load_patch("p1")
        assert "/etc/old.conf" in m.deleted_files
