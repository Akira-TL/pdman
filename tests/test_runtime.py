import tempfile
from collections import namedtuple
from pathlib import Path

import pytest

from pdman.runtime import (
    RuntimePaths,
    TmpSpaceInsufficient,
    default_cache_root,
    default_system_tmp_root,
    required_tmp_space,
)

DiskUsage = namedtuple("DiskUsage", "total used free")


def fake_disk_usage(free_bytes):
    return lambda path: DiskUsage(total=10_000_000_000, used=0, free=free_bytes)


def test_runtime_paths_default_roots(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache-home"))

    assert default_cache_root() == tmp_path / "cache-home" / "pdman"
    assert default_system_tmp_root() == Path(tempfile.gettempdir()) / "pdman"


def test_required_tmp_space_adds_margin():
    assert required_tmp_space(None) is None
    assert required_tmp_space(-1) is None
    assert required_tmp_space(1000) == int(1000 * 1.05) + 64 * 1024 * 1024


def test_runtime_resolves_system_tmp_by_default(tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )

    decision = runtime.resolve_task_tmp_decision(
        task_id="abc123",
        target_dir=tmp_path / "downloads",
        tmp_dir=None,
        tmp_policy="auto",
        file_size=1024,
    )

    assert decision.selected_dir == tmp_path / "system-tmp" / "runs" / "run-1" / "chunks" / "abc123"
    assert decision.policy == "auto"
    assert decision.fallback_used is False


def test_runtime_tmp_dir_override_wins(tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )

    decision = runtime.resolve_task_tmp_decision(
        task_id="abc123",
        target_dir=tmp_path / "downloads",
        tmp_dir=tmp_path / "custom-tmp",
        tmp_policy="auto",
        file_size=1024,
    )

    assert decision.selected_dir == tmp_path / "custom-tmp" / ".pdman.abc123"
    assert decision.policy == "explicit"


def test_runtime_target_policy_keeps_legacy_tmp_layout(tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )

    resolved = runtime.resolve_task_tmp_dir(
        task_id="abc123",
        target_dir=tmp_path / "downloads",
        tmp_dir=None,
        tmp_policy="target",
        file_size=1024,
    )

    assert resolved == tmp_path / "downloads" / ".pdman.abc123"


def test_runtime_auto_falls_back_to_target_when_system_tmp_has_insufficient_space(monkeypatch, tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )
    monkeypatch.setattr("pdman.runtime.shutil.disk_usage", fake_disk_usage(10))

    decision = runtime.resolve_task_tmp_decision(
        task_id="abc123",
        target_dir=tmp_path / "downloads",
        tmp_dir=None,
        tmp_policy="auto",
        file_size=1024,
    )

    assert decision.selected_dir == tmp_path / "downloads" / ".pdman.abc123"
    assert decision.fallback_used is True
    assert decision.reason == "system temporary directory has insufficient free space"


def test_runtime_system_policy_raises_when_space_is_insufficient(monkeypatch, tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )
    monkeypatch.setattr("pdman.runtime.shutil.disk_usage", fake_disk_usage(10))

    with pytest.raises(TmpSpaceInsufficient) as exc_info:
        runtime.resolve_task_tmp_decision(
            task_id="abc123",
            target_dir=tmp_path / "downloads",
            tmp_dir=None,
            tmp_policy="system",
            file_size=1024,
        )

    assert exc_info.value.policy == "system"
    assert exc_info.value.available_bytes == 10


def test_runtime_explicit_tmp_raises_when_space_is_insufficient(monkeypatch, tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )
    monkeypatch.setattr("pdman.runtime.shutil.disk_usage", fake_disk_usage(10))

    with pytest.raises(TmpSpaceInsufficient) as exc_info:
        runtime.resolve_task_tmp_decision(
            task_id="abc123",
            target_dir=tmp_path / "downloads",
            tmp_dir=tmp_path / "explicit-tmp",
            tmp_policy="auto",
            file_size=1024,
        )

    assert exc_info.value.policy == "explicit"


def test_runtime_unknown_file_size_uses_system_tmp_without_space_requirement(monkeypatch, tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )
    monkeypatch.setattr("pdman.runtime.shutil.disk_usage", fake_disk_usage(0))

    decision = runtime.resolve_task_tmp_decision(
        task_id="abc123",
        target_dir=tmp_path / "downloads",
        tmp_dir=None,
        tmp_policy="auto",
        file_size=-1,
    )

    assert decision.selected_dir == tmp_path / "system-tmp" / "runs" / "run-1" / "chunks" / "abc123"
    assert decision.required_bytes is None


def test_runtime_metadata_and_history_files(tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )

    runtime.ensure()
    runtime.write_active_run({"run_id": "run-1", "status": "running"})
    runtime.append_history({"run_id": "run-1", "status": "completed"})
    runtime.write_final_run({"run_id": "run-1", "status": "finished"})
    runtime.clear_active_run()

    assert not runtime.active_run_path.exists()
    assert runtime.final_run_path.exists()
    assert runtime.history_path.read_text().strip()
