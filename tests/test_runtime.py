import tempfile
from pathlib import Path

from pdman.runtime import RuntimePaths, default_cache_root, default_system_tmp_root


def test_runtime_paths_default_roots(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache-home"))

    assert default_cache_root() == tmp_path / "cache-home" / "pdman"
    assert default_system_tmp_root() == Path(tempfile.gettempdir()) / "pdman"


def test_runtime_resolves_system_tmp_by_default(tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )

    resolved = runtime.resolve_task_tmp_dir(
        task_id="abc123",
        target_dir=tmp_path / "downloads",
        tmp_dir=None,
        tmp_policy="auto",
        file_size=1024,
    )

    assert resolved == tmp_path / "system-tmp" / "runs" / "run-1" / "chunks" / "abc123"


def test_runtime_tmp_dir_override_wins(tmp_path):
    runtime = RuntimePaths.create(
        cache_dir=tmp_path / "cache",
        system_tmp_root=tmp_path / "system-tmp",
        run_id="run-1",
    )

    resolved = runtime.resolve_task_tmp_dir(
        task_id="abc123",
        target_dir=tmp_path / "downloads",
        tmp_dir=tmp_path / "custom-tmp",
        tmp_policy="auto",
        file_size=1024,
    )

    assert resolved == tmp_path / "custom-tmp" / ".pdman.abc123"


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
