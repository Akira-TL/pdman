from __future__ import annotations

import json
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .status import TaskResult


TMP_POLICIES = {"auto", "system", "target"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def default_cache_root() -> Path:
    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        return Path(xdg_cache_home).expanduser() / "pdman"
    return Path.home() / ".cache" / "pdman"


def default_system_tmp_root() -> Path:
    return Path(tempfile.gettempdir()) / "pdman"


@dataclass
class RuntimePaths:
    run_id: str
    cache_root: Path
    system_tmp_root: Path

    @classmethod
    def create(
        cls,
        cache_dir: str | os.PathLike[str] | None = None,
        system_tmp_root: str | os.PathLike[str] | None = None,
        run_id: str | None = None,
    ) -> "RuntimePaths":
        return cls(
            run_id=run_id or new_run_id(),
            cache_root=Path(cache_dir).expanduser()
            if cache_dir
            else default_cache_root(),
            system_tmp_root=Path(system_tmp_root).expanduser()
            if system_tmp_root
            else default_system_tmp_root(),
        )

    @property
    def run_dir(self) -> Path:
        return self.system_tmp_root / "runs" / self.run_id

    @property
    def chunks_dir(self) -> Path:
        return self.run_dir / "chunks"

    @property
    def locks_dir(self) -> Path:
        return self.run_dir / "locks"

    @property
    def active_dir(self) -> Path:
        return self.cache_root / "active"

    @property
    def cache_runs_dir(self) -> Path:
        return self.cache_root / "runs"

    @property
    def metadata_dir(self) -> Path:
        return self.cache_root / "metadata"

    @property
    def history_path(self) -> Path:
        return self.cache_root / "history.jsonl"

    @property
    def active_run_path(self) -> Path:
        return self.active_dir / f"{self.run_id}.json"

    @property
    def final_run_path(self) -> Path:
        return self.cache_runs_dir / f"{self.run_id}.json"

    def ensure(self) -> None:
        for path in (
            self.chunks_dir,
            self.locks_dir,
            self.active_dir,
            self.cache_runs_dir,
            self.metadata_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def task_chunk_dir(self, task_id: str) -> Path:
        return self.chunks_dir / task_id

    @staticmethod
    def target_tmp_dir(target_dir: str | os.PathLike[str], task_id: str) -> Path:
        return Path(target_dir).expanduser() / f".pdman.{task_id}"

    @staticmethod
    def _has_space(path: Path, required_bytes: int | None) -> bool:
        if required_bytes is None or required_bytes <= 0:
            return True
        usage = shutil.disk_usage(path)
        return usage.free > required_bytes

    def resolve_task_tmp_dir(
        self,
        *,
        task_id: str,
        target_dir: str | os.PathLike[str],
        tmp_dir: str | os.PathLike[str] | None,
        tmp_policy: str,
        file_size: int | None,
    ) -> Path:
        policy = (tmp_policy or "auto").lower()
        if policy not in TMP_POLICIES:
            raise ValueError(f"Invalid tmp_policy: {tmp_policy}")
        if tmp_dir:
            return Path(tmp_dir).expanduser() / f".pdman.{task_id}"
        target_tmp = self.target_tmp_dir(target_dir, task_id)
        if policy == "target":
            return target_tmp
        system_tmp = self.task_chunk_dir(task_id)
        if policy == "system":
            return system_tmp
        self.system_tmp_root.mkdir(parents=True, exist_ok=True)
        required_bytes = file_size if file_size and file_size > 0 else None
        if self._has_space(self.system_tmp_root, required_bytes):
            return system_tmp
        return target_tmp

    def write_json(self, path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
        tmp_path.replace(path)

    def write_active_run(self, data: dict[str, Any]) -> None:
        self.write_json(self.active_run_path, data)

    def write_final_run(self, data: dict[str, Any]) -> None:
        self.write_json(self.final_run_path, data)

    def clear_active_run(self) -> None:
        try:
            self.active_run_path.unlink()
        except FileNotFoundError:
            pass

    def append_history(self, record: dict[str, Any]) -> None:
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with self.history_path.open("a") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")

    def cleanup_run_dir(self) -> None:
        shutil.rmtree(self.run_dir, ignore_errors=True)


def task_result_to_record(
    *,
    run_id: str,
    task_id: str | None,
    result: TaskResult,
    started_at: str | None,
    finished_at: str | None,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "task_id": task_id,
        "url": result.url,
        "filename": result.filename,
        "status": result.status.value,
        "reason": result.reason,
        "reason_code": result.reason_code.value if result.reason_code else None,
        "error": result.error,
        "downloaded_bytes": result.downloaded_bytes,
        "total_bytes": result.total_bytes,
        "started_at": started_at,
        "finished_at": finished_at,
    }
