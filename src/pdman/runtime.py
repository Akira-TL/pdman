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
TMP_SPACE_MARGIN_RATIO = 0.05
TMP_SPACE_MARGIN_BYTES = 64 * 1024 * 1024


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


def required_tmp_space(file_size: int | None) -> int | None:
    if file_size is None or file_size <= 0:
        return None
    return int(file_size * (1 + TMP_SPACE_MARGIN_RATIO)) + TMP_SPACE_MARGIN_BYTES


class TmpSpaceInsufficient(Exception):
    def __init__(
        self,
        selected_dir: Path,
        required_bytes: int | None,
        available_bytes: int | None,
        policy: str,
    ):
        self.selected_dir = selected_dir
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes
        self.policy = policy
        super().__init__(self.describe())

    def describe(self) -> str:
        return (
            "temporary directory has insufficient free space "
            f"for policy={self.policy}: {self.selected_dir} "
            f"requires={self.required_bytes} available={self.available_bytes}"
        )


@dataclass
class TmpSpaceDecision:
    selected_dir: Path
    policy: str
    fallback_used: bool
    required_bytes: int | None
    available_bytes: int | None
    reason: str | None = None


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
    def _available_space(path: Path) -> int:
        path.mkdir(parents=True, exist_ok=True)
        return shutil.disk_usage(path).free

    @staticmethod
    def _has_space(available_bytes: int | None, required_bytes: int | None) -> bool:
        if required_bytes is None or required_bytes <= 0:
            return True
        if available_bytes is None:
            return False
        return available_bytes >= required_bytes

    def resolve_task_tmp_decision(
        self,
        *,
        task_id: str,
        target_dir: str | os.PathLike[str],
        tmp_dir: str | os.PathLike[str] | None,
        tmp_policy: str,
        file_size: int | None,
    ) -> TmpSpaceDecision:
        policy = (tmp_policy or "auto").lower()
        if policy not in TMP_POLICIES:
            raise ValueError(f"Invalid tmp_policy: {tmp_policy}")

        required_bytes = required_tmp_space(file_size)
        target_tmp = self.target_tmp_dir(target_dir, task_id)

        if tmp_dir:
            tmp_root = Path(tmp_dir).expanduser()
            available_bytes = self._available_space(tmp_root)
            selected_dir = tmp_root / f".pdman.{task_id}"
            if not self._has_space(available_bytes, required_bytes):
                raise TmpSpaceInsufficient(
                    selected_dir, required_bytes, available_bytes, "explicit"
                )
            return TmpSpaceDecision(
                selected_dir=selected_dir,
                policy="explicit",
                fallback_used=False,
                required_bytes=required_bytes,
                available_bytes=available_bytes,
            )

        if policy == "target":
            return TmpSpaceDecision(
                selected_dir=target_tmp,
                policy=policy,
                fallback_used=False,
                required_bytes=required_bytes,
                available_bytes=None,
            )

        system_tmp = self.task_chunk_dir(task_id)
        available_bytes = self._available_space(self.system_tmp_root)
        if self._has_space(available_bytes, required_bytes):
            return TmpSpaceDecision(
                selected_dir=system_tmp,
                policy=policy,
                fallback_used=False,
                required_bytes=required_bytes,
                available_bytes=available_bytes,
            )

        reason = "system temporary directory has insufficient free space"
        if policy == "system":
            raise TmpSpaceInsufficient(
                system_tmp, required_bytes, available_bytes, policy
            )
        return TmpSpaceDecision(
            selected_dir=target_tmp,
            policy=policy,
            fallback_used=True,
            required_bytes=required_bytes,
            available_bytes=available_bytes,
            reason=reason,
        )

    def resolve_task_tmp_dir(
        self,
        *,
        task_id: str,
        target_dir: str | os.PathLike[str],
        tmp_dir: str | os.PathLike[str] | None,
        tmp_policy: str,
        file_size: int | None,
    ) -> Path:
        return self.resolve_task_tmp_decision(
            task_id=task_id,
            target_dir=target_dir,
            tmp_dir=tmp_dir,
            tmp_policy=tmp_policy,
            file_size=file_size,
        ).selected_dir

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
        shutil.rmtree(self.run_dir, ignore_errors=False)


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
        "header_probe_method": result.header_probe_method,
        "header_probe_fallback_reason": result.header_probe_fallback_reason,
        "resume_rejection_code": result.resume_rejection_code,
        "resume_rejection_reason": result.resume_rejection_reason,
        "started_at": started_at,
        "finished_at": finished_at,
    }
