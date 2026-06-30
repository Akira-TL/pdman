from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .runtime import RuntimePaths, default_cache_root


VALID_STATUSES = {"completed", "skipped", "failed"}


def cache_root(cache_dir: str | None = None) -> Path:
    return Path(cache_dir).expanduser() if cache_dir else default_cache_root()


def history_path(cache_dir: str | None = None) -> Path:
    return cache_root(cache_dir) / "history.jsonl"


def runs_dir(cache_dir: str | None = None) -> Path:
    return cache_root(cache_dir) / "runs"


def _load_json_line(line: str) -> dict[str, Any] | None:
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def iter_history(cache_dir: str | None = None) -> Iterable[dict[str, Any]]:
    path = history_path(cache_dir)
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = _load_json_line(line)
            if record is not None:
                records.append(record)
    return records


def query_history(
    cache_dir: str | None = None,
    *,
    last: int = 20,
    status: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status}")
    records = list(iter_history(cache_dir))
    if run_id:
        records = [r for r in records if r.get("run_id") == run_id]
    if status:
        records = [r for r in records if r.get("status") == status]
    if last is not None and last > 0:
        records = records[-last:]
    return records


def _run_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    return (
        str(record.get("finished_at") or record.get("started_at") or ""),
        str(record.get("run_id") or ""),
    )


def list_runs(
    cache_dir: str | None = None,
    *,
    last: int = 20,
) -> list[dict[str, Any]]:
    path = runs_dir(cache_dir)
    if not path.exists():
        return []
    runs: list[dict[str, Any]] = []
    for run_file in path.glob("*.json"):
        try:
            data = json.loads(run_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(data, dict):
            runs.append(data)
    runs.sort(key=_run_sort_key)
    if last is not None and last > 0:
        runs = runs[-last:]
    return runs


def load_run(run_id: str, cache_dir: str | None = None) -> dict[str, Any] | None:
    path = runs_dir(cache_dir) / f"{run_id}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def format_bytes(size: int | None) -> str:
    if size is None:
        return "-"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def _record_name(record: dict[str, Any]) -> str:
    return str(record.get("filename") or record.get("url") or "-")


def _record_reason(record: dict[str, Any]) -> str:
    return str(
        record.get("reason")
        or record.get("reason_code")
        or record.get("error")
        or ""
    )


def format_history(records: list[dict[str, Any]]) -> str:
    if not records:
        return "No history found."
    lines = ["History:"]
    for record in records:
        status = str(record.get("status") or "unknown")
        finished_at = str(record.get("finished_at") or record.get("started_at") or "-")
        name = _record_name(record)
        size = format_bytes(record.get("downloaded_bytes"))
        reason = _record_reason(record)
        if reason and status != "completed":
            lines.append(f"  {finished_at} {status:<9} {name} {size} - {reason}")
        else:
            lines.append(f"  {finished_at} {status:<9} {name} {size}")
    return "\n".join(lines)


def _counts_text(record: dict[str, Any]) -> str:
    counts = record.get("task_counts") or {}
    return (
        f"completed={counts.get('completed', 0)} "
        f"skipped={counts.get('skipped', 0)} "
        f"failed={counts.get('failed', 0)}"
    )


def format_runs(records: list[dict[str, Any]]) -> str:
    if not records:
        return "No runs found."
    lines = ["Runs:"]
    for record in records:
        run_id = str(record.get("run_id") or "-")
        status = str(record.get("status") or "unknown")
        exit_code = record.get("exit_code")
        lines.append(
            f"  {run_id} {status:<8} {_counts_text(record)} exit={exit_code}"
        )
    return "\n".join(lines)


def format_run_detail(run: dict[str, Any], tasks: list[dict[str, Any]]) -> str:
    run_id = str(run.get("run_id") or "-")
    counts = run.get("task_counts") or {}
    lines = [
        f"Run {run_id}:",
        f"  status: {run.get('status')}",
        f"  started_at: {run.get('started_at')}",
        f"  finished_at: {run.get('finished_at')}",
        f"  tmp_policy: {run.get('tmp_policy')}",
        f"  exit_code: {run.get('exit_code')}",
        f"  completed: {counts.get('completed', 0)}",
        f"  skipped: {counts.get('skipped', 0)}",
        f"  failed: {counts.get('failed', 0)}",
        "",
        "Tasks:",
    ]
    if not tasks:
        lines.append("  No tasks found for this run.")
        return "\n".join(lines)
    for record in tasks:
        status = str(record.get("status") or "unknown")
        name = _record_name(record)
        size = format_bytes(record.get("downloaded_bytes"))
        reason = _record_reason(record)
        if reason and status != "completed":
            lines.append(f"  {status:<9} {name} {size} - {reason}")
        else:
            lines.append(f"  {status:<9} {name} {size}")
    return "\n".join(lines)


__all__ = [
    "RuntimePaths",
    "format_history",
    "format_run_detail",
    "format_runs",
    "history_path",
    "iter_history",
    "list_runs",
    "load_run",
    "query_history",
]
