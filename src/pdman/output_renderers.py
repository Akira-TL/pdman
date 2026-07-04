from __future__ import annotations

from itertools import count
from typing import Any

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


class NoOpProgress:
    """Progress-compatible sink used by non-Rich output modes.

    Downloader code can continue to call add/update/remove/stop without needing
    to know whether the current output mode renders an interactive progress UI.
    """

    def __init__(self) -> None:
        self._ids = count(1)
        self.tasks: dict[int, dict[str, Any]] = {}

    def __enter__(self) -> "NoOpProgress":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def add_task(self, description: str, *args: Any, **kwargs: Any) -> int:
        task_id = next(self._ids)
        self.tasks[task_id] = {
            "description": description,
            "args": args,
            "fields": dict(kwargs),
        }
        return task_id

    def update(self, task_id: int, *args: Any, **kwargs: Any) -> None:
        task = self.tasks.get(task_id)
        if task is None:
            return
        if "description" in kwargs:
            task["description"] = kwargs["description"]
        task.setdefault("updates", []).append({"args": args, "fields": dict(kwargs)})

    def remove_task(self, task_id: int) -> None:
        self.tasks.pop(task_id, None)

    def stop_task(self, task_id: int) -> None:
        task = self.tasks.get(task_id)
        if task is not None:
            task["stopped"] = True


class PlainOutputRenderer:
    """ANSI-free output boundary for log-oriented modes.

    v0.9.2 only provides a no-op progress sink. Lifecycle lines and structured
    summaries are intentionally left for later v0.9 stages.
    """

    mode = "plain"

    def create_console(self) -> Console:
        return Console(force_terminal=False, color_system=None)

    def create_progress(self, *, console: Console, summary_interval: float) -> NoOpProgress:
        return NoOpProgress()


class RichOutputRenderer:
    """Interactive Rich output boundary."""

    mode = "rich"

    def create_console(self) -> Console:
        return Console()

    def create_progress(self, *, console: Console, summary_interval: float) -> Progress:
        return Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[bold blue]DL:{task.fields[dl]}"),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=1.0 / max(summary_interval, 0.1),
        )
