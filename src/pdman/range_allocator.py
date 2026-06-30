from __future__ import annotations

from collections import deque
from pathlib import Path

from .range_task import RangeTask


class RangeAllocator:
    def __init__(
        self,
        *,
        file_size: int,
        range_size: int,
        tmp_dir: str | Path,
        filename: str,
        max_retries: int = 0,
    ):
        if file_size <= 0:
            raise ValueError("file_size must be positive")
        if range_size <= 0:
            raise ValueError("range_size must be positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        self.file_size = file_size
        self.range_size = range_size
        self.tmp_dir = Path(tmp_dir)
        self.filename = filename
        self.max_retries = max_retries
        self.ranges = self._build_ranges()
        self._pending = deque(self.ranges)
        self._active: dict[int, RangeTask] = {}
        self.completed: list[RangeTask] = []
        self.failed: list[RangeTask] = []

    def _build_ranges(self) -> list[RangeTask]:
        ranges: list[RangeTask] = []
        for index, start in enumerate(range(0, self.file_size, self.range_size)):
            end = min(start + self.range_size - 1, self.file_size - 1)
            path = self.tmp_dir / f"{self.filename}.range.{start}-{end}"
            ranges.append(RangeTask(index=index, start=start, end=end, path=path))
        return ranges

    def claim_next(self) -> RangeTask | None:
        while self._pending:
            task = self._pending.popleft()
            if task.is_complete:
                self.mark_completed(task)
                continue
            task.attempts += 1
            self._active[task.index] = task
            return task
        return None

    def mark_completed(self, task: RangeTask) -> None:
        self._active.pop(task.index, None)
        if task not in self.completed:
            self.completed.append(task)

    def mark_failed(self, task: RangeTask, error: str) -> bool:
        self._active.pop(task.index, None)
        task.last_error = error
        if task.attempts <= self.max_retries:
            self._pending.append(task)
            return True
        if task not in self.failed:
            self.failed.append(task)
        return False

    @property
    def done(self) -> bool:
        return not self._pending and not self._active and not self.failed

    @property
    def has_failures(self) -> bool:
        return bool(self.failed)

    @property
    def completed_bytes(self) -> int:
        return sum(task.expected_size for task in self.completed)
