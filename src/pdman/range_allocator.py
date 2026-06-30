from __future__ import annotations

from collections import deque
from pathlib import Path

from .range_task import RangeTask


DYNAMIC_RANGE_ALIGNMENT = 64 * 1024


def _align_down(value: int, alignment: int = DYNAMIC_RANGE_ALIGNMENT) -> int:
    if value < alignment:
        return value
    return value - (value % alignment)


def choose_dynamic_range_size(
    *,
    file_size: int,
    min_split_size: int,
    worker_count: int,
    target_ranges_per_worker: int = 4,
) -> int:
    if file_size <= 0:
        raise ValueError("file_size must be positive")
    if min_split_size <= 0:
        raise ValueError("min_split_size must be positive")
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    if target_ranges_per_worker <= 0:
        raise ValueError("target_ranges_per_worker must be positive")
    target_range_count = worker_count * target_ranges_per_worker
    base_size = max(1, file_size // target_range_count)
    range_size = max(min_split_size, base_size)
    return max(min_split_size, _align_down(range_size))


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
        self.range_size = min(range_size, file_size)
        self.tmp_dir = Path(tmp_dir)
        self.filename = filename
        self.max_retries = max_retries
        self._next_index = 0
        self.ranges = self._build_ranges()
        self._pending = deque(self.ranges)
        self._active: dict[int, RangeTask] = {}
        self.completed: list[RangeTask] = []
        self.failed: list[RangeTask] = []
        self.requeue_count = 0
        self.split_count = 0

    def _range_path(self, start: int, end: int) -> Path:
        return self.tmp_dir / f"{self.filename}.range.{start}-{end}"

    def _build_ranges(self) -> list[RangeTask]:
        ranges: list[RangeTask] = []
        for index, start in enumerate(range(0, self.file_size, self.range_size)):
            end = min(start + self.range_size - 1, self.file_size - 1)
            ranges.append(
                RangeTask(
                    index=index,
                    start=start,
                    end=end,
                    path=self._range_path(start, end),
                )
            )
        self._next_index = len(ranges)
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
            self.requeue_count += 1
            self._pending.append(task)
            return True
        if task not in self.failed:
            self.failed.append(task)
        return False

    def split_remaining(self, task: RangeTask, *, min_size: int) -> RangeTask | None:
        if min_size <= 0:
            raise ValueError("min_size must be positive")
        existing_size = task.existing_size()
        if not task.can_split(min_size):
            return None
        old_end = task.end
        partial_end = task.start + existing_size - 1
        child_start = partial_end + 1
        child_path = self._range_path(child_start, old_end)
        task.end = partial_end
        new_task_path = self._range_path(task.start, task.end)
        if task.path != new_task_path:
            task.path.replace(new_task_path)
            task.path = new_task_path
        task.downloaded_bytes = existing_size
        self._active.pop(task.index, None)
        if task not in self.completed:
            self.completed.append(task)
        child = RangeTask(
            index=self._next_index,
            start=child_start,
            end=old_end,
            path=child_path,
        )
        self._next_index += 1
        self.ranges.append(child)
        self._pending.appendleft(child)
        self.split_count += 1
        return child

    def task_state(self, task: RangeTask) -> str:
        if task in self.completed:
            return "completed"
        if task in self.failed:
            return "failed"
        if task.index in self._active:
            return "active"
        if task in self._pending:
            return "pending"
        return "unknown"

    @property
    def total_ranges(self) -> int:
        return len(self.ranges)

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def active_count(self) -> int:
        return len(self._active)

    @property
    def completed_count(self) -> int:
        return len(self.completed)

    @property
    def failed_count(self) -> int:
        return len(self.failed)

    @property
    def retried_count(self) -> int:
        return sum(max(task.attempts - 1, 0) for task in self.ranges)

    @property
    def done(self) -> bool:
        return not self._pending and not self._active and not self.failed

    @property
    def has_failures(self) -> bool:
        return bool(self.failed)

    @property
    def completed_bytes(self) -> int:
        return sum(task.expected_size for task in self.completed)
