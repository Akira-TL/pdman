from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class RangeTask:
    index: int
    start: int
    end: int
    path: Path
    attempts: int = 0
    last_error: str | None = None
    downloaded_bytes: int = 0
    last_speed_bps: float | None = None

    @property
    def expected_size(self) -> int:
        return self.end - self.start + 1

    def existing_size(self) -> int:
        if not self.path.exists():
            return 0
        return min(self.path.stat().st_size, self.expected_size)

    @property
    def is_complete(self) -> bool:
        return self.existing_size() == self.expected_size

    @property
    def next_start(self) -> int:
        return min(self.start + self.existing_size(), self.end + 1)

    @property
    def remaining_size(self) -> int:
        return max(0, self.end - self.next_start + 1)

    def can_split(self, min_size: int) -> bool:
        return self.existing_size() > 0 and self.remaining_size > min_size

    def discard_partial(self) -> int:
        removed = self.path.stat().st_size if self.path.exists() else 0
        if self.path.exists():
            self.path.unlink()
        self.downloaded_bytes = 0
        return min(removed, self.expected_size)
