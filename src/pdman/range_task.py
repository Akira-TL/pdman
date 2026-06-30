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
