from __future__ import annotations

import re
from dataclasses import dataclass


_CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$")


@dataclass(frozen=True)
class ContentRange:
    start: int
    end: int
    total: int | None


class RangeResponseValidationError(ValueError):
    pass


def parse_content_range(value: str) -> ContentRange:
    match = _CONTENT_RANGE_RE.match(value.strip())
    if match is None:
        raise RangeResponseValidationError(f"Invalid Content-Range: {value!r}")
    start = int(match.group(1))
    end = int(match.group(2))
    if end < start:
        raise RangeResponseValidationError(f"Invalid Content-Range span: {value!r}")
    raw_total = match.group(3)
    total = None if raw_total == "*" else int(raw_total)
    if total is not None and end >= total:
        raise RangeResponseValidationError(f"Content-Range end exceeds total: {value!r}")
    return ContentRange(start=start, end=end, total=total)


def validate_range_response(
    *,
    status: int,
    requested_start: int,
    requested_end: int,
    file_size: int,
    content_range: str | None,
) -> None:
    if requested_start < 0 or requested_end < requested_start:
        raise RangeResponseValidationError(
            f"Invalid requested range: {requested_start}-{requested_end}"
        )
    if file_size <= 0:
        raise RangeResponseValidationError("file_size must be positive")
    full_file_range = requested_start == 0 and requested_end == file_size - 1
    if status == 200:
        if not full_file_range:
            raise RangeResponseValidationError(
                f"HTTP 200 is only valid for full-file range; requested {requested_start}-{requested_end}"
            )
        return
    if status != 206:
        raise RangeResponseValidationError(f"Unexpected range response status: {status}")
    if not content_range:
        raise RangeResponseValidationError("HTTP 206 response missing Content-Range")
    parsed = parse_content_range(content_range)
    if parsed.start != requested_start:
        raise RangeResponseValidationError(
            f"Content-Range start mismatch: expected {requested_start}, got {parsed.start}"
        )
    if parsed.end != requested_end:
        raise RangeResponseValidationError(
            f"Content-Range end mismatch: expected {requested_end}, got {parsed.end}"
        )
    if parsed.total is not None and parsed.total != file_size:
        raise RangeResponseValidationError(
            f"Content-Range total mismatch: expected {file_size}, got {parsed.total}"
        )
