from __future__ import annotations

from enum import Enum


class OutputMode(str, Enum):
    """Supported top-level output modes for pdman CLI surfaces."""

    RICH = "rich"
    PLAIN = "plain"
    JSON = "json"
    JSONL = "jsonl"


OUTPUT_MODE_CHOICES = tuple(mode.value for mode in OutputMode)


def parse_output_mode(value: str | OutputMode) -> OutputMode:
    """Parse a user-facing output mode value into an OutputMode.

    Raises:
        ValueError: if the value is not one of the stable output mode names.
    """

    if isinstance(value, OutputMode):
        return value
    try:
        return OutputMode(value)
    except ValueError as exc:
        choices = ", ".join(OUTPUT_MODE_CHOICES)
        raise ValueError(f"unsupported output mode: {value!r}; expected one of: {choices}") from exc


def resolve_output_mode(
    explicit: str | OutputMode | None = None,
    *,
    is_tty: bool,
) -> OutputMode:
    """Resolve the effective top-level output mode.

    Explicit user selection always wins. Without an explicit value, TTY output
    defaults to Rich UI and non-TTY output defaults to plain text.
    """

    if explicit is not None:
        return parse_output_mode(explicit)
    return OutputMode.RICH if is_tty else OutputMode.PLAIN


def is_structured_output(mode: str | OutputMode) -> bool:
    """Return whether the mode must keep stdout machine-readable."""

    parsed = parse_output_mode(mode)
    return parsed in {OutputMode.JSON, OutputMode.JSONL}
