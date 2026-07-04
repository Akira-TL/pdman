import pytest

from pdman.output_modes import (
    OUTPUT_MODE_CHOICES,
    OutputMode,
    is_structured_output,
    parse_output_mode,
    resolve_output_mode,
)
from pdman.output_renderers import NoOpProgress, PlainOutputRenderer, RichOutputRenderer


def test_output_mode_choices_are_stable():
    assert OUTPUT_MODE_CHOICES == ("rich", "plain", "json", "jsonl")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("rich", OutputMode.RICH),
        ("plain", OutputMode.PLAIN),
        ("json", OutputMode.JSON),
        ("jsonl", OutputMode.JSONL),
        (OutputMode.JSON, OutputMode.JSON),
    ],
)
def test_parse_output_mode_accepts_stable_values(value, expected):
    assert parse_output_mode(value) is expected


def test_parse_output_mode_rejects_unknown_values():
    with pytest.raises(ValueError, match="unsupported output mode"):
        parse_output_mode("xml")


def test_resolve_output_mode_prefers_explicit_value():
    assert resolve_output_mode("json", is_tty=True) is OutputMode.JSON
    assert resolve_output_mode("rich", is_tty=False) is OutputMode.RICH


def test_resolve_output_mode_defaults_by_tty():
    assert resolve_output_mode(is_tty=True) is OutputMode.RICH
    assert resolve_output_mode(is_tty=False) is OutputMode.PLAIN


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("rich", False),
        ("plain", False),
        ("json", True),
        ("jsonl", True),
        (OutputMode.JSONL, True),
    ],
)
def test_is_structured_output(mode, expected):
    assert is_structured_output(mode) is expected


def test_noop_progress_accepts_rich_progress_calls():
    progress = NoOpProgress()

    with progress:
        task_id = progress.add_task("Downloading file.bin", total=100, dl="0")
        progress.update(task_id, completed=50, dl="50")
        progress.stop_task(task_id)
        assert progress.tasks[task_id]["stopped"] is True
        progress.remove_task(task_id)

    assert progress.tasks == {}


def test_plain_renderer_uses_noop_progress_and_plain_console():
    renderer = PlainOutputRenderer()
    console = renderer.create_console()
    progress = renderer.create_progress(console=console, summary_interval=1.0)

    assert isinstance(progress, NoOpProgress)
    assert console.is_terminal is False


def test_rich_renderer_creates_rich_progress():
    renderer = RichOutputRenderer()
    console = renderer.create_console()
    progress = renderer.create_progress(console=console, summary_interval=1.0)

    assert not isinstance(progress, NoOpProgress)
