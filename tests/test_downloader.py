import asyncio

from pdman.chunk import Chunk
from pdman.downloader import Downloader
from pdman.manager import Manager
from pdman.status import TaskReason, TaskStatus


def test_downloader_dynamic_segment_support_checks(tmp_path):
    manager = Manager(
        segment_mode="dynamic",
        max_concurrent_downloads=2,
        min_split_size="1K",
        log_path=None,
    )
    downloader = Downloader(
        manager,
        "https://example.com/file.bin",
        str(tmp_path),
        filename="file.bin",
    )
    downloader.file_size = 4096
    downloader.header_info = {"Accept-Ranges": "bytes"}

    assert downloader._can_use_dynamic_segments() is True

    downloader.header_info = {"Accept-Ranges": "none"}
    assert downloader._can_use_dynamic_segments() is False

    downloader.header_info = {"Accept-Ranges": "bytes"}
    downloader.file_size = -1
    assert downloader._can_use_dynamic_segments() is False

    downloader.file_size = 4096
    manager.continue_download = True
    assert downloader._can_use_dynamic_segments() is False


def make_decision_downloader(tmp_path, **manager_kwargs):
    defaults = {
        "segment_mode": "auto",
        "max_concurrent_downloads": 2,
        "min_split_size": "1K",
        "log_path": None,
    }
    defaults.update(manager_kwargs)
    manager = Manager(**defaults)
    downloader = Downloader(
        manager,
        "https://example.com/file.bin",
        str(tmp_path),
        filename="file.bin",
    )
    downloader.file_size = 4096
    downloader.header_info = {"Accept-Ranges": "bytes"}
    return downloader


def test_auto_segment_mode_uses_dynamic_when_eligible(tmp_path):
    downloader = make_decision_downloader(tmp_path)

    decision = downloader._dynamic_segment_decision()

    assert decision.use_dynamic is True
    assert decision.reason == "dynamic_eligible"
    assert downloader._can_use_dynamic_segments() is True
    assert downloader.segment_decision_reason == "dynamic_eligible"


def test_dynamic_segment_decision_fallback_reasons(tmp_path):
    cases = [
        ({"segment_mode": "static"}, {}, "segment_mode_static"),
        ({"continue_download": True}, {}, "continue_not_supported"),
        ({}, {"file_size": -1}, "unknown_file_size"),
        ({}, {"header_info": {"Accept-Ranges": "none"}}, "accept_ranges_not_bytes"),
        ({}, {"header_info": {}}, "accept_ranges_not_bytes"),
        ({"force_sequential": True}, {}, "force_sequential_enabled"),
        ({"max_concurrent_downloads": 1}, {}, "insufficient_workers"),
        ({"min_split_size": "4K"}, {}, "file_too_small"),
    ]

    for manager_kwargs, downloader_attrs, expected_reason in cases:
        downloader = make_decision_downloader(tmp_path, **manager_kwargs)
        for name, value in downloader_attrs.items():
            setattr(downloader, name, value)

        decision = downloader._dynamic_segment_decision()

        assert decision.use_dynamic is False
        assert decision.reason == expected_reason


def test_dynamic_segment_decision_accept_ranges_header_quirks(tmp_path):
    accepted_values = ["bytes", "Bytes", "BYTES", "bytes, none", "none, bytes"]
    for accept_ranges in accepted_values:
        downloader = make_decision_downloader(tmp_path)
        downloader.header_info = {"Accept-Ranges": accept_ranges}

        decision = downloader._dynamic_segment_decision()

        assert decision.use_dynamic is True
        assert decision.reason == "dynamic_eligible"

    rejected_values = ["none", "items", "", None]
    for accept_ranges in rejected_values:
        downloader = make_decision_downloader(tmp_path)
        downloader.header_info = {"Accept-Ranges": accept_ranges}

        decision = downloader._dynamic_segment_decision()

        assert decision.use_dynamic is False
        assert decision.reason == "accept_ranges_not_bytes"


def test_get_url_file_size_treats_invalid_content_length_as_unknown(tmp_path):
    downloader = make_decision_downloader(tmp_path)
    for content_length in (None, "", "unknown", "12.5", "-10"):
        downloader.header_info = {"Content-Length": content_length}

        assert asyncio.run(downloader.get_url_file_size()) == -1


def test_downloader_dynamic_allocator_uses_range_size_policy(tmp_path):
    manager = Manager(
        segment_mode="dynamic",
        max_concurrent_downloads=4,
        min_split_size="1M",
        log_path=None,
    )
    downloader = Downloader(
        manager,
        "https://example.com/file.bin",
        str(tmp_path),
        filename="file.bin",
        pdm_tmp=str(tmp_path / "tmp"),
    )
    downloader.file_size = 1024 * 1024 * 1024

    allocator = downloader._build_range_allocator()

    assert allocator.range_size == 64 * 1024 * 1024
    assert allocator.total_ranges == 16


def test_manager_segment_mode_defaults_to_static():
    manager = Manager(log_path=None)

    assert manager.segment_mode == "static"


def test_manager_accepts_dynamic_segment_mode():
    manager = Manager(segment_mode="dynamic", log_path=None)

    assert manager.segment_mode == "dynamic"


def test_manager_accepts_auto_segment_mode():
    manager = Manager(segment_mode="auto", log_path=None)

    assert manager.segment_mode == "auto"


def test_manager_rejects_invalid_segment_mode():
    try:
        Manager(segment_mode="bad", log_path=None)
    except ValueError as e:
        assert "Invalid segment_mode" in str(e)
    else:
        raise AssertionError("invalid segment mode should raise")


def test_refresh_downloaded_bytes_uses_existing_chunk_sizes(tmp_path):
    manager = Manager(log_path=None)
    downloader = Downloader(manager, "https://example.com/file.bin", str(tmp_path))

    first = Chunk(downloader, 0, 99, str(tmp_path / "file.0"))
    second = Chunk(downloader, 100, 199, str(tmp_path / "file.100"))
    first.size = 40
    second.size = 25
    first.next = second
    downloader.chunk_root = first

    assert downloader.refresh_downloaded_bytes() == 65
    assert downloader.downloaded_bytes == 65


def test_quit_if_exists_skips_named_file_before_parse_config(tmp_path):
    async def run_case():
        existing_file = tmp_path / "already-there.bin"
        existing_file.write_bytes(b"existing")
        manager = Manager(quit_if_exists=True, log_path=None)
        downloader = Downloader(
            manager,
            "https://example.com/should-not-be-requested.bin",
            str(tmp_path),
            filename=existing_file.name,
        )

        async def fail_if_called():
            raise AssertionError("parse_config should not run for an existing named target")

        downloader.parse_config = fail_if_called

        result = await downloader.start_download()

        assert result.url == downloader.url
        assert result.status == TaskStatus.SKIPPED
        assert result.reason_code == TaskReason.TARGET_EXISTS
        assert existing_file.read_bytes() == b"existing"
        assert downloader._done is True

    asyncio.run(run_case())
