import asyncio

from pdman.chunk import Chunk
from pdman.downloader import Downloader
from pdman.manager import Manager
from pdman.status import TaskReason, TaskStatus


def test_downloader_dynamic_segment_support_checks(tmp_path):
    manager = Manager(segment_mode="dynamic", log_path=None)
    downloader = Downloader(
        manager,
        "https://example.com/file.bin",
        str(tmp_path),
        filename="file.bin",
    )
    downloader.file_size = 1024
    downloader.header_info = {"Accept-Ranges": "bytes"}

    assert downloader._can_use_dynamic_segments() is True

    downloader.header_info = {"Accept-Ranges": "none"}
    assert downloader._can_use_dynamic_segments() is False

    downloader.header_info = {"Accept-Ranges": "bytes"}
    downloader.file_size = -1
    assert downloader._can_use_dynamic_segments() is False

    downloader.file_size = 1024
    manager.continue_download = True
    assert downloader._can_use_dynamic_segments() is False


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
