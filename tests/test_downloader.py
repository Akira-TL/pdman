import asyncio
import json
from types import SimpleNamespace

from pdman.chunk import Chunk
from pdman.downloader import Downloader
from pdman.manager import Manager
from pdman.resume_metadata import RESUME_METADATA_FILENAME, static_resume_metadata_payload
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


def test_downloader_writes_static_resume_metadata(tmp_path):
    async def run_case():
        manager = Manager(log_path=None)
        downloader = Downloader(
            manager,
            "https://example.com/file.bin",
            str(tmp_path),
            filename="file.bin",
            pdm_tmp=str(tmp_path / "tmp"),
        )
        downloader.file_size = 2048
        downloader.header_info = {
            "ETag": "abc123",
            "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
        }
        downloader.chunk_root = Chunk(downloader, 0, 1023, str(tmp_path / "tmp" / "file.bin.0"))
        downloader.chunk_root.next = Chunk(
            downloader,
            1024,
            2047,
            str(tmp_path / "tmp" / "file.bin.1024"),
            downloader.chunk_root,
        )
        (tmp_path / "tmp").mkdir()
        (tmp_path / "tmp" / "file.bin.0").write_bytes(b"x" * 1024)

        await downloader._write_static_resume_metadata()

        payload = json.loads(
            (tmp_path / "tmp" / RESUME_METADATA_FILENAME).read_text(encoding="utf-8")
        )
        assert payload["mode"] == "static"
        assert payload["url"] == "https://example.com/file.bin"
        assert payload["target_path"] == str(tmp_path / "file.bin")
        assert payload["etag"] == "abc123"
        assert payload["last_modified"] == "Wed, 01 Jan 2025 00:00:00 GMT"
        assert payload["segments"][0]["state"] == "completed"
        assert payload["segments"][1]["state"] == "pending"

    asyncio.run(run_case())


def test_rebuild_task_uses_resume_metadata_layout_even_when_chunk_options_change(tmp_path):
    async def run_case():
        manager = Manager(
            continue_download=True,
            max_concurrent_downloads=8,
            min_split_size="512",
            log_path=None,
        )
        tmp_dir = tmp_path / "tmp"
        tmp_dir.mkdir()
        first_path = tmp_dir / "file.bin.0"
        second_path = tmp_dir / "file.bin.1024"
        first_path.write_bytes(b"x" * 1024)
        expected_chunks = [
            Chunk(None, 0, 1023, str(first_path)),
            Chunk(None, 1024, 2047, str(second_path)),
        ]
        payload = static_resume_metadata_payload(
            url="https://example.com/file.bin",
            filename="file.bin",
            target_path=tmp_path / "file.bin",
            file_size=2048,
            chunks=expected_chunks,
            etag="abc123",
            last_modified="Wed, 01 Jan 2025 00:00:00 GMT",
        )
        (tmp_dir / RESUME_METADATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")
        downloader = Downloader(
            manager,
            "https://example.com/file.bin",
            str(tmp_path),
            filename="file.bin",
            pdm_tmp=str(tmp_dir),
        )
        downloader.file_size = 2048
        downloader.header_info = {
            "ETag": "abc123",
            "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
        }

        root = await downloader.rebuild_task()

        assert root is not None
        assert root.start == 0
        assert root.end == 1023
        assert root.size == 1024
        assert root.next is not None
        assert root.next.start == 1024
        assert root.next.end == 2047
        assert root.next.size == 0

    asyncio.run(run_case())


def test_rebuild_task_rejects_mismatched_resume_metadata(tmp_path):
    async def run_case():
        manager = Manager(continue_download=True, log_path=None)
        tmp_dir = tmp_path / "tmp"
        tmp_dir.mkdir()
        partial_path = tmp_dir / "file.bin.0"
        partial_path.write_bytes(b"stale")
        payload = {
            "schema_version": 2,
            "kind": "resume",
            "mode": "static",
            "url": "https://example.com/other.bin",
            "filename": "file.bin",
            "target_path": str(tmp_path / "file.bin"),
            "file_size": 5,
            "etag": None,
            "last_modified": None,
            "created_at": None,
            "updated_at": None,
            "segments": [
                {
                    "index": 0,
                    "start": 0,
                    "end": 4,
                    "path": str(partial_path),
                    "expected_size": 5,
                    "existing_size": 5,
                    "state": "completed",
                }
            ],
        }
        (tmp_dir / RESUME_METADATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")
        downloader = Downloader(
            manager,
            "https://example.com/file.bin",
            str(tmp_path),
            filename="file.bin",
            pdm_tmp=str(tmp_dir),
        )
        downloader.file_size = 5
        downloader.header_info = {}

        assert await downloader.rebuild_task() is None
        assert not partial_path.exists()

    asyncio.run(run_case())


def test_rebuild_task_warns_when_using_legacy_pdm_fallback(tmp_path):
    async def run_case():
        manager = Manager(continue_download=True, log_path=None)
        tmp_dir = tmp_path / "tmp"
        tmp_dir.mkdir()
        partial_path = tmp_dir / "file.bin.0"
        partial_path.write_bytes(b"abc")
        (tmp_dir / ".pdm").write_text(
            json.dumps(
                {
                    "url": "https://example.com/file.bin",
                    "filename": "file.bin",
                    "md5": None,
                    "file_size": 5,
                }
            ),
            encoding="utf-8",
        )
        downloader = Downloader(
            manager,
            "https://example.com/file.bin",
            str(tmp_path),
            filename="file.bin",
            pdm_tmp=str(tmp_dir),
        )
        downloader.file_size = 5
        downloader.header_info = {}
        warnings = []
        downloader._logger = SimpleNamespace(warning=warnings.append)

        root = await downloader.rebuild_task()

        assert root is not None
        assert root.start == 0
        assert root.end == 4
        assert root.size == 3
        assert any("Legacy .pdm resume fallback" in item for item in warnings)

    asyncio.run(run_case())


def test_rebuild_task_does_not_fallback_to_legacy_pdm_after_v2_rejection(tmp_path):
    async def run_case():
        manager = Manager(continue_download=True, log_path=None)
        tmp_dir = tmp_path / "tmp"
        tmp_dir.mkdir()
        partial_path = tmp_dir / "file.bin.0"
        partial_path.write_bytes(b"abcde")
        (tmp_dir / ".pdm").write_text(
            json.dumps(
                {
                    "url": "https://example.com/file.bin",
                    "filename": "file.bin",
                    "md5": None,
                    "file_size": 5,
                }
            ),
            encoding="utf-8",
        )
        payload = {
            "schema_version": 2,
            "kind": "resume",
            "mode": "static",
            "url": "https://example.com/other.bin",
            "filename": "file.bin",
            "target_path": str(tmp_path / "file.bin"),
            "file_size": 5,
            "etag": None,
            "last_modified": None,
            "created_at": None,
            "updated_at": None,
            "segments": [
                {
                    "index": 0,
                    "start": 0,
                    "end": 4,
                    "path": str(partial_path),
                    "expected_size": 5,
                    "existing_size": 5,
                    "state": "completed",
                }
            ],
        }
        (tmp_dir / RESUME_METADATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")
        downloader = Downloader(
            manager,
            "https://example.com/file.bin",
            str(tmp_path),
            filename="file.bin",
            pdm_tmp=str(tmp_dir),
        )
        downloader.file_size = 5
        downloader.header_info = {}
        warnings = []
        downloader._logger = SimpleNamespace(warning=warnings.append)

        assert await downloader.rebuild_task() is None
        assert downloader.resume_rejection_code == "url_mismatch"
        assert downloader.resume_rejection_reason.startswith("Resume rejected [url_mismatch]")
        assert not partial_path.exists()
        assert any("Resume rejected [url_mismatch]" in item for item in warnings)
        assert not any("Legacy .pdm resume fallback" in item for item in warnings)

    asyncio.run(run_case())


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
