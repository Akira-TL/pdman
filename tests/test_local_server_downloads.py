import asyncio
import hashlib
import json
import socket
from collections import namedtuple
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pdman.cli as cli
from pdman.chunk import Chunk
from pdman.manager import Manager
from pdman.queue import load_queue
from pdman.range_metadata import DYNAMIC_RANGE_METADATA_FILENAME
from pdman.resume_metadata import RESUME_METADATA_FILENAME, static_resume_metadata_payload
from pdman.status import TaskReason, TaskStatus

PAYLOAD = (b"pdman-local-test-" * 8192) + b"end"
UNKNOWN_SIZE_PAYLOAD = b"unknown-size-body" * 1024
UNEVEN_PAYLOAD = (b"uneven-pdman-payload" * 157) + b"tail"
REQUIRED_UA = "PDMAN-Integration-Test/1.0"
FLAKY_COUNTS = {}
DiskUsage = namedtuple("DiskUsage", "total used free")


def fake_disk_usage(free_bytes):
    return lambda path: DiskUsage(total=10_000_000_000, used=0, free=free_bytes)


class LocalDownloadHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return

    def do_HEAD(self):
        self._handle_request(send_body=False)

    def do_GET(self):
        self._handle_request(send_body=True)

    def _handle_request(self, send_body: bool):
        parsed = urlparse(self.path)
        if parsed.path == "/normal.bin":
            self._send_payload(PAYLOAD, "normal.bin", send_body)
            return
        if parsed.path == "/head-405-get-ok.bin":
            if not send_body:
                self._send_text(405, b"HEAD not allowed")
                return
            self._send_payload(PAYLOAD, "head-405-get-ok.bin", send_body)
            return
        if parsed.path == "/head-501-get-ok.bin":
            if not send_body:
                self._send_text(501, b"HEAD not implemented")
                return
            self._send_payload(PAYLOAD, "head-501-get-ok.bin", send_body)
            return
        if parsed.path == "/head-status-get-ok.bin":
            status = int(parse_qs(parsed.query).get("status", ["405"])[0])
            if not send_body:
                self._send_text(status, f"HEAD HTTP {status}".encode())
                return
            self._send_payload(PAYLOAD, f"head-http-{status}.bin", send_body)
            return
        if parsed.path == "/head-close-get-ok.bin":
            if not send_body:
                self.close_connection = True
                self.connection.close()
                return
            self._send_payload(PAYLOAD, "head-close-get-ok.bin", send_body)
            return
        if parsed.path == "/head-501-get-ignored-range.bin":
            if not send_body:
                self._send_text(501, b"HEAD not implemented")
                return
            self._send_ignored_range_payload(PAYLOAD, "head-501-get-ignored-range.bin", send_body)
            return
        if parsed.path == "/head-501-get-unknown-total.bin":
            if not send_body:
                self._send_text(501, b"HEAD not implemented")
                return
            self._send_unknown_total_range_probe(PAYLOAD, "head-501-get-unknown-total.bin")
            return
        if parsed.path == "/uneven.bin":
            self._send_payload(UNEVEN_PAYLOAD, "uneven.bin", send_body)
            return
        if parsed.path == "/slow.bin":
            if not send_body:
                delay = float(parse_qs(parsed.query).get("delay", ["0.1"])[0])
                time.sleep(delay)
            self._send_payload(PAYLOAD, "slow.bin", send_body)
            return
        if parsed.path == "/ua.bin":
            if self.headers.get("User-Agent") != REQUIRED_UA:
                self._send_text(403, b"user-agent required")
                return
            self._send_payload(PAYLOAD, "ua.bin", send_body)
            return
        if parsed.path == "/unknown.bin":
            self._send_unknown_size(send_body)
            return
        if parsed.path == "/status.bin":
            status = int(parse_qs(parsed.query).get("status", ["503"])[0])
            self._send_text(status, f"HTTP {status}".encode())
            return
        if parsed.path == "/flaky.bin":
            key = self.path
            FLAKY_COUNTS[key] = FLAKY_COUNTS.get(key, 0) + 1
            if FLAKY_COUNTS[key] == 1:
                self._send_text(503, b"flaky first failure")
                return
            self._send_payload(PAYLOAD, "flaky.bin", send_body)
            return
        if parsed.path == "/flaky-range.bin":
            range_header = self.headers.get("Range")
            if send_body and range_header:
                key = f"{parsed.path}:{range_header}"
                FLAKY_COUNTS[key] = FLAKY_COUNTS.get(key, 0) + 1
                if FLAKY_COUNTS[key] == 1:
                    self._send_text(503, b"flaky range first failure")
                    return
            self._send_payload(PAYLOAD, "flaky-range.bin", send_body)
            return
        if parsed.path == "/slow-range-once.bin":
            range_header = self.headers.get("Range")
            if send_body and range_header:
                key = parsed.path
                FLAKY_COUNTS[key] = FLAKY_COUNTS.get(key, 0) + 1
                if FLAKY_COUNTS[key] == 1:
                    self._send_slow_payload(PAYLOAD, "slow-range-once.bin", send_body)
                    return
            self._send_payload(PAYLOAD, "slow-range-once.bin", send_body)
            return
        if parsed.path == "/bad-content-range.bin":
            self._send_bad_content_range_payload(PAYLOAD, "bad-content-range.bin", send_body)
            return
        if parsed.path == "/ignored-range.bin":
            self._send_ignored_range_payload(PAYLOAD, "ignored-range.bin", send_body)
            return
        if parsed.path == "/short-range.bin":
            self._send_short_range_payload(PAYLOAD, "short-range.bin", send_body)
            return
        self._send_text(404, b"not found")

    def _send_text(self, status: int, body: bytes):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _payload_range(self, data: bytes):
        range_header = self.headers.get("Range")
        status = 200
        start = 0
        end = len(data) - 1
        if self.command != "HEAD" and range_header:
            unit, raw_range = range_header.split("=", 1)
            assert unit == "bytes"
            raw_start, raw_end = raw_range.split("-", 1)
            start = int(raw_start)
            end = int(raw_end) if raw_end else len(data) - 1
            end = min(end, len(data) - 1)
            status = 206
        return status, start, end, data[start:end + 1]

    def _send_payload(self, data: bytes, filename: str, send_body: bool):
        status, start, end, body = self._payload_range(data)
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def _send_slow_payload(self, data: bytes, filename: str, send_body: bool):
        status, start, end, body = self._payload_range(data)
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        if send_body:
            for index in range(0, len(body), 512):
                self.wfile.write(body[index:index + 512])
                self.wfile.flush()
                time.sleep(0.05)

    def _send_bad_content_range_payload(self, data: bytes, filename: str, send_body: bool):
        status, start, end, body = self._payload_range(data)
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        if status == 206:
            bad_end = min(len(body) - 1, len(data) - 1)
            self.send_header("Content-Range", f"bytes 0-{bad_end}/{len(data)}")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def _send_ignored_range_payload(self, data: bytes, filename: str, send_body: bool):
        self.send_response(200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if send_body:
            self.wfile.write(data)

    def _send_short_range_payload(self, data: bytes, filename: str, send_body: bool):
        status, start, end, body = self._payload_range(data)
        if status == 206 and len(body) > 1:
            body = body[:-1]
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def _send_unknown_total_range_probe(self, data: bytes, filename: str):
        range_header = self.headers.get("Range")
        if range_header:
            self.send_response(206)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", "1")
            self.send_header("Content-Range", "bytes 0-0/*")
            self.end_headers()
            self.wfile.write(data[:1])
            return
        self.send_response(200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def _send_unknown_size(self, send_body: bool):
        self.send_response(200)
        self.send_header("Content-Disposition", 'attachment; filename="unknown.bin"')
        self.send_header("Connection", "close")
        self.end_headers()
        if send_body:
            for index in range(0, len(UNKNOWN_SIZE_PAYLOAD), 4096):
                self.wfile.write(UNKNOWN_SIZE_PAYLOAD[index:index + 4096])
        self.close_connection = True


class LocalDownloadServer:
    def __enter__(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), LocalDownloadHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]
        return self

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"


def _unused_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_local_yaml_downloads_normal_and_user_agent_urls(tmp_path):
    with LocalDownloadServer() as server:
        tasks_file = tmp_path / "tasks.yaml"
        download_dir = tmp_path / "downloads"
        tasks_file.write_text(
            "\n".join(
                [
                    f"{server.url('/normal.bin')}:",
                    "  file_name: normal.bin",
                    f"  dir_path: {download_dir}",
                    f"{server.url('/ua.bin')}:",
                    "  file_name: ua.bin",
                    f"  dir_path: {download_dir}",
                ]
            )
        )

        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            min_split_size="1K",
            user_agent=REQUIRED_UA,
            log_path=None,
        )
        manager.load_input_file(str(tasks_file))
        asyncio.run(manager.download())

        assert (download_dir / "normal.bin").read_bytes() == PAYLOAD
        assert (download_dir / "ua.bin").read_bytes() == PAYLOAD


def test_slow_head_succeeds_after_progress_delay(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            connect_timeout=0.5,
            connect_progress_delay=0.01,
            summary_interval=0.01,
            log_path=None,
        )
        manager.append(
            server.url("/slow.bin?delay=0.05"),
            file_name="slow.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "slow.bin").read_bytes() == PAYLOAD


def test_auto_segment_download_uses_dynamic_for_range_server(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="auto",
            retry=0,
            log_path=None,
        )
        manager.append(
            server.url("/bad-content-range.bin"),
            file_name="auto-dynamic-validation.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "auto-dynamic-validation.bin").exists()
        assert manager.results[0].status == TaskStatus.FAILED
        assert "Content-Range start mismatch" in (manager.results[0].error or "")
        assert manager.exit_code == 1


def test_auto_segment_download_falls_back_to_static_for_unknown_size(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="auto",
            log_path=None,
        )
        manager.append(
            server.url("/unknown.bin"),
            file_name="auto-unknown.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "auto-unknown.bin").read_bytes() == UNKNOWN_SIZE_PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_dynamic_segment_download_writes_exact_file_with_two_workers(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="dynamic",
            log_path=None,
        )
        manager.append(
            server.url("/normal.bin"),
            file_name="dynamic-2.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "dynamic-2.bin").read_bytes() == PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_dynamic_segment_download_handles_uneven_file_with_four_workers(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=4,
            min_split_size="1K",
            segment_mode="dynamic",
            log_path=None,
        )
        manager.append(
            server.url("/uneven.bin"),
            file_name="dynamic-uneven.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "dynamic-uneven.bin").read_bytes() == UNEVEN_PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_dynamic_segment_download_rejects_bad_content_range(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="dynamic",
            retry=0,
            log_path=None,
        )
        manager.append(
            server.url("/bad-content-range.bin"),
            file_name="bad-content-range.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "bad-content-range.bin").exists()
        assert manager.results[0].status == TaskStatus.FAILED
        assert "Content-Range start mismatch" in (manager.results[0].error or "")
        assert manager.exit_code == 1


def test_dynamic_failed_download_with_keep_tmp_retains_range_and_resume_metadata(tmp_path):
    with LocalDownloadServer() as server:
        tmp_root = tmp_path / "tmp"
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="dynamic",
            retry=0,
            tmp_dir=str(tmp_root),
            keep_tmp=True,
            log_path=None,
        )
        manager.append(
            server.url("/bad-content-range.bin"),
            file_name="bad-content-range-metadata.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        metadata_files = list(tmp_root.glob(f"**/{DYNAMIC_RANGE_METADATA_FILENAME}"))
        assert len(metadata_files) == 1
        payload = json.loads(metadata_files[0].read_text())
        assert payload["schema_version"] == 1
        assert payload["mode"] == "dynamic"
        assert payload["stats"]["failed_count"] >= 1
        assert any(item["state"] == "failed" for item in payload["ranges"])
        assert any(
            "Content-Range start mismatch" in (item["last_error"] or "")
            for item in payload["ranges"]
        )

        resume_files = list(tmp_root.glob(f"**/{RESUME_METADATA_FILENAME}"))
        assert len(resume_files) == 1
        resume_payload = json.loads(resume_files[0].read_text())
        assert resume_payload["schema_version"] == 2
        assert resume_payload["kind"] == "resume"
        assert resume_payload["mode"] == "dynamic"
        assert resume_payload["file_size"] == len(PAYLOAD)
        assert any(item["state"] == "failed" for item in resume_payload["segments"])
        assert "ranges" not in resume_payload
        assert "stats" not in resume_payload

        assert manager.results[0].status == TaskStatus.FAILED
        assert manager.exit_code == 1


def test_auto_segment_download_metadata_includes_selector_diagnostics(tmp_path):
    with LocalDownloadServer() as server:
        tmp_root = tmp_path / "tmp"
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="auto",
            retry=0,
            tmp_dir=str(tmp_root),
            keep_tmp=True,
            log_path=None,
        )
        manager.append(
            server.url("/bad-content-range.bin"),
            file_name="auto-selector-metadata.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        metadata_files = list(tmp_root.glob(f"**/{DYNAMIC_RANGE_METADATA_FILENAME}"))
        assert len(metadata_files) == 1
        payload = json.loads(metadata_files[0].read_text())
        assert payload["selector"] == {
            "requested_mode": "auto",
            "selected_mode": "dynamic",
            "fallback_reason": None,
            "reason": "dynamic_eligible",
        }
        assert manager.results[0].status == TaskStatus.FAILED
        assert manager.exit_code == 1


def test_dynamic_segment_download_rejects_ignored_range_response(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="dynamic",
            retry=0,
            log_path=None,
        )
        manager.append(
            server.url("/ignored-range.bin"),
            file_name="ignored-range.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "ignored-range.bin").exists()
        assert manager.results[0].status == TaskStatus.FAILED
        assert "HTTP 200 is only valid for full-file range" in (manager.results[0].error or "")
        assert manager.exit_code == 1


def test_dynamic_segment_download_rejects_short_range_body(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="dynamic",
            retry=0,
            log_path=None,
        )
        manager.append(
            server.url("/short-range.bin"),
            file_name="short-range.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "short-range.bin").exists()
        assert manager.results[0].status == TaskStatus.FAILED
        assert "incomplete" in (manager.results[0].error or "")
        assert manager.exit_code == 1


def test_dynamic_segment_download_recovers_flaky_ranges(tmp_path):
    FLAKY_COUNTS.clear()
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=3,
            min_split_size="1K",
            segment_mode="dynamic",
            retry=1,
            retry_wait=0,
            log_path=None,
        )
        manager.append(
            server.url("/flaky-range.bin"),
            file_name="dynamic-flaky-range.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "dynamic-flaky-range.bin").read_bytes() == PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_dynamic_segment_download_fails_after_range_retry_limit(tmp_path):
    FLAKY_COUNTS.clear()
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="dynamic",
            retry=0,
            log_path=None,
        )
        manager.append(
            server.url("/flaky-range.bin"),
            file_name="dynamic-range-failed.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "dynamic-range-failed.bin").exists()
        assert manager.results[0].status == TaskStatus.FAILED
        assert manager.exit_code == 1


def test_dynamic_segment_download_splits_slow_range_once(tmp_path):
    FLAKY_COUNTS.clear()
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=2,
            min_split_size="1K",
            segment_mode="dynamic",
            retry=1,
            retry_wait=0,
            chunk_retry_speed="10M",
            log_path=None,
        )
        manager.append(
            server.url("/slow-range-once.bin"),
            file_name="dynamic-slow-range.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "dynamic-slow-range.bin").read_bytes() == PAYLOAD
        assert FLAKY_COUNTS["/slow-range-once.bin"] > 1
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_dynamic_segment_download_validates_md5(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=4,
            min_split_size="1K",
            segment_mode="dynamic",
            check_integrity=True,
            log_path=None,
        )
        manager.append(
            server.url("/normal.bin"),
            file_name="dynamic-md5.bin",
            dir_path=str(tmp_path),
            md5=hashlib.md5(PAYLOAD).hexdigest(),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "dynamic-md5.bin").read_bytes() == PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_static_continue_uses_resume_metadata_partial(tmp_path):
    with LocalDownloadServer() as server:
        url = server.url("/normal.bin")
        file_name = "resume-static.bin"
        tmp_root = tmp_path / "tmp-root"
        tmp_dir = tmp_root / f".pdman.{hashlib.sha256(url.encode('utf-8')).hexdigest()[:6]}"
        tmp_dir.mkdir(parents=True)
        partial_path = tmp_dir / f"{file_name}.0"
        partial_path.write_bytes(PAYLOAD[:12345])
        (tmp_dir / ".pdm").write_text(
            json.dumps(
                {
                    "url": url,
                    "filename": file_name,
                    "md5": None,
                    "file_size": len(PAYLOAD),
                }
            ),
            encoding="utf-8",
        )
        chunks = [Chunk(None, 0, len(PAYLOAD) - 1, str(partial_path))]
        payload = static_resume_metadata_payload(
            url=url,
            filename=file_name,
            target_path=tmp_path / file_name,
            file_size=len(PAYLOAD),
            chunks=chunks,
        )
        (tmp_dir / RESUME_METADATA_FILENAME).write_text(json.dumps(payload), encoding="utf-8")

        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            continue_download=True,
            tmp_dir=str(tmp_root),
            log_path=None,
        )
        manager.append(url, file_name=file_name, dir_path=str(tmp_path))
        asyncio.run(manager.download())

        assert (tmp_path / file_name).read_bytes() == PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_cli_single_url_out_respects_dir(tmp_path):
    with LocalDownloadServer() as server:
        download_dir = tmp_path / "downloads"
        exit_code = cli.main(
            [
                "-d",
                str(download_dir),
                "-o",
                "renamed.bin",
                server.url("/normal.bin"),
            ]
        )

        assert exit_code == 0
        assert (download_dir / "renamed.bin").read_bytes() == PAYLOAD
        assert not (tmp_path / "renamed.bin").exists()


def test_cli_dynamic_segment_download(tmp_path):
    with LocalDownloadServer() as server:
        exit_code = cli.main(
            [
                "--segment-mode",
                "dynamic",
                "-x",
                "3",
                "-k",
                "1K",
                "-N",
                "1",
                "-d",
                str(tmp_path),
                server.url("/normal.bin"),
            ]
        )

        assert exit_code == 0
        assert (tmp_path / "normal.bin").read_bytes() == PAYLOAD


def test_cli_auto_segment_download(tmp_path):
    with LocalDownloadServer() as server:
        exit_code = cli.main(
            [
                "--segment-mode",
                "auto",
                "-x",
                "3",
                "-k",
                "1K",
                "-N",
                "1",
                "-d",
                str(tmp_path),
                server.url("/normal.bin"),
            ]
        )

        assert exit_code == 0
        assert (tmp_path / "normal.bin").read_bytes() == PAYLOAD


def test_download_writes_runtime_history_and_cleans_active_run(tmp_path):
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            cache_dir=str(cache_dir),
            log_path=None,
        )
        manager.append(
            server.url("/normal.bin"),
            file_name="runtime.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "runtime.bin").read_bytes() == PAYLOAD
        assert not manager.runtime_paths.active_run_path.exists()
        assert manager.runtime_paths.final_run_path.exists()
        assert not manager.runtime_paths.run_dir.exists()
        assert not list(tmp_path.glob(".pdman.*"))
        history_record = json.loads(
            manager.runtime_paths.history_path.read_text().splitlines()[0]
        )
        assert history_record["filename"] == "runtime.bin"
        assert history_record["status"] == "completed"


def test_system_tmp_space_insufficient_records_failed_result(monkeypatch, tmp_path):
    monkeypatch.setattr("pdman.runtime.shutil.disk_usage", fake_disk_usage(10))
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            tmp_policy="system",
            retry=0,
            cache_dir=str(tmp_path / "cache"),
            log_path=None,
        )
        manager.append(
            server.url("/normal.bin"),
            file_name="no-space.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "no-space.bin").exists()
        assert manager.results[0].status == TaskStatus.FAILED
        assert manager.results[0].reason_code == TaskReason.TMP_SPACE_INSUFFICIENT
        assert manager.exit_code == 1
        history_record = json.loads(
            manager.runtime_paths.history_path.read_text().splitlines()[0]
        )
        assert history_record["reason_code"] == "tmp_space_insufficient"


def test_queue_start_downloads_from_local_http_server(tmp_path):
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        download_dir = tmp_path / "downloads"
        add_exit = cli.main(
            [
                "queue",
                "add",
                "--cache-dir",
                str(cache_dir),
                "-d",
                str(download_dir),
                "--file-name",
                "queue.bin",
                server.url("/slow.bin?delay=0.05"),
            ]
        )
        start_exit = cli.main(
            [
                "queue",
                "start",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "-N",
                "1",
                "-x",
                "1",
            ]
        )

        assert add_exit == 0
        assert start_exit == 0
        assert (download_dir / "queue.bin").read_bytes() == PAYLOAD
        records = load_queue(str(cache_dir))
        assert records[0].status == "completed"
        assert records[0].last_run_id is not None
        assert records[0].last_error is None
        assert records[0].attempts == 1


def test_queue_retry_failed_succeeds_against_flaky_local_http_server(tmp_path):
    FLAKY_COUNTS.clear()
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        download_dir = tmp_path / "downloads"
        add_exit = cli.main(
            [
                "queue",
                "add",
                "--cache-dir",
                str(cache_dir),
                "-d",
                str(download_dir),
                "--file-name",
                "flaky.bin",
                server.url("/flaky.bin"),
            ]
        )
        first_exit = cli.main(
            [
                "queue",
                "start",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "--retry",
                "0",
            ]
        )
        first_records = load_queue(str(cache_dir))
        retry_exit = cli.main(
            [
                "queue",
                "retry-failed",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "-N",
                "1",
                "-x",
                "1",
            ]
        )

        assert add_exit == 0
        assert first_exit == 1
        assert first_records[0].status == "failed"
        assert first_records[0].attempts == 1
        assert first_records[0].last_error == "HTTP 503 during header check"
        assert retry_exit == 0
        assert (download_dir / "flaky.bin").read_bytes() == PAYLOAD
        records = load_queue(str(cache_dir))
        assert records[0].status == "completed"
        assert records[0].attempts == 2
        assert records[0].last_error is None
        assert records[0].last_status_reason == "download completed"


def test_queue_retry_failed_max_attempts_blocks_retry(tmp_path):
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        download_dir = tmp_path / "downloads"
        add_exit = cli.main(
            [
                "queue",
                "add",
                "--cache-dir",
                str(cache_dir),
                "-d",
                str(download_dir),
                "--file-name",
                "blocked.bin",
                server.url("/status.bin?status=503"),
            ]
        )
        first_exit = cli.main(
            [
                "queue",
                "start",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "--retry",
                "0",
            ]
        )
        retry_exit = cli.main(
            [
                "queue",
                "retry-failed",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "--max-attempts",
                "1",
                "--retry",
                "0",
            ]
        )

        assert add_exit == 0
        assert first_exit == 1
        assert retry_exit == 0
        records = load_queue(str(cache_dir))
        assert records[0].status == "failed"
        assert records[0].attempts == 1
        assert records[0].last_error == "HTTP 503 during header check"
        assert not (download_dir / "blocked.bin").exists()


def test_queue_retry_failed_error_contains_selects_matching_failure(tmp_path):
    FLAKY_COUNTS.clear()
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        download_dir = tmp_path / "downloads"
        assert cli.main(
            [
                "queue",
                "add",
                "--cache-dir",
                str(cache_dir),
                "-d",
                str(download_dir),
                "--file-name",
                "flaky-select.bin",
                server.url("/flaky.bin"),
            ]
        ) == 0
        assert cli.main(
            [
                "queue",
                "add",
                "--cache-dir",
                str(cache_dir),
                "-d",
                str(download_dir),
                "--file-name",
                "notfound.bin",
                server.url("/status.bin?status=404"),
            ]
        ) == 0
        first_exit = cli.main(
            [
                "queue",
                "start",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "2",
                "-N",
                "2",
                "-x",
                "1",
                "--retry",
                "0",
            ]
        )
        retry_exit = cli.main(
            [
                "queue",
                "retry-failed",
                "--cache-dir",
                str(cache_dir),
                "--error-contains",
                "503",
                "-N",
                "1",
                "-x",
                "1",
            ]
        )

        assert first_exit == 1
        assert retry_exit == 0
        assert (download_dir / "flaky-select.bin").read_bytes() == PAYLOAD
        assert not (download_dir / "notfound.bin").exists()
        records = load_queue(str(cache_dir))
        assert records[0].status == "completed"
        assert records[0].attempts == 2
        assert records[0].last_error is None
        assert records[1].status == "failed"
        assert records[1].attempts == 1
        assert records[1].last_error == "HTTP 404 during header check"


def test_queue_retry_failed_records_second_failure(tmp_path):
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        download_dir = tmp_path / "downloads"
        add_exit = cli.main(
            [
                "queue",
                "add",
                "--cache-dir",
                str(cache_dir),
                "-d",
                str(download_dir),
                "--file-name",
                "failed-retry.bin",
                server.url("/status.bin?status=503"),
            ]
        )
        first_exit = cli.main(
            [
                "queue",
                "start",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "--retry",
                "0",
            ]
        )
        retry_exit = cli.main(
            [
                "queue",
                "retry-failed",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "--retry",
                "0",
            ]
        )

        assert add_exit == 0
        assert first_exit == 1
        assert retry_exit == 1
        assert not (download_dir / "failed-retry.bin").exists()
        records = load_queue(str(cache_dir))
        assert records[0].status == "failed"
        assert records[0].attempts == 2
        assert records[0].last_error == "HTTP 503 during header check"


def test_queue_start_records_failed_local_http_result(tmp_path):
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        download_dir = tmp_path / "downloads"
        add_exit = cli.main(
            [
                "queue",
                "add",
                "--cache-dir",
                str(cache_dir),
                "-d",
                str(download_dir),
                "--file-name",
                "failed.bin",
                server.url("/status.bin?status=503"),
            ]
        )
        start_exit = cli.main(
            [
                "queue",
                "start",
                "--cache-dir",
                str(cache_dir),
                "--limit",
                "1",
                "--retry",
                "0",
            ]
        )

        assert add_exit == 0
        assert start_exit == 1
        assert not (download_dir / "failed.bin").exists()
        records = load_queue(str(cache_dir))
        assert records[0].status == "failed"
        assert records[0].last_run_id is not None
        assert records[0].last_error == "HTTP 503 during header check"


def test_slow_head_is_failed_after_connection_timeout(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            connect_timeout=0.05,
            connect_progress_delay=0.01,
            summary_interval=0.01,
            retry=0,
            log_path=None,
        )
        manager.append(
            server.url("/slow.bin?delay=1"),
            file_name="timeout.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "timeout.bin").exists()
        assert not list(tmp_path.glob(".pdman.*"))
        assert manager.results[0].status == TaskStatus.FAILED
        assert manager.results[0].reason_code == TaskReason.CONNECTION_TIMEOUT
        assert manager.exit_code == 1


def test_unreachable_local_port_is_skipped(tmp_path):
    port = _unused_local_port()
    manager = Manager(
        connect_timeout=0.2,
        connect_progress_delay=0.01,
        summary_interval=0.01,
        retry=0,
        log_path=None,
    )
    manager.append(
        f"http://127.0.0.1:{port}/missing.bin",
        file_name="missing.bin",
        dir_path=str(tmp_path),
    )
    asyncio.run(manager.download())

    assert not (tmp_path / "missing.bin").exists()
    assert not list(tmp_path.glob(".pdman.*"))
    assert manager.results[0].status == TaskStatus.FAILED
    assert manager.results[0].reason_code == TaskReason.CONNECTION_FAILED
    assert manager.exit_code == 1


def test_header_http_status_is_failed_without_raising(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            retry=0,
            log_path=None,
        )
        manager.append(
            server.url("/status.bin?status=503"),
            file_name="unavailable.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert not (tmp_path / "unavailable.bin").exists()
        assert not list(tmp_path.glob(".pdman.*"))
        assert manager.results[0].status == TaskStatus.FAILED
        assert manager.results[0].reason_code == TaskReason.HTTP_STATUS
        assert manager.exit_code == 1


def test_head_405_falls_back_to_get_probe_and_preserves_filename(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            retry=1,
            log_path=None,
        )
        manager.append(
            server.url("/head-405-get-ok.bin"),
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "head-405-get-ok.bin").read_bytes() == PAYLOAD
        assert manager.results[0].filename == "head-405-get-ok.bin"
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.results[0].header_probe_method == "GET"
        assert manager.results[0].header_probe_fallback_reason == "head_http_405"
        assert "Probe:" in manager.summarize_results()
        assert "head_http_405" in manager.summarize_results()
        assert manager.exit_code == 0


def test_head_get_fallback_is_written_to_runtime_history(tmp_path):
    with LocalDownloadServer() as server:
        cache_dir = tmp_path / "cache"
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            cache_dir=str(cache_dir),
            retry=1,
            log_path=None,
        )
        manager.append(
            server.url("/head-405-get-ok.bin"),
            file_name="history-fallback.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        history_record = json.loads(
            manager.runtime_paths.history_path.read_text().splitlines()[0]
        )
        assert history_record["header_probe_method"] == "GET"
        assert history_record["header_probe_fallback_reason"] == "head_http_405"



def test_head_http_fallback_reason_codes_are_stable(tmp_path):
    with LocalDownloadServer() as server:
        for status in (403, 404, 405, 501):
            target_dir = tmp_path / str(status)
            manager = Manager(
                max_downloads=1,
                max_concurrent_downloads=1,
                retry=1,
                log_path=None,
            )
            manager.append(
                server.url(f"/head-status-get-ok.bin?status={status}"),
                file_name=f"fallback-{status}.bin",
                dir_path=str(target_dir),
            )
            asyncio.run(manager.download())

            assert (target_dir / f"fallback-{status}.bin").read_bytes() == PAYLOAD
            assert manager.results[0].status == TaskStatus.COMPLETED
            assert manager.results[0].header_probe_method == "GET"
            assert manager.results[0].header_probe_fallback_reason == f"head_http_{status}"
            assert manager.exit_code == 0



def test_head_501_get_fallback_preserves_file_size_for_dynamic(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=4,
            min_split_size=1024,
            segment_mode="dynamic",
            retry=1,
            log_path=None,
        )
        manager.append(
            server.url("/head-501-get-ok.bin"),
            file_name="fallback-dynamic.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "fallback-dynamic.bin").read_bytes() == PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.results[0].total_bytes == len(PAYLOAD)
        assert manager.exit_code == 0


def test_head_connection_close_falls_back_to_get_probe(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            retry=1,
            log_path=None,
        )
        manager.append(
            server.url("/head-close-get-ok.bin"),
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "head-close-get-ok.bin").read_bytes() == PAYLOAD
        assert manager.results[0].filename == "head-close-get-ok.bin"
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_get_probe_200_ignored_range_preserves_full_content_length(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            retry=1,
            log_path=None,
        )
        manager.append(
            server.url("/head-501-get-ignored-range.bin"),
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "head-501-get-ignored-range.bin").read_bytes() == PAYLOAD
        assert manager.results[0].total_bytes == len(PAYLOAD)
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_get_probe_206_unknown_total_does_not_treat_probe_size_as_file_size(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            retry=1,
            log_path=None,
        )
        manager.append(
            server.url("/head-501-get-unknown-total.bin"),
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "head-501-get-unknown-total.bin").read_bytes() == PAYLOAD
        assert manager.results[0].total_bytes is None
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_unknown_content_length_downloads_successfully(tmp_path):
    with LocalDownloadServer() as server:
        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            log_path=None,
        )
        manager.append(
            server.url("/unknown.bin"),
            file_name="unknown.bin",
            dir_path=str(tmp_path),
        )
        asyncio.run(manager.download())

        assert (tmp_path / "unknown.bin").read_bytes() == UNKNOWN_SIZE_PAYLOAD


def test_yaml_md5_integrity_path_downloads_successfully(tmp_path):
    with LocalDownloadServer() as server:
        download_dir = tmp_path / "downloads"
        md5_file = tmp_path / "normal.md5"
        md5_file.write_text(hashlib.md5(PAYLOAD).hexdigest())
        tasks_file = tmp_path / "tasks-md5.yaml"
        tasks_file.write_text(
            "\n".join(
                [
                    f"{server.url('/normal.bin')}:",
                    "  file_name: checked.bin",
                    f"  dir_path: {download_dir}",
                    f"  md5: {md5_file}",
                ]
            )
        )

        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            min_split_size="1K",
            check_integrity=True,
            log_path=None,
        )
        manager.load_input_file(str(tasks_file))
        asyncio.run(manager.download())

        assert (download_dir / "checked.bin").read_bytes() == PAYLOAD
        assert manager.results[0].status == TaskStatus.COMPLETED
        assert manager.exit_code == 0


def test_yaml_md5_integrity_mismatch_records_failed_result(tmp_path):
    with LocalDownloadServer() as server:
        download_dir = tmp_path / "downloads"
        md5_file = tmp_path / "mismatch.md5"
        md5_file.write_text("1" * 32)
        tasks_file = tmp_path / "tasks-md5-mismatch.yaml"
        tasks_file.write_text(
            "\n".join(
                [
                    f"{server.url('/normal.bin')}:",
                    "  file_name: md5-mismatch.bin",
                    f"  dir_path: {download_dir}",
                    f"  md5: {md5_file}",
                ]
            )
        )

        manager = Manager(
            max_downloads=1,
            max_concurrent_downloads=1,
            min_split_size="1K",
            check_integrity=True,
            log_path=None,
        )
        manager.load_input_file(str(tasks_file))
        asyncio.run(manager.download())

        assert manager.results[0].status == TaskStatus.FAILED
        assert manager.results[0].reason_code == TaskReason.INTEGRITY_MISMATCH
        assert manager.exit_code == 1
