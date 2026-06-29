import asyncio
import hashlib
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from pdman.manager import Manager
from pdman.status import TaskReason, TaskStatus

PAYLOAD = (b"pdman-local-test-" * 8192) + b"end"
UNKNOWN_SIZE_PAYLOAD = b"unknown-size-body" * 1024
REQUIRED_UA = "PDMAN-Integration-Test/1.0"


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
        self._send_text(404, b"not found")

    def _send_text(self, status: int, body: bytes):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_payload(self, data: bytes, filename: str, send_body: bool):
        range_header = self.headers.get("Range")
        status = 200
        start = 0
        end = len(data) - 1
        if send_body and range_header:
            unit, raw_range = range_header.split("=", 1)
            assert unit == "bytes"
            raw_start, raw_end = raw_range.split("-", 1)
            start = int(raw_start)
            end = int(raw_end) if raw_end else len(data) - 1
            end = min(end, len(data) - 1)
            status = 206

        body = data[start:end + 1]
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

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
