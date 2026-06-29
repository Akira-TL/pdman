#!/usr/bin/env python
"""Manual local HTTP server for exercising pdman download behavior.

This script is intentionally not named test_*.py so pytest will not collect it.
It serves deterministic byte streams with Range support and several edge-case
routes for manual CLI testing.
"""

from __future__ import annotations

import argparse
import hashlib
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DEFAULT_UA = "PDMAN-Manual-Test/1.0"
PATTERN = b"pdman-manual-download-test-0123456789\n"
RESPONSE_BLOCK_SIZE = 256 * 1024
DEFAULT_MAX_RATE = "5M"


def parse_size(value: str) -> int:
    raw = str(value).strip().upper()
    units = {"K": 1024, "M": 1024**2, "G": 1024**3}
    if raw[-1:] in units:
        return int(float(raw[:-1]) * units[raw[-1]])
    return int(raw)


def iter_bytes(start: int, length: int, block_size: int = RESPONSE_BLOCK_SIZE):
    """Yield deterministic bytes from a virtual file without storing it all."""
    sent = 0
    pattern_len = len(PATTERN)
    while sent < length:
        current_start = start + sent
        chunk_len = min(block_size, length - sent)
        offset = current_start % pattern_len
        chunk = (PATTERN[offset:] + PATTERN * ((chunk_len + offset) // pattern_len + 1))[:chunk_len]
        yield chunk
        sent += chunk_len


def md5_for_size(size: int) -> str:
    digest = hashlib.md5()
    for chunk in iter_bytes(0, size):
        digest.update(chunk)
    return digest.hexdigest()


class ManualDownloadHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    default_size = 0
    slow_delay = 10.0
    required_ua = DEFAULT_UA
    throttle_sleep = RESPONSE_BLOCK_SIZE / parse_size(DEFAULT_MAX_RATE)

    def log_message(self, format, *args):
        print(f"[{self.log_date_time_string()}] {self.address_string()} {format % args}")

    def do_HEAD(self):
        self._handle(send_body=False)

    def do_GET(self):
        self._handle(send_body=True)

    def _handle(self, send_body: bool):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        size = parse_size(query.get("size", [str(self.default_size)])[0])

        if parsed.path == "/normal.bin":
            self._send_virtual_file(size, "normal.bin", send_body)
            return

        if parsed.path == "/slow.bin":
            if not send_body:
                delay = float(query.get("delay", [str(self.slow_delay)])[0])
                time.sleep(delay)
            self._send_virtual_file(size, "slow.bin", send_body)
            return

        if parsed.path == "/ua.bin":
            if self.headers.get("User-Agent") != self.required_ua:
                self._send_text(403, b"user-agent required\n")
                return
            self._send_virtual_file(size, "ua.bin", send_body)
            return

        if parsed.path == "/unknown.bin":
            self._send_unknown_size(size, send_body)
            return

        if parsed.path == "/md5.txt":
            body = md5_for_size(size).encode() + b"\n"
            self._send_text(200, body)
            return

        self._send_text(404, b"not found\n")

    def _send_text(self, status: int, body: bytes):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        self.close_connection = True

    def _send_virtual_file(self, size: int, filename: str, send_body: bool):
        range_header = self.headers.get("Range")
        status = 200
        start = 0
        end = size - 1

        if send_body and range_header:
            unit, raw_range = range_header.split("=", 1)
            if unit != "bytes":
                self._send_text(416, b"unsupported range unit\n")
                return
            raw_start, raw_end = raw_range.split("-", 1)
            start = int(raw_start)
            end = int(raw_end) if raw_end else size - 1
            end = min(end, size - 1)
            status = 206

        if size == 0 or start >= size:
            body_len = 0
            status = 416 if range_header else 200
        else:
            body_len = end - start + 1

        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(body_len))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()

        if send_body and body_len > 0:
            for chunk in iter_bytes(start, body_len):
                self.wfile.write(chunk)
                if self.throttle_sleep > 0:
                    time.sleep(self.throttle_sleep)

    def _send_unknown_size(self, size: int, send_body: bool):
        self.send_response(200)
        self.send_header("Content-Disposition", 'attachment; filename="unknown.bin"')
        self.send_header("Connection", "close")
        self.end_headers()
        if send_body:
            for chunk in iter_bytes(0, size):
                self.wfile.write(chunk)
                if self.throttle_sleep > 0:
                    time.sleep(self.throttle_sleep)
        self.close_connection = True


def unused_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def write_yaml(path: Path, base_url: str, output_dir: Path, size: str, slow_delay: float, ua: str):
    content = f"""{base_url}/normal.bin?size={size}:
  file_name: manual-normal.bin
  dir_path: {output_dir}

{base_url}/slow.bin?delay={slow_delay}&size={size}:
  file_name: manual-slow.bin
  dir_path: {output_dir}

{base_url}/ua.bin?size={size}:
  file_name: manual-ua.bin
  dir_path: {output_dir}

{base_url}/unknown.bin?size={size}:
  file_name: manual-unknown.bin
  dir_path: {output_dir}

{base_url}/normal.bin?size={size}:
  file_name: manual-md5.bin
  dir_path: {output_dir}
  md5: {base_url}/md5.txt?size={size}
"""
    path.write_text(content, encoding="utf-8")
    print(f"\nYAML written to: {path}")
    print("Run all manual routes with:")
    print(f"  uv run pdman -N 1 -x 1 --check-integrity --user-agent {ua!r} -i {path}")


def main():
    parser = argparse.ArgumentParser(description="Start a manual pdman local download server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0, help="0 means choose a free port.")
    parser.add_argument("--size", default="50M", help="Virtual file size, e.g. 10M, 512K, 1G.")
    parser.add_argument("--slow-delay", type=float, default=10.0, help="HEAD delay for /slow.bin.")
    parser.add_argument("--ua", default=DEFAULT_UA, help="Required User-Agent for /ua.bin.")
    parser.add_argument(
        "--max-rate",
        default=DEFAULT_MAX_RATE,
        help="Maximum response speed per connection, e.g. 5M, 512K. Use 0 to disable throttling.",
    )
    parser.add_argument(
        "--throttle-sleep",
        type=float,
        default=None,
        help="Override sleep after each 256 KiB response chunk.",
    )
    parser.add_argument(
        "--write-yaml",
        type=Path,
        default=None,
        help="Write a pdman YAML task file for the printed routes.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/pdman-manual-downloads"),
        help="Output dir embedded in generated YAML.",
    )
    args = parser.parse_args()

    size_bytes = parse_size(args.size)
    ManualDownloadHandler.default_size = size_bytes
    ManualDownloadHandler.slow_delay = args.slow_delay
    ManualDownloadHandler.required_ua = args.ua
    max_rate = parse_size(args.max_rate)
    throttle_sleep = 0.0 if max_rate <= 0 else RESPONSE_BLOCK_SIZE / max_rate
    if args.throttle_sleep is not None:
        throttle_sleep = args.throttle_sleep
    ManualDownloadHandler.throttle_sleep = throttle_sleep

    server = ThreadingHTTPServer((args.host, args.port), ManualDownloadHandler)
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    unreachable_port = unused_local_port()

    print("pdman manual download server started")
    print(f"Base URL:        {base_url}")
    print(f"Virtual size:    {args.size} ({size_bytes} bytes)")
    print(f"Slow HEAD delay: {args.slow_delay}s")
    print(f"Required UA:     {args.ua}")
    print(f"Max rate:        {'unlimited' if max_rate <= 0 else args.max_rate + '/s'}")
    print(f"Throttle sleep:  {throttle_sleep:.4f}s per {RESPONSE_BLOCK_SIZE // 1024} KiB")
    print("\nRoutes:")
    print(f"  normal:       {base_url}/normal.bin?size={args.size}")
    print(f"  slow HEAD:    {base_url}/slow.bin?delay={args.slow_delay}&size={args.size}")
    print(f"  UA required:  {base_url}/ua.bin?size={args.size}")
    print(f"  unknown size: {base_url}/unknown.bin?size={args.size}")
    print(f"  md5:          {base_url}/md5.txt?size={args.size}")
    print(f"  unreachable:  http://127.0.0.1:{unreachable_port}/missing.bin")
    print("\nExample commands:")
    print(
        f"  uv run pdman -N 1 -x 1 --connect-timeout 30 --connect-progress-delay 5 "
        f"{base_url}/slow.bin?delay={args.slow_delay}\\&size={args.size}"
    )
    print(
        f"  uv run pdman -N 1 -x 1 --user-agent {args.ua!r} "
        f"{base_url}/ua.bin?size={args.size}"
    )
    print(
        f"  uv run pdman -N 1 -x 1 --connect-timeout 5 --connect-progress-delay 1 "
        f"http://127.0.0.1:{unreachable_port}/missing.bin"
    )

    if args.write_yaml:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        write_yaml(args.write_yaml, base_url, args.output_dir, args.size, args.slow_delay, args.ua)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        print("\nPress Ctrl+C to stop the server.")
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
