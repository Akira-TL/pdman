#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
CLI 入口：提供命令行解析并调用 PDManager。
安装后可通过 console_scripts 生成可执行命令。
"""

import os
import sys
import argparse
import asyncio
from importlib.metadata import PackageNotFoundError, version as package_version

from .manager import Manager


def get_version() -> str:
    try:
        return package_version("pdman")
    except PackageNotFoundError:
        return "unknown"


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"PythonDownloadManager(PDMAN) version {get_version()}",
        help="Print the version number and exit.",
    )
    parser.add_argument(
        "-l",
        "--log",
        type=str,
        required=False,
        default=None,
        help="The file name of the log file. If '-' is specified, log is written to stdout.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode with verbose logging.",
    )
    parser.add_argument(
        "-d",
        "--dir",
        type=str,
        default=os.path.join(os.getcwd(), "pdman"),
        help="The directory to store the downloaded file.",
    )
    parser.add_argument(
        "-o",
        "--out",
        type=str,
        default=None,
        help="The file name of the downloaded file. It is always relative to the directory given in -d option. When the -Z option is used, this option will be ignored.",
    )
    parser.add_argument(
        "-V",
        "--check-integrity",
        action="store_true",
        help="Check file integrity by validating piece hashes or a hash of the entire file.",
    )
    parser.add_argument(
        "-c",
        "--continue",
        dest="continue_download",
        action="store_true",
        help="Continue downloading a partially downloaded file.",
    )
    parser.add_argument(
        "-i",
        "--input-file",
        type=str,
        default=[],
        action="append",
        help="Downloads URIs found in FILE(s). Supports JSON, YAML, or plain text.",
    )
    parser.add_argument(
        "-x",
        "--max-concurrent-downloads",
        type=int,
        default=5,
        help="Set maximum number of parallel downloads for each URL or task.",
    )
    parser.add_argument(
        "--chunk-retry-speed",
        default="",
        help="If the chunk speed falls below SIZE bytes/second, restart that chunk. Append K/M.",
    )
    parser.add_argument(
        "-r",
        "--retry",
        type=int,
        default=3,
        help="Number of times to retry downloading a URL upon failure.",
    )
    parser.add_argument(
        "-W",
        "--retry-wait",
        type=int,
        default=5,
        help="Maximum wait time in seconds between retries.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout in seconds for each download request.",
    )
    parser.add_argument(
        "--chunk-timeout",
        type=int,
        default=None,
        help="Timeout in seconds for each chunk download request.",
    )
    parser.add_argument(
        "-N",
        "--max-downloads",
        type=int,
        default=4,
        help="The maximum number of concurrent downloads.",
    )
    parser.add_argument(
        "--no-auto-file-renaming",
        action="store_false",
        help="Disable auto renaming when target file exists.",
    )
    parser.add_argument(
        "-Z",
        "--force-sequential",
        action="store_true",
        help="Fetch URIs sequentially.",
    )
    parser.add_argument(
        "-k",
        "--min-split-size",
        type=str,
        default="1M",
        help="Minimum split size. Append K/M.",
    )
    parser.add_argument(
        "--tmp",
        type=str,
        default=None,
        help="Temporary directory for chunk files.",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=4,
        help="Alias of max-downloads (deprecated).",
    )
    parser.add_argument(
        "-ua",
        "--user-agent",
        type=str,
        default="PDMAN-Downloader/1.0",
        help="The User-Agent string to use for HTTP requests.",
    )
    # === 认证与 Cookie ===
    parser.add_argument(
        "--http-auth",
        type=str,
        default=None,
        help="HTTP authentication credentials in user:pass format.",
    )
    parser.add_argument(
        "--cookie-file",
        type=str,
        default=None,
        help="Load cookies from a Netscape/Mozilla format file.",
    )
    # === 限速 ===
    parser.add_argument(
        "--max-download-limit",
        type=str,
        default=None,
        help="Max download speed per task (bytes/sec). Append K or M.",
    )
    parser.add_argument(
        "--max-overall-download-limit",
        type=str,
        default=None,
        help="Max overall download speed (bytes/sec). Append K or M.",
    )
    # === 代理 ===
    parser.add_argument(
        "--proxy",
        type=str,
        default=None,
        help="HTTP/HTTPS proxy URL (e.g., http://127.0.0.1:8080).",
    )
    parser.add_argument(
        "--proxy-auth",
        type=str,
        default=None,
        help="Proxy authentication credentials in user:pass format.",
    )
    # === 请求头与超时 ===
    parser.add_argument(
        "--header",
        type=str,
        default=[],
        action="append",
        help="Add custom HTTP header (Key: Value). Repeatable.",
    )
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=30,
        help="Connection timeout in seconds before skipping the URL.",
    )
    parser.add_argument(
        "--connect-progress-delay",
        type=float,
        default=5.0,
        help="Seconds to wait before showing an indeterminate connection progress indicator.",
    )
    parser.add_argument(
        "--max-connection-per-server",
        type=int,
        default=0,
        help="Max connections per server. 0 means unlimited.",
    )
    parser.add_argument(
        "--referer",
        type=str,
        default=None,
        help="Set HTTP Referer header.",
    )
    # === 回调 ===
    parser.add_argument(
        "--on-download-complete",
        type=str,
        default=None,
        help="Shell command to run after download completes. Supports {filename}/{filepath}/{url}/{dir}/{size} placeholders.",
    )
    # === SSL ===
    parser.add_argument(
        "--no-check-certificate",
        dest="check_certificate",
        action="store_false",
        help="Do not verify SSL certificates.",
    )
    parser.add_argument(
        "--ca-certificate",
        type=str,
        default=None,
        help="Path to custom CA certificate file.",
    )
    # === 其他 ===
    parser.add_argument(
        "--conf-path",
        type=str,
        default=None,
        help="Path to config file (JSON or YAML).",
    )
    parser.add_argument(
        "-q",
        "--quit",
        dest="quit_if_exists",
        action="store_true",
        help="Quit if the target file already exists.",
    )
    parser.add_argument(
        "--summary-interval",
        type=float,
        default=1.0,
        help="Progress summary output interval in seconds.",
    )
    parser.add_argument(
        "urls",
        type=str,
        nargs="*",
        default=None,
        help="The URL(s) to download.",
    )

    args = parser.parse_args(argv)
    if args.log == "-":
        args.log = sys.stdout
    if args.force_sequential and args.out is not None:
        args.out = None

    pdman = Manager(
        max_downloads=args.max_downloads,
        log_path=args.log,
        debug=args.debug,
        continue_download=args.continue_download,
        max_concurrent_downloads=args.max_concurrent_downloads,
        min_split_size=args.min_split_size,
        force_sequential=args.force_sequential,
        tmp_dir=args.tmp,
        check_integrity=args.check_integrity,
        user_agent=args.user_agent,
        chunk_retry_speed=args.chunk_retry_speed,
        retry=args.retry,
        retry_wait=args.retry_wait,
        timeout=args.timeout,
        chunk_timeout=args.chunk_timeout,
        auto_file_renaming=args.no_auto_file_renaming,
        out_dir=args.dir,
        # 认证与 Cookie
        http_auth=args.http_auth,
        cookie_file=args.cookie_file,
        # 限速
        max_download_limit=args.max_download_limit,
        max_overall_download_limit=args.max_overall_download_limit,
        # 代理
        proxy=args.proxy,
        proxy_auth=args.proxy_auth,
        # 请求头与超时
        headers=args.header if args.header else None,
        connect_timeout=args.connect_timeout,
        connect_progress_delay=args.connect_progress_delay,
        max_connection_per_server=args.max_connection_per_server,
        referer=args.referer,
        # 回调
        on_download_complete=args.on_download_complete,
        # SSL
        check_certificate=args.check_certificate,
        ca_certificate=args.ca_certificate,
        # 其他
        conf_path=args.conf_path,
        quit_if_exists=args.quit_if_exists,
        summary_interval=args.summary_interval,
    )

    if args.urls and len(args.urls) == 1 and args.out is not None:
        pdman.append(args.urls[0], file_name=args.out)
    else:
        if args.out is not None:
            pass  # ignore --out when multiple urls
        pdman.add_urls(args.urls or [])

    if args.input_file:
        for file in args.input_file:
            if os.path.exists(file):
                pdman.load_input_file(file)
    try:
        asyncio.run(pdman.download())
    except KeyboardInterrupt:
        print("\033[31mDownload interrupted by user.")


if __name__ == "__main__":
    main()
