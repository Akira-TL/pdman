import re
import os
import json
import time
import math
import shutil
import asyncio
import hashlib
import aiohttp
import aiofiles
import traceback
from yarl import URL
from glob import glob
from rich.text import Text
from urllib.parse import unquote
from pathlib import Path
from loguru._logger import Logger, Core

from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .manager import Manager
from .chunk import Chunk, STREAM_CHUNK_SIZE
from .range_allocator import RangeAllocator, choose_dynamic_range_size
from .range_metadata import DYNAMIC_RANGE_METADATA_FILENAME, write_range_metadata
from .range_response import RangeResponseValidationError, validate_range_response
from .range_task import RangeTask
from .runtime import TmpSpaceInsufficient
from .status import TaskReason, TaskResult, TaskStatus


class ConnectionTimeoutSkip(Exception):
    def __init__(
        self,
        message: str,
        reason_code: TaskReason = TaskReason.CONNECTION_TIMEOUT,
    ):
        self.reason_code = reason_code
        super().__init__(message)


class HeaderStatusSkip(Exception):
    RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}

    def __init__(self, url: str, status: int, reason: str | None = None):
        self.url = url
        self.status = status
        self.reason = reason or ""
        super().__init__(self.describe())

    @property
    def can_retry(self) -> bool:
        return self.status in self.RETRYABLE_STATUS_CODES

    def summary(self) -> str:
        return f"HTTP {self.status} during header check"

    def describe(self) -> str:
        reason = f" {self.reason}" if self.reason else ""
        return (
            f"Remote server returned HTTP {self.status}{reason} "
            f"while checking headers for {self.url}"
        )


class RangeResponseError(Exception):
    def __init__(
        self,
        task: RangeTask,
        message: str,
        *,
        status: int | None = None,
        content_range: str | None = None,
    ):
        self.task = task
        self.status = status
        self.content_range = content_range
        detail = f"range {task.start}-{task.end}: {message}"
        if status is not None:
            detail += f"; status={status}"
        if content_range is not None:
            detail += f"; Content-Range={content_range!r}"
        super().__init__(detail)


class SlowRangeError(Exception):
    def __init__(self, task: RangeTask, speed_bps: float, threshold_bps: int):
        self.task = task
        self.speed_bps = speed_bps
        self.threshold_bps = threshold_bps
        super().__init__(
            f"range {task.start}-{task.end} speed {speed_bps:.1f} B/s "
            f"below threshold {threshold_bps} B/s"
        )


class IntegrityCheckFailure(Exception):
    def __init__(self, filename: str, expected: str, actual: str):
        self.filename = filename
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"MD5 mismatch for {filename}. Expected: {expected}, got: {actual}"
        )


@dataclass(frozen=True)
class DynamicSegmentDecision:
    use_dynamic: bool
    reason: str


class Downloader:
    def __init__(
        self,
        parent,
        url,
        filepath,
        filename: str = None,
        md5=None,
        pdm_tmp=None,
        log_path=None,
    ):
        self.parent: Manager = parent
        self.url = url
        self.filepath = filepath
        self.filename = filename
        self.md5 = md5
        self.pdm_tmp = pdm_tmp
        self.file_size: int = 0
        self.chunk_root: Chunk | None = None
        self.range_allocator: RangeAllocator | None = None
        self.downloaded_bytes: int = 0
        self.lock = asyncio.Lock()
        self.header_info = None
        self.log_path = log_path
        self._downloaded = False
        self._done = False
        self.status = TaskStatus.PENDING
        self.status_reason: str | None = None
        self.status_reason_code: TaskReason | None = None
        self.status_error: str | None = None
        self.result: TaskResult | None = None
        self.segment_decision_reason: str | None = None
        self._logger = Logger(
            core=Core(),
            exception=None,
            depth=0,
            record=False,
            lazy=False,
            colors=False,
            raw=False,
            capture=True,
            patchers=[],
            extra={},
        )

    async def parse_config(self):
        sha = hashlib.sha256(self.url.encode("utf-8")).hexdigest()[:6]
        if self.log_path is None and self.parent.log_path is not None:
            self.log_path = os.path.join(self.filepath, f".pdman.{sha}.log")
        self._logger.remove()
        self._logger.add(
            lambda msg: self.parent._console.print(Text.from_ansi(str(msg)), end="\n"),
            level="DEBUG" if self.parent.debug else "INFO",
            diagnose=True,
            colorize=True,
            format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
        )
        if self.log_path is not None:
            self._logger.add(
                self.log_path,
                level="DEBUG" if self.parent.debug else "INFO",
                diagnose=True,
                colorize=True,
                format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
            )
        if self.md5 is not None:
            if self.md5.find("*") == 0:
                self.md5 = self.md5.replace("*", self.url)
            self.md5 = await self.process_md5(self.md5)
            # self._logger
        self.header_info = await self._await_connection(
            self.get_headers(), label=self.url
        )
        self.filename = self.filename if self.filename else await self.get_file_name()
        os.makedirs(self.filepath, exist_ok=True)
        self.file_size = self.file_size or await self.get_url_file_size()
        if self.pdm_tmp is None:
            tmp_decision = self.parent.resolve_task_tmp_decision(
                task_id=sha,
                target_dir=self.filepath,
                file_size=self.file_size,
            )
            self.pdm_tmp = str(tmp_decision.selected_dir)
            if tmp_decision.fallback_used:
                self._logger.warning(
                    "System temporary directory has insufficient free space; "
                    f"falling back to target tmp directory: {self.pdm_tmp}"
                )
        try:
            os.makedirs(self.pdm_tmp, exist_ok=True)
        except OSError as e:
            raise TmpSpaceInsufficient(
                Path(self.pdm_tmp), None, None, "mkdir"
            ) from e
        self.creat_info()
        self.chunk_root = await self.rebuild_task()
        if self.chunk_root is None:
            self.chunk_root = await self.build_task()
        self.refresh_downloaded_bytes()
        # 单任务限速器（在 Manager 全局限速之外）
        self._per_task_limiter = None
        if self.parent.max_download_limit:
            from .manager import RateLimiter
            self._per_task_limiter = RateLimiter(self.parent.max_download_limit)


    def set_status(
        self,
        status: TaskStatus,
        reason: str | None = None,
        reason_code: TaskReason | None = None,
        error: str | None = None,
    ) -> None:
        self.status = status
        if reason is not None:
            self.status_reason = reason
        if reason_code is not None:
            self.status_reason_code = reason_code
        if error is not None:
            self.status_error = error

    def _result_bytes(self) -> int:
        target_path = self.target_path_if_named()
        if target_path and os.path.exists(target_path):
            return os.path.getsize(target_path)
        return self.downloaded_bytes

    def record_result(
        self,
        status: TaskStatus,
        reason: str | None = None,
        reason_code: TaskReason | None = None,
        error: str | None = None,
    ) -> TaskResult:
        self.set_status(status, reason, reason_code, error)
        self.result = TaskResult(
            url=self.url,
            filename=self.filename,
            status=status,
            reason=reason or self.status_reason,
            reason_code=reason_code or self.status_reason_code,
            error=error or self.status_error,
            downloaded_bytes=self._result_bytes(),
            total_bytes=self.file_size if self.file_size > 0 else None,
        )
        return self.result

    def refresh_downloaded_bytes(self) -> int:
        self.downloaded_bytes = sum(self.chunk_root) if self.chunk_root else 0
        return self.downloaded_bytes

    def _dynamic_segment_decision(self) -> DynamicSegmentDecision:
        mode = self.parent.segment_mode
        if mode == "static":
            return DynamicSegmentDecision(False, "segment_mode_static")
        if self.parent.continue_download:
            return DynamicSegmentDecision(False, "continue_not_supported")
        if self.file_size <= 0:
            return DynamicSegmentDecision(False, "unknown_file_size")
        accept_ranges = ""
        if self.header_info is not None:
            accept_ranges = str(self.header_info.get("Accept-Ranges", "")).lower()
        if accept_ranges != "bytes":
            return DynamicSegmentDecision(False, "accept_ranges_not_bytes")
        if self.parent.force_sequential:
            return DynamicSegmentDecision(False, "force_sequential_enabled")
        if self.parent.max_concurrent_downloads <= 1:
            return DynamicSegmentDecision(False, "insufficient_workers")
        min_split_size = self.parent.min_split_size or self.file_size
        if self.file_size < min_split_size * 2:
            return DynamicSegmentDecision(False, "file_too_small")
        return DynamicSegmentDecision(True, "dynamic_eligible")

    def _can_use_dynamic_segments(self) -> bool:
        decision = self._dynamic_segment_decision()
        self.segment_decision_reason = decision.reason
        mode = self.parent.segment_mode
        if decision.use_dynamic:
            if mode == "auto":
                self._logger.info(
                    "Auto segment mode selected dynamic: dynamic_eligible"
                )
            return True
        if mode in {"dynamic", "auto"}:
            self._logger.info(
                f"Dynamic segment mode fallback to static: {decision.reason}"
            )
        return False

    def _build_range_allocator(self) -> RangeAllocator:
        worker_count = max(1, self.parent.max_concurrent_downloads)
        min_split_size = self.parent.min_split_size or self.file_size
        range_size = choose_dynamic_range_size(
            file_size=self.file_size,
            min_split_size=min_split_size,
            worker_count=worker_count,
        )
        allocator = RangeAllocator(
            file_size=self.file_size,
            range_size=range_size,
            tmp_dir=self.pdm_tmp,
            filename=self.filename,
            max_retries=self.parent.retry,
        )
        self._logger.debug(
            "Dynamic segment mode enabled: "
            f"file_size={self.file_size}, range_size={allocator.range_size}, "
            f"workers={worker_count}, ranges={allocator.total_ranges}"
        )
        return allocator

    def build_request_headers(self) -> dict[str, str]:
        headers = {"Accept-Encoding": "identity"}
        if isinstance(self.parent.user_agent, dict):
            headers.update(self.parent.user_agent)
        if self.parent.headers_dict:
            headers.update(self.parent.headers_dict)
        if self.parent.referer:
            headers.setdefault("Referer", self.parent.referer)
        return headers

    async def _await_connection(self, awaitable, label: str):
        self.set_status(TaskStatus.CONNECTING)
        timeout = float(self.parent.connect_timeout or 30)
        delay = min(float(self.parent.connect_progress_delay or 0), timeout)
        started_at = time.monotonic()
        task = asyncio.create_task(awaitable)
        progress_task = None
        try:
            done, _ = await asyncio.wait({task}, timeout=delay)
            if done:
                return await task
            while not task.done():
                elapsed = time.monotonic() - started_at
                remaining = max(0, math.ceil(timeout - elapsed))
                if remaining <= 0:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                    raise ConnectionTimeoutSkip(
                        f"Connection to {label} timed out after {int(timeout)}s"
                    )
                description = f"Connecting {label} ({remaining}s left)"
                if progress_task is None:
                    progress_task = self.parent._progress.add_task(
                        description, total=None, dl="wait"
                    )
                else:
                    self.parent._progress.update(
                        progress_task, description=description, dl="wait"
                    )
                await asyncio.sleep(min(self.parent.summary_interval, 1.0, remaining))
            return await task
        except (asyncio.TimeoutError, aiohttp.ServerTimeoutError) as e:
            raise ConnectionTimeoutSkip(
                f"Connection to {label} timed out after {int(timeout)}s"
            ) from e
        except aiohttp.ClientConnectionError as e:
            raise ConnectionTimeoutSkip(
                f"Connection to {label} failed",
                TaskReason.CONNECTION_FAILED,
            ) from e
        finally:
            if progress_task is not None:
                self.parent._progress.remove_task(progress_task)

    def _build_client_session(self, **overrides) -> aiohttp.ClientSession:
        """构建统一配置的 aiohttp.ClientSession，用于所有 HTTP 请求"""
        mgr = self.parent  # Manager 引用

        # 超时配置
        timeout = aiohttp.ClientTimeout(
            total=mgr.timeout,
            connect=mgr.connect_timeout,
            sock_read=mgr.chunk_timeout or 30,
        )

        # SSL 配置
        ssl_context = None
        if mgr.ca_certificate:
            import ssl as _ssl
            ssl_context = _ssl.create_default_context(cafile=mgr.ca_certificate)

        # 连接器（支持单 host 连接数限制）
        connector_kw = {
            "limit": 0,
            "verify_ssl": mgr.check_certificate,
            "ssl": ssl_context,
        }
        if mgr.max_connection_per_server and mgr.max_connection_per_server > 0:
            connector_kw["limit_per_host"] = mgr.max_connection_per_server
        connector = aiohttp.TCPConnector(
            **{k: v for k, v in connector_kw.items() if v is not None}
        )

        # Cookie
        cookie_jar = aiohttp.CookieJar()
        if mgr.cookie_file and os.path.exists(mgr.cookie_file):
            cookie_jar.load(mgr.cookie_file)

        # 组装 ClientSession 参数
        kwargs = {
            "timeout": timeout,
            "connector": connector,
            "cookie_jar": cookie_jar,
            "proxy": mgr.proxy,
        }
        if mgr.http_auth:
            kwargs["auth"] = mgr.http_auth
        if mgr.proxy_auth:
            kwargs["proxy_auth"] = mgr.proxy_auth
        kwargs.update(overrides)
        return aiohttp.ClientSession(
            **{k: v for k, v in kwargs.items() if v is not None}
        )

    def __str__(self):
        chunks = []
        for chunk in self.chunk_root:
            chunks.append(str(chunk))
        chunks_str = "\n".join(chunks)
        return (
            f"Downloader(url={self.url}, filepath={self.filepath}, "
            f"filename={self.filename}, md5={self.md5}, "
            f"pdm_tmp={self.pdm_tmp}, file_size={self.file_size})\n"
            f"{chunks_str}"
        )

    async def process_md5(self, md5):
        if md5 is None:
            return None
        elif os.path.exists(md5):
            async with aiofiles.open(md5, "r") as f:
                md5 = await f.read()
                return md5.strip()
        elif re.match(r"^(http|https|ftp)://", md5):
            async with self._build_client_session() as session:
                async with session.get(
                    md5, timeout=self.parent.timeout
                ) as md5_response:
                    if md5_response.status == 200:
                        md5_value = await md5_response.text()
                        return md5_value.strip()
                    else:
                        self._logger.error(
                            f"Failed to fetch md5 from url: {md5}, status code: {md5_response.status}"
                        )
        elif len(md5) == 32 and re.match(r"^[a-fA-F0-9]{32}$", md5):
            return md5.lower()
        else:
            self._logger.error(f"Invalid md5 value: {md5}")
            return None

    async def get_file_name(self) -> str:
        async with self._build_client_session() as session:
            cd = self.header_info.get("Content-Disposition")
            if cd:
                fname = re.findall('.*filename="*(.+)".*', cd)
                fname = unquote(fname[0]) if fname else None
                if fname:
                    return fname
            fname = os.path.basename(URL(self.url).path)
            if fname == "":
                fname = (
                    f"{hashlib.sha256(self.url.encode('utf-8')).hexdigest()[:6]}.dat"
                )
                self._logger.warning(
                    f"Cannot get filename from URL, use hash url as filename: {fname}"
                )
            return fname

    async def get_headers(self) -> dict:
        self.set_status(TaskStatus.HEADER_CHECKING)
        async with self._build_client_session() as session:
            headers_to_send = self.build_request_headers()
            async with session.head(
                self.url,
                allow_redirects=True,
                timeout=self.parent.timeout,
                headers=headers_to_send,
            ) as response:
                if response.status in (200, 206):
                    return response.headers
                raise HeaderStatusSkip(
                    self.url,
                    response.status,
                    getattr(response, "reason", None),
                )

    async def get_url_file_size(self) -> int:
        file_size = None
        if self.header_info is not None:
            file_size = self.header_info.get("Content-Length")
        if file_size:
            return int(file_size)
        else:
            return -1

    def get_file_size(self) -> int:
        return self.file_size

    async def build_task(self):
        if self.file_size < 0:
            return Chunk(
                self,
                0,
                None,
                os.path.join(self.pdm_tmp, f"{self.filename}.0"),
            )
        chunk_size = self.file_size // self.parent.max_concurrent_downloads
        if chunk_size < self.parent.min_split_size:
            chunk_size = self.parent.min_split_size
        elif chunk_size // 10240:
            chunk_size -= chunk_size % 10240
        starts = list(range(0, self.file_size, chunk_size))
        if starts[-1] < self.parent.min_split_size and len(starts) > 1:
            starts.pop()
        root = None
        for i in range(len(starts)):
            start = starts[i]
            end = starts[i + 1] - 1 if i + 1 < len(starts) else self.file_size - 1
            if root is None:
                root = cur = Chunk(
                    self,
                    start,
                    end,
                    os.path.join(self.pdm_tmp, f"{self.filename}.{start}"),
                )
                continue
            if start >= self.file_size:
                self._logger.warning(
                    f"start {start} >= file_size {self.file_size}, break"
                )
                break
            cur.next = Chunk(
                self,
                start,
                end,
                os.path.join(self.pdm_tmp, f"{self.filename}.{start}"),
            )
            cur = cur.next
        assert root is not None
        return root

    async def rebuild_task(self):
        # 校验 .pdm 元数据完整性：确保恢复的临时文件属于当前下载任务
        pdm_file = os.path.join(self.pdm_tmp, ".pdm")
        if not os.path.exists(pdm_file):
            return None
        try:
            with open(pdm_file, "r") as f:
                info = json.load(f)
            if (
                info.get("url") != self.url
                or info.get("filename") != self.filename
                or info.get("md5") != self.md5
                or info.get("file_size") != self.file_size
            ):
                self._logger.warning(
                    ".pdm metadata mismatch, discarding stale temp files"
                )
                return None
        except (json.JSONDecodeError, IOError):
            self._logger.warning("Failed to read .pdm file, discarding temp files")
            return None
        file_list = {
            p.removeprefix(os.path.join(self.pdm_tmp, self.filename) + "."): p
            for p in glob(os.path.join(self.pdm_tmp, self.filename) + "*")
        }
        ordered_starts = sorted([int(k) for k in file_list.keys()])
        root = None
        if not ordered_starts:
            return root
        for i in range(len(ordered_starts)):
            start = ordered_starts[i]
            end = (
                ordered_starts[i + 1] - 1
                if i + 1 < len(ordered_starts)
                else self.file_size - 1
            )
            if root is None:
                root = cur = Chunk(self, start, end, file_list[str(start)])
                continue
            cur.next = Chunk(self, start, end, file_list[str(start)], cur)
            cur = cur.next
        return root

    async def create_chunk(
        self,
    ) -> Chunk | None:
        async with self.lock:
            max_gap = 0
            target_chunk: Chunk = None
            for chunk in self.chunk_root:
                if chunk.end is None:
                    return None
                gap = chunk.end - chunk.size - chunk.start + 1
                if gap > max_gap:
                    max_gap = gap
                    target_chunk = chunk
            if target_chunk is None or max_gap <= self.parent.min_split_size:
                return None
            new_start = (
                target_chunk.start
                + target_chunk.size
                + (target_chunk.next.start if target_chunk.next else target_chunk.end)
            ) // 2
            if new_start // 10240:
                new_start -= new_start % 10240
            new_chunk = Chunk(
                self,
                new_start,
                (
                    target_chunk.next.start - 1
                    if target_chunk.next
                    else target_chunk.end
                ),
                os.path.join(self.pdm_tmp, f"{self.filename}.{new_start}"),
                target_chunk,
                next=target_chunk.next,
            )
            new_chunk.end = target_chunk.end
            target_chunk.end = new_start - 1
            target_chunk.next = new_chunk
        return new_chunk

    def creat_info(self):
        if not self.parent.continue_download or not os.path.exists(
            os.path.join(self.pdm_tmp, ".pdm")
        ):
            shutil.rmtree(self.pdm_tmp, ignore_errors=True)
            os.makedirs(self.pdm_tmp, exist_ok=True)
            with open(os.path.join(self.pdm_tmp, ".pdm"), "w") as f:
                info = {
                    "url": self.url,
                    "filename": self.filename,
                    "md5": self.md5,
                    "file_size": self.file_size,
                }
                json.dump(info, f, indent=4)
        elif os.path.exists(os.path.join(self.pdm_tmp, ".pdm")):
            with open(os.path.join(self.pdm_tmp, ".pdm"), "r") as f:
                info = json.load(f)
                if (
                    info.get("md5") != self.md5
                    or info.get("file_size") != self.file_size
                    or info.get("filename") != self.filename
                    or info.get("url") != self.url
                ):
                    self._logger.warning(
                        "Existing .pdm file info does not match current download info, recreating .pdm file."
                    )
                    shutil.rmtree(self.pdm_tmp)
                    os.makedirs(self.pdm_tmp, exist_ok=True)
                    with open(os.path.join(self.pdm_tmp, ".pdm"), "w") as f:
                        info = {
                            "url": self.url,
                            "filename": self.filename,
                            "md5": self.md5,
                            "file_size": self.file_size,
                        }
                        json.dump(info, f, indent=4)
        else:
            self._logger.error("Unknown error in creating .pdm file.")

    async def merge_chunks(self):
        self.set_status(TaskStatus.MERGING)
        if os.path.exists(os.path.join(self.filepath, self.filename)):
            suffixs = self.filename.split(".")
            if len(suffixs) > 2 and suffixs[-2] == "tar":
                suffix = ".".join(suffixs[-2:])
            else:
                suffix = suffixs[-1]
            prefix = self.filename[: -len(suffix) - 1]
            redownloaded_files = set(
                glob(os.path.join(self.filepath, f"{prefix}(*).{suffix}"))
            )
            index = 0
            while True:
                index += 1
                if (
                    os.path.join(self.filepath, f"{prefix}({index}).{suffix}")
                    not in redownloaded_files
                ):
                    self.filename = f"{prefix}({index}).{suffix}"
                    break

        dest_path = os.path.join(self.filepath, self.filename)
        temp_path = dest_path + ".tmp"
        self.parent._progress.update(
            self.task,
            description=f"Merging {self.filename}",
            total=self.file_size if self.file_size > 0 else sum(self.chunk_root),
            completed=0,
        )
        last_time = time.time()
        merge_chunk = 0
        async with aiofiles.open(temp_path, "wb") as outfile:
            for chunk in self.chunk_root:
                async with aiofiles.open(chunk.chunk_path, "rb") as infile:
                    while True:
                        data = await infile.read(64 * 1024)
                        if not data:
                            break
                        await outfile.write(data)
                        if last_time + 1 < time.time():
                            self.parent._progress.update(
                                self.task, advance=merge_chunk + len(data)
                            )
                            last_time = time.time()
                            merge_chunk = 0
                        else:
                            merge_chunk += len(data)
        # self.parent._progress.stop_task(self.task)
        self.parent._progress.remove_task(self.task)

        await asyncio.to_thread(os.replace, temp_path, dest_path)
        await asyncio.to_thread(shutil.rmtree, self.pdm_tmp, True)

    def target_path_if_named(self) -> str | None:
        if not self.filename:
            return None
        return os.path.join(self.filepath, self.filename)

    async def skip_existing_named_target(self) -> bool:
        target_path = self.target_path_if_named()
        if not target_path or not os.path.exists(target_path):
            return False
        if self.parent.quit_if_exists:
            reason = "target already exists"
            self.parent._logger.info(
                f"File {self.filename} already exists, skipping because "
                "--quit-if-exists is enabled."
            )
            self._done = True
            self.record_result(
                TaskStatus.SKIPPED,
                reason=reason,
                reason_code=TaskReason.TARGET_EXISTS,
            )
            return True
        if not self.parent.auto_file_renaming:
            reason = "target already exists and auto-renaming is disabled"
            self.parent._logger.error(f"Failed {self.filename}: {reason}.")
            await self.parent.pop(self.url)
            self.record_result(
                TaskStatus.FAILED,
                reason=reason,
                reason_code=TaskReason.TARGET_EXISTS,
            )
            return True
        return False

    async def check_integrity(self):
        if self.parent.check_integrity:
            self.set_status(TaskStatus.VERIFYING)
            if self.md5 is None:
                self.parent._logger.info(
                    f"{self.filename} No md5 provided, skipping integrity check."
                )
                return True
            dest_path = os.path.join(self.filepath, self.filename)
            hash_md5 = hashlib.md5()
            async with aiofiles.open(dest_path, "rb") as f:
                while True:
                    data = await f.read(64 * 1024)
                    if not data:
                        break
                    hash_md5.update(data)
            file_md5 = hash_md5.hexdigest()
            if file_md5.lower() == self.md5.lower():
                self._logger.info(
                    f"{self.filename} MD5 checksum matches, integrity check passed."
                )
                return True
            else:
                self.parent._logger.error(
                    f"{self.filename} MD5 checksum does not match! "
                    f"Expected: {self.md5}, Got: {file_md5}"
                )
                raise IntegrityCheckFailure(self.filename, self.md5, file_md5)
        return True

    async def _download_range_task(self, task: RangeTask) -> RangeTask:
        task.path.parent.mkdir(parents=True, exist_ok=True)
        raw_existing = task.path.stat().st_size if task.path.exists() else 0
        if raw_existing > task.expected_size:
            task.path.unlink()
            raw_existing = 0
        if raw_existing == task.expected_size:
            return task
        task.downloaded_bytes = raw_existing
        file_mode = "ab" if raw_existing else "wb"
        async with (
            self._build_client_session() as session,
            aiofiles.open(task.path, file_mode) as f,
        ):
            pos = raw_existing
            window_bytes = 0
            window_start = time.time()
            requested_start = task.start + pos
            requested_end = task.end
            headers = self.build_request_headers()
            headers["Range"] = f"bytes={requested_start}-{requested_end}"
            async with session.get(
                self.url,
                headers=headers,
                timeout=self.parent.chunk_timeout,
            ) as response:
                content_range = response.headers.get("Content-Range")
                try:
                    validate_range_response(
                        status=response.status,
                        requested_start=requested_start,
                        requested_end=requested_end,
                        file_size=self.file_size,
                        content_range=content_range,
                    )
                except RangeResponseValidationError as e:
                    raise RangeResponseError(
                        task,
                        str(e),
                        status=response.status,
                        content_range=content_range,
                    ) from e
                async for data in response.content.iter_chunked(STREAM_CHUNK_SIZE):
                    remaining = task.expected_size - pos
                    if remaining <= 0:
                        break
                    data = data[:remaining]
                    await f.write(data)
                    written = len(data)
                    pos += written
                    window_bytes += written
                    task.downloaded_bytes += written
                    async with self.lock:
                        self.downloaded_bytes += written
                    now = time.time()
                    window_elapsed = max(now - window_start, 1e-6)
                    if window_elapsed >= 0.5 or window_bytes >= 524288:
                        avg_speed = window_bytes / window_elapsed
                        task.last_speed_bps = avg_speed
                        if (
                            self.parent.chunk_retry_speed
                            and avg_speed < self.parent.chunk_retry_speed
                        ):
                            raise SlowRangeError(
                                task, avg_speed, self.parent.chunk_retry_speed
                            )
                        window_bytes = 0
                        window_start = now
                    if self._per_task_limiter:
                        await self._per_task_limiter.acquire(written)
                    if self.parent._global_limiter:
                        await self.parent._global_limiter.acquire(written)
        if task.path.stat().st_size != task.expected_size:
            raise RuntimeError(
                f"range {task.start}-{task.end} incomplete: "
                f"{task.path.stat().st_size}/{task.expected_size} bytes"
            )
        return task

    async def _dynamic_worker(self, allocator: RangeAllocator) -> None:
        while True:
            task = allocator.claim_next()
            if task is None:
                return
            try:
                await self._download_range_task(task)
                allocator.mark_completed(task)
                await self._write_dynamic_metadata()
            except SlowRangeError as e:
                child = allocator.split_remaining(
                    task, min_size=self.parent.min_split_size or 1
                )
                if child is not None:
                    self._logger.debug(
                        f"Split slow dynamic range {task.start}-{task.end}; "
                        f"queued remaining range {child.start}-{child.end}: {e}"
                    )
                    await self._write_dynamic_metadata()
                    continue
                await self._retry_dynamic_range_after_failure(allocator, task, e)
            except Exception as e:
                await self._retry_dynamic_range_after_failure(allocator, task, e)

    async def _retry_dynamic_range_after_failure(
        self,
        allocator: RangeAllocator,
        task: RangeTask,
        error: Exception,
    ) -> None:
        removed = task.discard_partial()
        if removed:
            async with self.lock:
                self.downloaded_bytes = max(0, self.downloaded_bytes - removed)
        if not allocator.mark_failed(task, str(error)):
            await self._write_dynamic_metadata()
            raise error
        self._logger.debug(
            f"Requeued dynamic range {task.start}-{task.end} after failure: {error}"
        )
        await self._write_dynamic_metadata()
        await asyncio.sleep(self.parent.retry_wait)

    async def _write_dynamic_metadata(self) -> None:
        if self.range_allocator is None or self.pdm_tmp is None:
            return
        metadata_path = Path(self.pdm_tmp) / DYNAMIC_RANGE_METADATA_FILENAME
        try:
            await asyncio.to_thread(
                write_range_metadata,
                metadata_path,
                self.range_allocator,
                file_size=self.file_size,
            )
        except Exception as e:
            self._logger.warning(f"Failed to write dynamic range metadata: {e}")

    async def _start_dynamic_download(self) -> None:
        allocator = self._build_range_allocator()
        self.range_allocator = allocator
        self.downloaded_bytes = sum(task.existing_size() for task in allocator.ranges)
        await self._write_dynamic_metadata()
        workers = []

        async def progress_run():
            self.task = self.parent._progress.add_task(
                f"Downloading {self.filename}",
                total=self.file_size,
                completed=self.downloaded_bytes,
                dl=0,
            )
            while self.downloaded_bytes < self.file_size:
                self.parent._progress.update(
                    self.task,
                    completed=min(self.downloaded_bytes, self.file_size),
                    dl=len([worker for worker in workers if not worker.done()]),
                )
                await asyncio.sleep(self.parent.summary_interval)
            self.parent._progress.update(
                self.task,
                completed=min(self.downloaded_bytes, self.file_size),
                dl=0,
            )
            self.parent._logger.info(f"Completed downloading {self.filename}")

        self.progress = asyncio.create_task(progress_run())
        worker_count = min(self.parent.max_concurrent_downloads, len(allocator.ranges))
        workers = [
            asyncio.create_task(self._dynamic_worker(allocator))
            for _ in range(worker_count)
        ]
        try:
            await asyncio.gather(*workers)
            await self._write_dynamic_metadata()
            if allocator.has_failures:
                failed = allocator.failed[0]
                raise RuntimeError(
                    f"range {failed.start}-{failed.end} failed: {failed.last_error}"
                )
            await self.progress
        except Exception:
            if not self.progress.done():
                self.progress.cancel()
                with suppress(asyncio.CancelledError):
                    await self.progress
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
            raise

    async def merge_range_tasks(self):
        self.set_status(TaskStatus.MERGING)
        assert self.range_allocator is not None
        if os.path.exists(os.path.join(self.filepath, self.filename)):
            suffixs = self.filename.split(".")
            if len(suffixs) > 2 and suffixs[-2] == "tar":
                suffix = ".".join(suffixs[-2:])
            else:
                suffix = suffixs[-1]
            prefix = self.filename[: -len(suffix) - 1]
            redownloaded_files = set(
                glob(os.path.join(self.filepath, f"{prefix}(*).{suffix}"))
            )
            index = 0
            while True:
                index += 1
                if (
                    os.path.join(self.filepath, f"{prefix}({index}).{suffix}")
                    not in redownloaded_files
                ):
                    self.filename = f"{prefix}({index}).{suffix}"
                    break
        dest_path = os.path.join(self.filepath, self.filename)
        temp_path = dest_path + ".tmp"
        self.parent._progress.update(
            self.task,
            description=f"Merging {self.filename}",
            total=self.file_size,
            completed=0,
        )
        last_time = time.time()
        merge_chunk = 0
        async with aiofiles.open(temp_path, "wb") as outfile:
            for task in sorted(self.range_allocator.completed, key=lambda item: item.start):
                async with aiofiles.open(task.path, "rb") as infile:
                    while True:
                        data = await infile.read(64 * 1024)
                        if not data:
                            break
                        await outfile.write(data)
                        if last_time + 1 < time.time():
                            self.parent._progress.update(
                                self.task, advance=merge_chunk + len(data)
                            )
                            last_time = time.time()
                            merge_chunk = 0
                        else:
                            merge_chunk += len(data)
        self.parent._progress.remove_task(self.task)
        await asyncio.to_thread(os.replace, temp_path, dest_path)
        await asyncio.to_thread(shutil.rmtree, self.pdm_tmp, True)

    async def start_download(self, _iter=None):
        if _iter is None:
            _iter = self.parent.retry
        parsed = False
        while _iter >= 0:
            try:
                if await self.skip_existing_named_target():
                    return self.result
                if not parsed:
                    await self.parse_config()
                    parsed = True
                if await self.skip_existing_named_target():
                    return self.result
                self.task = None
                self.set_status(TaskStatus.DOWNLOADING)
                if self._can_use_dynamic_segments():
                    await self._start_dynamic_download()
                    await self.merge_range_tasks()
                else:
                    await self._start_download()
                    await self.merge_chunks()
                await self.check_integrity()
                self._done = True
                self.record_result(
                    TaskStatus.COMPLETED,
                    reason="download completed",
                )
                break
            except HeaderStatusSkip as e:
                if e.can_retry and _iter > 0:
                    self.set_status(
                        TaskStatus.RETRYING,
                        reason=e.summary(),
                        reason_code=TaskReason.HTTP_STATUS,
                        error=str(e),
                    )
                    self._logger.warning(
                        f"{e}. Retrying in {self.parent.retry_wait}s "
                        f"({_iter} retries left)."
                    )
                    _iter -= 1
                    await asyncio.sleep(self.parent.retry_wait)
                    continue
                reason = e.summary()
                self._logger.error(f"Failed {self.filename or self.url}: {reason}.")
                if self.pdm_tmp and not self.parent.keep_tmp:
                    await asyncio.to_thread(shutil.rmtree, self.pdm_tmp, True)
                self._done = False
                return self.record_result(
                    TaskStatus.FAILED,
                    reason=reason,
                    reason_code=TaskReason.HTTP_STATUS,
                    error=str(e),
                )
            except ConnectionTimeoutSkip as e:
                reason = str(e)
                self._logger.error(f"Failed {self.filename or self.url}: {reason}.")
                if self.pdm_tmp and not self.parent.keep_tmp:
                    await asyncio.to_thread(shutil.rmtree, self.pdm_tmp, True)
                self._done = False
                return self.record_result(
                    TaskStatus.FAILED,
                    reason=reason,
                    reason_code=e.reason_code,
                    error=str(e),
                )
            except IntegrityCheckFailure as e:
                reason = "MD5 mismatch"
                self._logger.error(f"Failed {self.filename or self.url}: {e}.")
                self._done = False
                return self.record_result(
                    TaskStatus.FAILED,
                    reason=reason,
                    reason_code=TaskReason.INTEGRITY_MISMATCH,
                    error=str(e),
                )
            except TmpSpaceInsufficient as e:
                if e.policy == "mkdir":
                    reason = "temporary directory could not be created"
                    reason_code = TaskReason.TMP_DIR_CREATE_FAILED
                else:
                    reason = "temporary directory has insufficient free space"
                    reason_code = TaskReason.TMP_SPACE_INSUFFICIENT
                self._logger.error(f"Failed {self.filename or self.url}: {reason}.")
                self._done = False
                return self.record_result(
                    TaskStatus.FAILED,
                    reason=reason,
                    reason_code=reason_code,
                    error=str(e),
                )
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                if _iter > 0:
                    reason_code = (
                        TaskReason.MERGE_FAILED
                        if self.status == TaskStatus.MERGING
                        else TaskReason.UNEXPECTED_ERROR
                    )
                    self.set_status(
                        TaskStatus.RETRYING,
                        reason=str(e),
                        reason_code=reason_code,
                        error=str(e),
                    )
                    _iter -= 1
                    await asyncio.sleep(self.parent.retry_wait)
                else:
                    reason_code = (
                        TaskReason.MERGE_FAILED
                        if self.status == TaskStatus.MERGING
                        else TaskReason.UNEXPECTED_ERROR
                    )
                    reason = (
                        "merge failed"
                        if reason_code == TaskReason.MERGE_FAILED
                        else "failed after retries"
                    )
                    self._logger.error(
                        f"Failed {self.filename or self.url}: {reason}. {e}"
                    )
                    self._done = False
                    return self.record_result(
                        TaskStatus.FAILED,
                        reason=reason,
                        reason_code=reason_code,
                        error=str(e),
                    )
        if self._done:
            # 下载完成回调
            if self.parent.on_download_complete:
                self._run_download_complete_callback()
            self.parent._logger.success(
                f"Finished download {self.filename} from {self.url}"
            )
            return self.result
        return self.result or self.record_result(
            TaskStatus.FAILED,
            reason="download did not complete",
            reason_code=TaskReason.UNEXPECTED_ERROR,
        )

    def _run_download_complete_callback(self):
        """异步执行下载完成回调命令（不阻塞后续任务）"""
        dest = os.path.join(self.filepath, self.filename)
        cmd = self.parent.on_download_complete
        cmd = (cmd.replace("{filename}", self.filename)
                   .replace("{filepath}", dest)
                   .replace("{url}", self.url)
                   .replace("{dir}", self.filepath)
                   .replace("{size}", str(self.file_size)))

        async def _runner():
            try:
                proc = await asyncio.create_subprocess_shell(
                    cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode != 0:
                    self._logger.warning(
                        f"Download complete callback exited with code "
                        f"{proc.returncode}: {stderr.decode()}"
                    )
                else:
                    self._logger.info(
                        f"Download complete callback finished: {stdout.decode().strip()}"
                    )
            except Exception as e:
                self._logger.error(f"Download complete callback failed: {e}")

        asyncio.create_task(_runner())

    async def _start_download(self):
        tasks = []

        async def progress_run():
            if self.file_size < 0:
                self.task = self.parent._progress.add_task(
                    f"Downloading {self.filename}", total=None, dl=len(tasks)
                )
                while not self._downloaded:
                    self.parent._progress.update(
                        self.task, completed=self.downloaded_bytes, dl=len(tasks)
                    )
                    await asyncio.sleep(self.parent.summary_interval)
            else:
                self.task = self.parent._progress.add_task(
                    f"Downloading {self.filename}",
                    total=self.file_size,
                    completed=self.downloaded_bytes,
                    dl=len(tasks),
                )
                while self.file_size > 0:
                    completed = min(self.downloaded_bytes, self.file_size)
                    if self.file_size <= completed:
                        break
                    self.parent._progress.update(
                        self.task, completed=completed, dl=len(tasks)
                    )
                    await asyncio.sleep(self.parent.summary_interval)
                self.parent._progress.update(
                    self.task,
                    completed=min(self.downloaded_bytes, self.file_size),
                    dl=len(tasks),
                )
                self.parent._logger.info(f"Completed downloading {self.filename}")
            # self.parent._progress.stop_task(self.task)
            # self.parent._progress.remove_task(self.task)

        self.progress = asyncio.create_task(progress_run())

        # 在锁保护下收集所有初始 chunk 引用，避免并发修改链表导致迭代器异常
        async with self.lock:
            chunks_to_start = [chunk for chunk in self.chunk_root]
        for chunk in chunks_to_start:
            if tasks.__len__() < self.parent.max_concurrent_downloads:
                self.parent._logger.debug(
                    f"tasks number {tasks.__len__()} < max_concurrent_downloads {self.parent.max_concurrent_downloads}, creating new task."
                )
                tasks.append(asyncio.create_task(chunk.download()))
            else:
                self.parent._logger.debug(
                    f"tasks number {tasks.__len__()} >= max_concurrent_downloads {self.parent.max_concurrent_downloads}, wait for a task to complete before creating new task."
                )
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for d in done:
                    tasks.remove(d)
                tasks.append(asyncio.create_task(chunk.download()))
        while True:
            if tasks.__len__() < self.parent.max_concurrent_downloads:
                self.parent._logger.debug(
                    f"tasks number {tasks.__len__()} < max_concurrent_downloads {self.parent.max_concurrent_downloads}, creating new task."
                )
                new_chunk = await self.create_chunk()
                if new_chunk is None:
                    break
                tasks.append(asyncio.create_task(new_chunk.download()))
                continue
            self.parent._logger.debug(
                f"tasks number {tasks.__len__()} >= max_concurrent_downloads {self.parent.max_concurrent_downloads}, wait for a task to complete before creating new task."
            )
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for d in done:
                tasks.remove(d)
            new_chunk = await self.create_chunk()
            if new_chunk is None:
                break
            tasks.append(asyncio.create_task(new_chunk.download()))
        await asyncio.gather(*tasks, self.progress)
