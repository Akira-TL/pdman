#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
核心库模块：导出 Manager 类供外部 import 使用。
保留原有行为与日志/并发策略；CLI 部分在 cli.py。
"""

import random
import re
import os
import sys
import time
import json
import shutil
import asyncio
import hashlib
import traceback
import aiohttp
import aiofiles
import yaml
from yarl import URL
from glob import glob
from rich.text import Text
from urllib.parse import unquote
from typing import List, Optional, TextIO
from loguru._logger import Logger, Core

from rich.progress import (
    Progress,
    Console,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TextColumn,
)
from .chunk import Chunk
from .downloader import Downloader
from .runtime import RuntimePaths, task_result_to_record, utc_now_iso
from .status import TaskResult, TaskStatus
from .task_input import load_task_input
from .utils import auto_sync


class RateLimiter:
    """令牌桶限速器，用于单任务或全局下载限速"""

    def __init__(self, max_rate: int | None):
        """
        args:
            max_rate: 最大速率（字节/秒），None 表示不限速
        """
        self.max_rate = max_rate
        self._tokens = float(max_rate) if max_rate else float("inf")
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, byte_count: int) -> None:
        """获取 byte_count 字节的下载许可，超出速率时自动等待"""
        if self.max_rate is None:
            return
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                float(self.max_rate),
                self._tokens + self.max_rate * elapsed,
            )
            self._last_refill = now
            if byte_count > self._tokens:
                wait_time = (byte_count - self._tokens) / self.max_rate
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
                self._last_refill = time.monotonic()
            else:
                self._tokens -= byte_count


class Manager:
    """
    ### 负责管理下载任务，提供添加 URL、启动下载、停止下载等功能。

    args:
        max_downloads: 最大下载**任务**数
        timeout: 下载超时时间，单位秒
        retry: 下载失败重试次数
        retry_wait: 下载失败重试等待时间，单位秒
        log_path: 日志输出路径或对象
        debug: 是否启用调试模式
        check_integrity: 是否启用完整性校验
        continue_download: 是否启用断点续传
        max_concurrent_downloads: 最大并发下载数（单任务内部分块数）
        min_split_size: 最小分块大小，单位支持 K/M/G
        force_sequential: 是否强制顺序下载
        tmp_dir: 临时文件目录
        user_agent: HTTP 请求的 User-Agent 字段
        chunk_retry_speed: 分块下载重试速度阈值，单位支持 K/M/G
        chunk_timeout: 分块下载超时时间，单位秒
        auto_file_renaming: 是否启用自动文件重命名以避免冲突
        out_dir: 下载文件输出目录，默认为当前工作目录
        # 认证与 Cookie
        http_auth: HTTP 认证，格式 "user:pass"
        cookie_file: Netscape/Mozilla 格式 Cookie 文件路径
        # 限速
        max_download_limit: 单任务下载限速（字节/秒），支持 K/M/G
        max_overall_download_limit: 全局下载限速（字节/秒），支持 K/M/G
        # 代理
        proxy: HTTP/HTTPS 代理 URL
        proxy_auth: 代理认证，格式 "user:pass"
        # 请求头与超时
        headers: 自定义 HTTP 头列表，每个元素为 "Key: Value"
        connect_timeout: 连接超时（秒），独立于读写超时
        connect_progress_delay: 连接等待提示延迟（秒），超过该时间仍未连通则显示等待进度
        max_connection_per_server: 单服务器最大连接数，0 表示不限制
        referer: HTTP Referer 头
        # 回调
        on_download_complete: 下载完成回调 shell 命令，支持占位符
        # SSL
        check_certificate: 是否验证 SSL 证书
        ca_certificate: 自定义 CA 证书文件路径
        # 其他
        conf_path: 配置文件路径（JSON/YAML）
        quit_if_exists: 目标文件已存在则跳过下载
        summary_interval: 进度刷新间隔（秒）
        segment_mode: 分段下载模式，static 或 dynamic

    attributes:
        config: 更新配置项的方法
        add_urls: 添加下载 URL 的方法，支持列表或字典格式
        load_input_file: 从文件加载下载 URL 的方法，支持 JSON/YAML/纯文本格式
        append: 添加单个下载 URL 的方法，支持指定文件名、目录和日志路径
        pop: 移除下载 URL 的方法
        wait: 等待下载任务完成的方法
        download: 启动下载的方法
        start_download: 启动持续下载的方法
        stop_download: 停止持续下载的方法
        urls: 获取当前下载 URL 列表的方法
    """

    def __init__(
        self,
        max_downloads: int = 4,
        timeout: int = 60,
        retry: int = 3,
        retry_wait: int = 5,
        log_path: str | TextIO = sys.stdout,
        debug: bool = False,
        check_integrity: bool = False,
        continue_download: bool = False,
        max_concurrent_downloads: int = 5,
        min_split_size: str = "1M",
        force_sequential: bool = False,
        tmp_dir: str = None,
        tmp_policy: str = "auto",
        cache_dir: str = None,
        keep_tmp: bool = False,
        user_agent: dict | str = None,
        chunk_retry_speed: str | int = None,
        chunk_timeout: int = 10,
        auto_file_renaming: bool = True,
        out_dir: str = None,
        # === 认证与 Cookie ===
        http_auth: str = None,
        cookie_file: str = None,
        # === 限速 ===
        max_download_limit: str | int = None,
        max_overall_download_limit: str | int = None,
        # === 代理 ===
        proxy: str = None,
        proxy_auth: str = None,
        # === 请求头与超时 ===
        headers: list[str] = None,
        connect_timeout: int = 30,
        connect_progress_delay: float = 5.0,
        max_connection_per_server: int = 0,
        referer: str = None,
        # === 回调 ===
        on_download_complete: str = None,
        # === SSL ===
        check_certificate: bool = True,
        ca_certificate: str = None,
        # === 其他 ===
        conf_path: str = None,
        quit_if_exists: bool = False,
        summary_interval: float = 1.0,
        segment_mode: str = "static",
    ):
        self.max_downloads = max_downloads
        self.timeout = timeout
        self.chunk_timeout = chunk_timeout
        self.retry = retry
        self.log_path = log_path
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
        self.debug = debug
        self.continue_download = continue_download
        self.max_concurrent_downloads = max_concurrent_downloads
        self.min_split_size = min_split_size
        self.force_sequential = force_sequential
        self.tmp_dir = tmp_dir
        self.tmp_policy = tmp_policy
        self.cache_dir = cache_dir
        self.keep_tmp = keep_tmp
        self.runtime_paths = RuntimePaths.create(cache_dir=cache_dir)
        self.run_id = self.runtime_paths.run_id
        self.user_agent = user_agent
        self.check_integrity = check_integrity
        self.chunk_retry_speed = chunk_retry_speed
        self.retry_wait = retry_wait
        self.auto_file_renaming = auto_file_renaming
        self.out_dir = out_dir
        # === 认证与 Cookie ===
        self.http_auth = http_auth
        self.cookie_file = cookie_file
        # === 限速 ===
        self.max_download_limit = max_download_limit
        self.max_overall_download_limit = max_overall_download_limit
        self._global_limiter: RateLimiter | None = None
        # === 代理 ===
        self.proxy = proxy
        self.proxy_auth = proxy_auth
        # === 请求头与超时 ===
        self.headers = headers
        self.connect_timeout = connect_timeout
        self.connect_progress_delay = connect_progress_delay
        self.max_connection_per_server = max_connection_per_server
        self.referer = referer
        # === 回调 ===
        self.on_download_complete = on_download_complete
        # === SSL ===
        self.check_certificate = check_certificate
        self.ca_certificate = ca_certificate
        # === 其他 ===
        self.conf_path = conf_path
        self.quit_if_exists = quit_if_exists
        self.summary_interval = summary_interval
        self.segment_mode = segment_mode

        self._urls_lock = asyncio.Lock()
        self._urls: dict = {}  # {url: Downloader item, ...}
        """{url: `Downloader` item, ...}"""
        self._console = Console()
        self._progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[bold blue]DL:{task.fields[dl]}"),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self._console,
            refresh_per_second=1.0 / max(self.summary_interval, 0.1),
        )
        self._downloader_main = None
        self._downloaders = []
        self.results: list[TaskResult] = []
        self.exit_code = 0
        self.run_started_at: str | None = None
        self.run_finished_at: str | None = None
        self._runtime_active = False
        self.tmp_cleanup = {
            "policy": "cleanup_on_finish",
            "kept": False,
            "run_dir": str(self.runtime_paths.run_dir),
            "error": None,
        }
        # 先设置 summary_interval 再调用 _parse_config（这样 _parse_config 可以用）
        self._parse_config()

    def config(self, **kwargs):
        need_reparse_logging = False
        need_reparse_download = False
        # 需要重新解析下载参数的配置项
        reparse_keys = {
            "max_downloads", "max_concurrent_downloads", "min_split_size",
            "chunk_retry_speed", "force_sequential", "max_download_limit",
            "max_overall_download_limit", "http_auth", "proxy_auth", "headers",
            "connect_timeout", "connect_progress_delay",
            "max_connection_per_server", "summary_interval", "segment_mode",
            "tmp_policy", "tmp_dir", "cache_dir", "keep_tmp",
        }
        for k, v in kwargs.items():
            if hasattr(self, k) and not k.startswith("_"):
                setattr(self, k, v)
                if k in ("debug", "log_path"):
                    need_reparse_logging = True
                if k in reparse_keys:
                    need_reparse_download = True
        if need_reparse_logging:
            self._reparse_logging()
        if need_reparse_download:
            self._reparse_download_params()

    def _parse_config(self) -> None:
        """
        ### 解析配置项，处理日志设置、并发限制、大小单位转换等逻辑。
        （仅在 __init__ 中完整调用；更新配置时用 _reparse_logging 和 _reparse_download_params）
        """
        self._reparse_logging()
        self._reparse_download_params()
        # >>> 解析 User-Agent 配置
        if isinstance(self.user_agent, str):
            try:
                self.user_agent = json.loads(self.user_agent)
            except Exception:
                self.user_agent = {"User-Agent": self.user_agent}
        # <<< 解析 User-Agent 配置

    def _reparse_logging(self) -> None:
        """重新配置日志处理器（仅在日志相关配置变更时调用）"""
        self._logger.remove()
        self._logger.add(
            lambda msg: self._console.print(Text.from_ansi(str(msg)), end="\n"),
            level="DEBUG" if self.debug else "INFO",
            diagnose=True,
            colorize=True,
            format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
        )
        if isinstance(self.log_path, str):
            self._logger.add(
                self.log_path,
                level="DEBUG" if self.debug else "INFO",
                diagnose=True,
                colorize=True,
                format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
            )

    def _reparse_download_params(self) -> None:
        """重新解析下载参数（大小转换、并发限制、认证信息等）"""
        self.max_downloads = int(self.max_downloads)
        if self.max_downloads < 1:
            self.max_downloads = 1
            self._logger.warning("threads cannot be less than 1. Setting to 1.")
        elif self.max_downloads > 32:
            self._logger.warning(
                "threads are more than 32, may cause high resource usage. "
            )
        if self.max_concurrent_downloads < 1:
            self.max_concurrent_downloads = 1
            self._logger.warning(
                "max_concurrent_downloads cannot be less than 1. Setting to 1."
            )
        elif self.max_concurrent_downloads > 32:
            self._logger.warning(
                "max_concurrent_downloads is more than 32, becareful of server limits. "
            )
        self.min_split_size = self._parse_size(self.min_split_size)
        self.chunk_retry_speed = self._parse_size(self.chunk_retry_speed)
        if self.force_sequential:
            self.max_concurrent_downloads = 1
            self._logger.info("Force sequential download enabled.")
        # 限速解析
        self.max_download_limit = self._parse_size(self.max_download_limit)
        self.max_overall_download_limit = self._parse_size(self.max_overall_download_limit)
        self._global_limiter = (
            RateLimiter(self.max_overall_download_limit)
            if self.max_overall_download_limit
            else None
        )
        # HTTP 认证解析（"user:pass" → aiohttp.BasicAuth）
        if self.http_auth and isinstance(self.http_auth, str):
            if ":" in self.http_auth:
                user, pwd = self.http_auth.split(":", 1)
                self.http_auth = aiohttp.BasicAuth(login=user, password=pwd)
            else:
                self.http_auth = aiohttp.BasicAuth(login=self.http_auth, password="")
        # 代理认证解析
        if self.proxy_auth and isinstance(self.proxy_auth, str):
            if ":" in self.proxy_auth:
                user, pwd = self.proxy_auth.split(":", 1)
                self.proxy_auth = aiohttp.BasicAuth(login=user, password=pwd)
            else:
                self.proxy_auth = aiohttp.BasicAuth(login=self.proxy_auth, password="")
        # 自定义 HTTP 头解析（"Key: Value" → dict）
        self.headers_dict: dict[str, str] = {}
        if self.headers:
            if isinstance(self.headers, list):
                for h in self.headers:
                    if ":" in h:
                        k, v = h.split(":", 1)
                        self.headers_dict[k.strip()] = v.strip()
                    else:
                        self._logger.warning(
                            f"Ignoring malformed header (missing colon): {h}"
                        )
            elif isinstance(self.headers, dict):
                self.headers_dict = dict(self.headers)
        if self.connect_timeout is None or self.connect_timeout <= 0:
            self.connect_timeout = 30
        self.connect_progress_delay = float(self.connect_progress_delay)
        if self.connect_progress_delay < 0:
            self.connect_progress_delay = 0
        if self.connect_progress_delay >= self.connect_timeout:
            self.connect_progress_delay = max(self.connect_timeout - 0.1, 0)
        # 连接数上限校验
        if self.max_connection_per_server < 0:
            self.max_connection_per_server = 0
            self._logger.warning(
                "max_connection_per_server cannot be negative. Setting to 0 (unlimited)."
            )
        # 进度刷新间隔
        if self.summary_interval < 0.1:
            self.summary_interval = 0.1
        # runtime 目录策略
        self.tmp_policy = (self.tmp_policy or "auto").lower()
        if self.tmp_policy not in {"auto", "system", "target"}:
            raise ValueError(f"Invalid tmp_policy: {self.tmp_policy}")
        self.segment_mode = (self.segment_mode or "static").lower()
        if self.segment_mode not in {"static", "dynamic", "auto"}:
            raise ValueError(f"Invalid segment_mode: {self.segment_mode}")
        # 加载配置文件（如果指定了 conf_path 且尚未加载）
        if self.conf_path is not None:
            self._load_config_file()

    def _load_config_file(self) -> None:
        """从配置文件加载参数（JSON/YAML），CLI 参数优先级高于配置文件"""
        if not self.conf_path or not os.path.exists(self.conf_path):
            return
        try:
            with open(self.conf_path, "r") as f:
                suffix = os.path.splitext(self.conf_path)[1].lower()
                if suffix in (".yaml", ".yml"):
                    config_data = yaml.safe_load(f) or {}
                elif suffix == ".json":
                    config_data = json.load(f)
                else:
                    self._logger.warning(
                        f"Unsupported config file format: {suffix}, skipping."
                    )
                    return
            if not isinstance(config_data, dict):
                self._logger.warning("Config file content is not a dict, skipping.")
                return
            # 配置文件值作为默认值，不覆盖 CLI 已设置的非 None 值
            for key, value in config_data.items():
                if hasattr(self, key) and not key.startswith("_"):
                    current = getattr(self, key)
                    # 只有当前值为默认的假值时才覆盖
                    if current in (None, False, 0, "", [], {}):
                        setattr(self, key, value)
        except Exception as e:
            self._logger.warning(f"Failed to load config file: {e}")

    def _parse_size(self, size_str: str) -> int:
        """
        ### 解析大小字符串，支持 K/M/G 单位，并转换为字节数。

        args:
            size_str: 大小字符串，例如 "1M", "500K", "2G"
        returns:
            int: 转换后的字节数，整数类型
        raises:
            ValueError: 如果输入格式不正确，抛出异常
        """
        if size_str is None or size_str == "":
            return None
        size_str = str(size_str).strip().upper()
        size_map = {"K": 1024, "M": 1024**2, "G": 1024**3}
        size = 1
        for i in range(len(size_str) - 1, -1, -1):
            unit = size_str[i]
            if unit in size_map:
                size *= size_map[unit]
                continue
            break
        num = size_str[: i + 1]
        if re.match(r"^\d+(\.\d+)?$", num):
            return int(float(num) * size)
        else:
            raise ValueError(f"Invalid size format: {size_str}")

    def add_urls(self, url_list: dict | list[str]) -> None:
        """
        添加多个 URL 到下载队列。

        args:
            url_list: 可以是字典或字符串列表。如果是字典，键为 URL，值为包含 md5、file_name、dir_path 和 log_path 的字典。
        returns:
            None
        """
        if type(url_list) == dict:
            for url, v in url_list.items():
                assert type(v) == dict
                self.append(
                    url,
                    md5=v.get("md5"),
                    file_name=v.get("file_name"),
                    dir_path=v.get(
                        "dir_path", self.out_dir if self.out_dir else os.getcwd()
                    ),
                    log_path=v.get("log_path", None),
                )
        else:
            for url in url_list:
                self.append(url, dir_path=self.out_dir if self.out_dir else os.getcwd())

    def load_input_file(self, input_file: str) -> None:
        """
        加载输入文件并将其中的 URL 添加到下载队列。

        args:
            input_file: 输入文件路径，支持 JSON、YAML 或纯文本格式。
        returns:
            None
        """
        try:
            for task in load_task_input(input_file):
                self.append(
                    task.url,
                    md5=task.md5,
                    file_name=task.file_name,
                    dir_path=task.dir_path or self.out_dir or os.getcwd(),
                    log_path=task.log_path,
                )
        except (json.JSONDecodeError, yaml.YAMLError, ValueError) as e:
            self._logger.error(f"Failed to parse input file: {e}")

    def resolve_task_tmp_decision(
        self,
        *,
        task_id: str,
        target_dir: str,
        file_size: int | None,
    ):
        legacy_tmp = self.runtime_paths.target_tmp_dir(target_dir, task_id)
        if self.continue_download and self.tmp_dir is None:
            if (legacy_tmp / ".pdm").exists():
                decision = self.runtime_paths.resolve_task_tmp_decision(
                    task_id=task_id,
                    target_dir=target_dir,
                    tmp_dir=None,
                    tmp_policy="target",
                    file_size=file_size,
                )
                decision.reason = "using legacy target tmp directory for --continue"
                return decision
        return self.runtime_paths.resolve_task_tmp_decision(
            task_id=task_id,
            target_dir=target_dir,
            tmp_dir=self.tmp_dir,
            tmp_policy=self.tmp_policy,
            file_size=file_size,
        )

    def resolve_task_tmp_dir(
        self,
        *,
        task_id: str,
        target_dir: str,
        file_size: int | None,
    ) -> str:
        return str(
            self.resolve_task_tmp_decision(
                task_id=task_id,
                target_dir=target_dir,
                file_size=file_size,
            ).selected_dir
        )

    @auto_sync
    async def append(
        self,
        url: str,
        md5: str = None,
        file_name: str = None,
        dir_path: str = os.getcwd(),
        log_path: str = None,
    ) -> None:
        """
        添加单个 URL 到下载队列。
        args:
            url: 下载链接
            md5: 可选的 MD5 校验值
            file_name: 可选的文件名，默认为 URL 中的文件名
            dir_path: 可选的下载目录，默认为当前工作目录
            log_path: 可选的日志路径，默认为 None
        returns:
            None
        """
        async with self._urls_lock:
            self._urls[url] = Downloader(
                self, url, dir_path, filename=file_name, md5=md5, log_path=log_path
            )
            self._logger.debug(f"Added URL: {url}")

    @auto_sync
    async def pop(self, url: str) -> dict | None:
        async with self._urls_lock:
            result = self._urls.pop(url, None)
        self._logger.debug(f"Popped URL: {url}")
        return result

    @auto_sync
    async def popitem(self) -> tuple[str, dict] | None:
        async with self._urls_lock:
            if not self._urls:
                return None
            url, download_entity = self._urls.popitem()
        self._logger.debug(f"Popped URL item: {url}")
        return url, download_entity

    @staticmethod
    def _format_bytes(size: int) -> str:
        units = ["B", "KiB", "MiB", "GiB", "TiB"]
        value = float(size)
        for unit in units:
            if value < 1024 or unit == units[-1]:
                if unit == "B":
                    return f"{int(value)} {unit}"
                return f"{value:.1f} {unit}"
            value /= 1024
        return f"{size} B"

    def _task_counts(self) -> dict[str, int]:
        return {
            "completed": len(
                [r for r in self.results if r.status == TaskStatus.COMPLETED]
            ),
            "skipped": len(
                [r for r in self.results if r.status == TaskStatus.SKIPPED]
            ),
            "failed": len(
                [r for r in self.results if r.status == TaskStatus.FAILED]
            ),
        }

    def _run_payload(self, status: str) -> dict:
        return {
            "run_id": self.run_id,
            "pid": os.getpid(),
            "status": status,
            "started_at": self.run_started_at,
            "finished_at": self.run_finished_at,
            "tmp_policy": self.tmp_policy,
            "tmp_root": str(self.runtime_paths.run_dir),
            "cache_root": str(self.runtime_paths.cache_root),
            "task_counts": self._task_counts(),
            "exit_code": self.exit_code,
            "tmp_cleanup": self.tmp_cleanup,
        }

    def _write_active_run(self) -> None:
        if self._runtime_active:
            self.runtime_paths.write_active_run(self._run_payload("running"))

    def _start_runtime_run(self) -> None:
        self.runtime_paths.ensure()
        self.run_started_at = utc_now_iso()
        self.run_finished_at = None
        self._runtime_active = True
        self._write_active_run()

    def _finish_runtime_run(self) -> None:
        self.run_finished_at = utc_now_iso()
        self.tmp_cleanup = {
            "policy": "keep_failed" if self.keep_tmp and self.exit_code != 0 else "cleanup_on_finish",
            "kept": False,
            "run_dir": str(self.runtime_paths.run_dir),
            "error": None,
        }
        if self.keep_tmp and self.exit_code != 0:
            self.tmp_cleanup["kept"] = True
        else:
            try:
                self.runtime_paths.cleanup_run_dir()
            except FileNotFoundError:
                pass
            except Exception as e:
                self.tmp_cleanup["kept"] = True
                self.tmp_cleanup["error"] = str(e)
        self.runtime_paths.write_final_run(self._run_payload("finished"))
        self.runtime_paths.clear_active_run()
        self._runtime_active = False

    @staticmethod
    def _task_id_for_url(url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()[:6]

    def record_task_result(self, result) -> None:
        if isinstance(result, TaskResult):
            self.results.append(result)
            if result.status == TaskStatus.FAILED:
                self.exit_code = 1
            if self._runtime_active:
                finished_at = utc_now_iso()
                task_id = self._task_id_for_url(result.url)
                self.runtime_paths.append_history(
                    task_result_to_record(
                        run_id=self.run_id,
                        task_id=task_id,
                        result=result,
                        started_at=self.run_started_at,
                        finished_at=finished_at,
                    )
                )
                self._write_active_run()

    def summarize_results(self) -> str:
        completed = [
            r for r in self.results if r.status == TaskStatus.COMPLETED
        ]
        skipped = [r for r in self.results if r.status == TaskStatus.SKIPPED]
        failed = [r for r in self.results if r.status == TaskStatus.FAILED]
        downloaded = sum(r.downloaded_bytes for r in completed)
        lines = [
            "Summary:",
            f"  completed: {len(completed)}",
            f"  skipped: {len(skipped)}",
            f"  failed: {len(failed)}",
            f"  downloaded: {self._format_bytes(downloaded)}",
        ]
        resume_rejected = [r for r in self.results if r.resume_rejection_reason]
        if resume_rejected:
            lines.append("Resume:")
            for result in resume_rejected:
                name = result.filename or result.url
                lines.append(f"  {name} - {result.resume_rejection_reason}")
        if skipped:
            lines.append("Skipped:")
            for result in skipped:
                name = result.filename or result.url
                reason = result.reason or result.error or "unknown reason"
                lines.append(f"  {name} - {reason}")
        if failed:
            lines.append("Failed:")
            for result in failed:
                name = result.filename or result.url
                reason = result.reason or result.error or "unknown reason"
                lines.append(f"  {name} - {reason}")
        return "\n".join(lines)

    def print_summary(self) -> None:
        if self.results:
            self._console.print(self.summarize_results())

    async def wait(self) -> None:
        while self._downloaders:
            await self.wait_one()

    async def wait_one(self) -> None:
        try:
            done, pending = await asyncio.wait(
                self._downloaders, return_when=asyncio.FIRST_COMPLETED
            )
        except Exception as e:
            self._logger.error(f"wait error: {e}")
            self._logger.error(traceback.format_exc())
            return
        for d in done:
            try:
                result = d.result()
                self.record_task_result(result)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.exit_code = 1
                self._logger.error(f"task error: {e}")
                self._logger.error(traceback.format_exc())
        self._downloaders = list(pending)

    async def download(self) -> None:
        """
        开始下载任务。
        """
        self._logger.debug(self)
        self._start_runtime_run()
        self._downloader_main = asyncio.create_task(self._download_once())
        try:
            await self._downloader_main
        finally:
            self._downloader_main = None
            self._finish_runtime_run()
        self.print_summary()

    async def _download_once(self, downloading=None) -> None:
        if downloading is None:
            downloading = {}
        with self._progress:
            while self._urls:
                url, download_entity = await self.popitem()
                if url in downloading:
                    continue
                downloading[url] = True
                assert isinstance(download_entity, Downloader)
                while len(self._downloaders) >= self.max_downloads:
                    await self.wait_one()
                self._downloaders.append(
                    asyncio.create_task(download_entity.start_download())
                )
                self._logger.debug(f"Starting download for {url}")
            await self.wait()
            await asyncio.sleep(0.1)

    async def _loop(self) -> None:
        self._logger.debug(self)
        downloading = {}  # 存储正在下载的 URL，避免本轮重复下载
        while True:
            await self._download_once(downloading)
            # 清理已完成的 URL，防止持续模式下内存泄漏
            downloaded_urls = [url for url in downloading.copy()
                               if url not in self._urls]
            for url in downloaded_urls:
                downloading.pop(url, None)
            await asyncio.sleep(1)

    async def start_loop(self) -> None:  # 持续下载
        """
        开始下载循环
        """
        if self._downloader_main is not None:
            self._logger.warning("Download loop is already running.")
            return
        else:
            self._downloader_main = asyncio.create_task(self._loop())

    def stop_loop(self) -> None:  # 停止持续下载
        """
        停止下载循环。
        """
        if self._downloader_main is not None:
            self._downloader_main.cancel()
            self._downloader_main = None

    def urls(self) -> List[str]:
        """
        获取当前下载 URL 列表。
        returns:
            List[str]: 当前下载 URL 列表
        """
        return list(self._urls.keys())

    def __str__(self):
        return (
            f"Manager(max_downloads={self.max_downloads}, timeout={self.timeout}, "
            f"retry={self.retry}, debug={self.debug}, "
            f"continue_download={self.continue_download}, "
            f"max_concurrent_downloads={self.max_concurrent_downloads}, "
            f"min_split_size={self.min_split_size}, "
            f"segment_mode={self.segment_mode}, "
            f"proxy={self.proxy})"
        )

    def __del__(self):
        self.stop_loop()

    # 支持with语法
    async def __aenter__(self):
        await self.start_loop()
        return self

    def __enter__(self):
        asyncio.run(self.__aenter__())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.wait()
        self.stop_loop()

    def __exit__(self, exc_type, exc, tb):
        asyncio.run(self.__aexit__(exc_type, exc, tb))
