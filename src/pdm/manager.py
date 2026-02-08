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
import traceback
import yaml
import json
import time
import shutil
import asyncio
import hashlib
import aiohttp
import aiofiles
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


class Manager:
    """
    下载管理器，负责全局配置、任务队列调度与进度显示。
    提供解析输入、创建下载器、并发控制与重试等能力。
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
        user_agent: dict | str = None,
        chunk_retry_speed: str | int = None,
        chunk_timeout: int = 10,
        auto_file_renaming: bool = True,
        out_dir: str = None,
    ):
        """
        初始化下载管理器。
        参数：同原脚本 pdm.py。
        """
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
        self.user_agent = user_agent
        self.check_integrity = check_integrity
        self.chunk_retry_speed = chunk_retry_speed
        self.retry_wait = retry_wait
        self.auto_file_renaming = auto_file_renaming
        self.out_dir = out_dir

        self._urls_lock = asyncio.Lock()
        self._urls: dict = {}
        self._console = Console()
        self._progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self._console,
        )
        self._downloader_main = None
        self._downloaders = []
        self.parse_config()

    def config(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k) and k not in [
                "_urls",
                "_progress",
                "_logger",
                "_urls_lock",
            ]:
                setattr(self, k, v)
        self.parse_config()

    def parse_config(self):
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
        if self.max_concurrent_downloads < 1:
            self.max_concurrent_downloads = 1
            self._logger.warning(
                "max_concurrent_downloads cannot be less than 1. Setting to 1."
            )
        elif self.max_concurrent_downloads > 32:
            self._logger.warning(
                "max_concurrent_downloads is more than 32, becareful of server limits. "
            )
        self.min_split_size = self.parse_size(self.min_split_size)
        self.chunk_retry_speed = self.parse_size(self.chunk_retry_speed)
        if self.force_sequential:
            self.max_concurrent_downloads = 1
            self._logger.info("Force sequential download enabled.")
        self.max_downloads = int(self.max_downloads)
        if self.max_downloads < 1:
            self.max_downloads = 1
            self._logger.warning("threads cannot be less than 1. Setting to 1.")
        elif self.max_downloads > 32:
            self._logger.warning(
                "threads are more than 32, may cause high resource usage. "
            )
        if isinstance(self.user_agent, str):
            try:
                self.user_agent = json.loads(self.user_agent)
            except Exception:
                self.user_agent = {"User-Agent": self.user_agent}

    def parse_size(self, size_str: str) -> int:
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

    def add_urls(self, url_list: dict | list[str]):
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

    def load_input_file(self, input_file: str):
        with open(input_file, "r") as f:
            content = f.read()
            try:
                data = json.loads(content)
                self.add_urls(data)
            except json.JSONDecodeError:
                try:
                    data = yaml.safe_load(content)
                    self.add_urls(data)
                except Exception:
                    url_list = content.splitlines()
                    url_list = [url.strip() for url in url_list if url.strip()]
                    self.add_urls(url_list)

    def append(
        self,
        url: str,
        md5: str = None,
        file_name: str = None,
        dir_path: str = os.getcwd(),
        log_path: str = None,
    ):
        asyncio.run(self.aappend(url, md5, file_name, dir_path, log_path))

    async def aappend(
        self,
        url: str,
        md5: str = None,
        file_name: str = None,
        dir_path: str = os.getcwd(),
        log_path: str = None,
    ):
        async with self._urls_lock:
            self._urls[url] = Downloader(
                self, url, dir_path, filename=file_name, md5=md5, log_path=log_path
            )
            self._logger.debug(f"Added URL: {url}")

    def pop(self, url: str):
        return asyncio.run(self.apop(url))

    async def apop(self, url: str):
        async with self._urls_lock:
            result = self._urls.pop(url, None)
        self._logger.debug(f"Removed URL: {url}")
        return result

    async def wait(self, downloaders: list[asyncio.Task] = None):
        if downloaders is None:
            downloaders = self._downloaders
        while downloaders:
            try:
                done, pending = await asyncio.wait(
                    downloaders, return_when=asyncio.FIRST_COMPLETED
                )
            except Exception as e:
                self._logger.error(f"wait error: {e}")
                self._logger.error(traceback.format_exc())
                return
            for d in done:
                try:
                    _url = d.result()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self._logger.error(f"task error: {e}")
                    self._logger.error(traceback.format_exc())
            downloaders = pending

    async def download(self):
        self._downloader_main = asyncio.create_task(self._download_once())
        try:
            await self._downloader_main
        finally:
            self._downloader_main = None

    async def _download_once(self):
        self._logger.debug(self)
        self._downloaders = []
        downloading = {}
        if self._urls:
            with self._progress:
                while self._urls:
                    async with self._urls_lock:
                        url, download_entity = self._urls.popitem()
                    if url in downloading:
                        continue
                    downloading[url] = True
                    assert isinstance(download_entity, Downloader)
                    if len(self._downloaders) < self.max_downloads:
                        self._downloaders.append(
                            asyncio.create_task(download_entity.start_download())
                        )
                    else:
                        await self.wait(self._downloaders)
                        self._downloaders.append(
                            asyncio.create_task(download_entity.start_download())
                        )
                    self._logger.debug(f"Starting download for {url}")
                await self.wait(self._downloaders)
                await asyncio.sleep(1)

    async def _start_download(self):
        self._logger.debug(self)
        self._downloaders = []
        downloading = {}
        while True:
            if self._urls:
                with self._progress or self._downloaders:
                    while self._urls:
                        async with self._urls_lock:
                            url, download_entity = self._urls.popitem()
                        if url in downloading:
                            continue
                        downloading[url] = True
                        assert isinstance(download_entity, Downloader)
                        if len(self._downloaders) < self.max_downloads:
                            self._downloaders.append(
                                asyncio.create_task(download_entity.start_download())
                            )
                        else:
                            # await self.wait(self._downloaders)
                            done, pending = await asyncio.wait(
                                self._downloaders, return_when=asyncio.FIRST_COMPLETED
                            )
                            for d in done:
                                try:
                                    _url = d.result()
                                except asyncio.CancelledError:
                                    pass
                                except Exception as e:
                                    self._logger.error(f"task error: {e}")
                                    self._logger.error(traceback.format_exc())
                                finally:
                                    self._downloaders.remove(d)
                            self._downloaders.append(
                                asyncio.create_task(download_entity.start_download())
                            )
                        await asyncio.sleep(0.1)
                        if not self._urls:
                            break
            await asyncio.sleep(1)

    async def start_download(self):  # 持续下载
        self._downloader_main = asyncio.create_task(self._start_download())

    async def stop_download(self):  # 停止持续下载
        if self._downloader_main:
            self._downloader_main.cancel()
            self._downloader_main = None

    def urls(self) -> List[str]:
        return list(self._urls.keys())

    def __str__(self):
        return f"Manager(threads={self.max_downloads}, timeout={self.timeout}, retry={self.retry}, debug={self.debug}, continue_download={self.continue_download}, max_concurrent_downloads={self.max_concurrent_downloads}, min_split_size={self.min_split_size})"

    def __del__(self):
        asyncio.run(self.stop_download())

    # 支持with语法
    async def __aenter__(self):
        await self.continue_download()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.wait()
        await self.stop_download()

    def __enter__(self):
        asyncio.run(self.__aenter__())
        return self

    def __exit__(self, exc_type, exc, tb):
        asyncio.run(self.__aexit__(exc_type, exc, tb))
