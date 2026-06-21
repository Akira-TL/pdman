import re
import os
import json
import time
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
from loguru._logger import Logger, Core

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .manager import Manager
from .chunk import Chunk


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
        self.lock = asyncio.Lock()
        self.header_info = None
        self.log_path = log_path
        self._downloaded = False
        self._done = False
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
        if self.pdm_tmp is None and self.parent.tmp_dir is not None:
            self.pdm_tmp = os.path.join(self.parent.tmp_dir, f".pdman.{sha}")
        elif self.pdm_tmp is None and self.parent.tmp_dir is None:
            self.pdm_tmp = os.path.join(self.filepath, f".pdman.{sha}")
        os.makedirs(self.pdm_tmp, exist_ok=True)
        self.header_info = await self.get_headers()
        self.filename = self.filename if self.filename else await self.get_file_name()
        os.makedirs(self.filepath, exist_ok=True)
        self.file_size = self.file_size or await self.get_url_file_size()
        self.creat_info()
        self.chunk_root = await self.rebuild_task()
        if self.chunk_root is None:
            self.chunk_root = await self.build_task()
        # 单任务限速器（在 Manager 全局限速之外）
        self._per_task_limiter = None
        if self.parent.max_download_limit:
            from .manager import RateLimiter
            self._per_task_limiter = RateLimiter(self.parent.max_download_limit)


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
        async with self._build_client_session() as session:
            headers_to_send = {"Accept-Encoding": "identity"}
            # 合并全局自定义 HTTP 头
            if self.parent.headers_dict:
                headers_to_send.update(self.parent.headers_dict)
            async with session.head(
                self.url,
                allow_redirects=True,
                timeout=self.parent.timeout,
                headers=headers_to_send,
            ) as response:
                if response.status in (200, 206):
                    return response.headers
                else:
                    raise Exception(
                        f"Failed to get header info, status code: {response.status},headers:{response.headers}"
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

    async def check_integrity(self):
        if self.parent.check_integrity:
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
                    f"{self.filename}MD5 checksum does not match! Expected: {self.md5}, Got: {file_md5}"
                )
                return False

    async def start_download(self, _iter=None):
        if _iter is None:
            _iter = self.parent.retry
        parsed = False
        while _iter >= 0:
            try:
                if not parsed:
                    await self.parse_config()
                    parsed = True
                # quit 模式：目标文件已存在则跳过
                if self.parent.quit_if_exists and os.path.exists(
                    os.path.join(self.filepath, self.filename)
                ):
                    self._logger.info(
                        f"File {self.filename} already exists, skipping."
                    )
                    self._done = True
                    return self.url
                if (
                    self.filename is not None
                    and os.path.exists(os.path.join(self.filepath, self.filename))
                    and not self.parent.auto_file_renaming
                ):
                    await self.parent.pop(self.url)
                    return self.url
                self.task = None
                await self._start_download()
                await self.merge_chunks()
                await self.check_integrity()
                self._done = True
                break
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                if _iter > 0:
                    _iter -= 1
                    await asyncio.sleep(self.parent.retry_wait)
                else:
                    self._logger.error(
                        f"Failed to download {self.filename} from {self.url}"
                    )
                    raise Exception(
                        f"Failed to download {self.url} after retries."
                    ) from e
        if self._done:
            # 下载完成回调
            if self.parent.on_download_complete:
                self._run_download_complete_callback()
            self.parent._logger.success(
                f"Finished download {self.filename} from {self.url}"
            )
            return self.url

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
                    await asyncio.sleep(self.parent.summary_interval)
            else:
                self.task = self.parent._progress.add_task(
                    f"Downloading {self.filename}", total=self.file_size, dl=len(tasks)
                )
                while self.file_size > 0:
                    async with self.lock:
                        completed = sum(self.chunk_root)
                    if self.file_size <= completed:
                        break
                    self.parent._progress.update(
                        self.task, completed=completed, dl=len(tasks)
                    )
                    await asyncio.sleep(self.parent.summary_interval)
                self.parent._progress.update(
                    self.task, completed=completed, dl=len(tasks)
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
