import random
import os
import time
import asyncio
import aiohttp
import aiofiles

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .downloader import Downloader


class Chunk:
    def __init__(
        self,
        parent: Downloader,
        start: int,
        end: int,
        chunk_path: str,
        forward: Chunk = None,
        next: Chunk = None,
    ):
        self.parent = parent
        self.start = start
        self.end = end
        self.chunk_path = chunk_path
        if os.path.exists(chunk_path):
            self.size = os.path.getsize(chunk_path)
        else:
            self.size = 0
        self.forward: Chunk = forward
        self.next: Chunk = next

    def __iter__(self):
        current = self
        while current:
            yield current
            current = current.next

    def __str__(self):
        return f"Chunk(start={self.start}, end={self.end},target size={(self.end - self.start + 1) if self.end is not None else -1}, size={self.size}, chunk_path={self.chunk_path})"

    def __add__(self, other):
        if not isinstance(other, Chunk):
            return NotImplemented
        return self.size + other.size

    def __radd__(self, other):
        if other == 0:
            return self.size
        if not isinstance(other, int):
            return NotImplemented
        return self.size + other

    def _is_complete(self) -> bool:
        if self.end is None:
            self.parent._downloaded = True
            return self.size > 0
        return self.end is not None and self.size == self.end - self.start + 1

    def _needs_download(self) -> bool:
        return self.end is None or self.size < self.end - self.start + 1

    def _apply_range_header(self, headers: dict):
        if self.end is not None:
            if self.start + self.size <= self.end:
                headers["Range"] = f"bytes={self.start + self.size}-{self.end}"
            else:
                headers["break"] = True
        else:
            if "Range" in headers:
                headers.pop("Range")

    async def _stream_response(self, response, f) -> bool:
        last_time = time.time()
        pos = await f.tell()
        continue_flag = False
        async for data in response.content.iter_chunked(10240):
            if self.end is not None:
                remaining = self.end - self.start + 1 - pos
                if remaining <= 0:
                    break
                data = data[:remaining]
            await f.write(data)
            async with self.parent.lock:
                self.size += len(data)
                now = time.time()
                elaps = max(now - last_time, 1e-6)
                speed = len(data) / elaps
                if (
                    self.parent.parent.chunk_retry_speed
                    and speed < self.parent.parent.chunk_retry_speed
                ):
                    continue_flag = True
                last_time = now
            pos += len(data)
        return continue_flag

    async def _split_incomplete(self):
        if self.size != self.end - self.start + 1:
            self.parent._logger.debug(
                f"Chunk not fully downloaded, splitting chunk: {self}"
            )
            async with self.parent.lock:
                new_start = self.start + self.size
                new_chunk = Chunk(
                    self.parent,
                    new_start,
                    self.end,
                    os.path.join(
                        self.parent.pdm_tmp,
                        f"{self.parent.filename}.{new_start}",
                    ),
                    self,
                    next=self.next,
                )
                self.end = new_start - 1
                self.next = new_chunk

    async def download(self):
        assert self.end is not None or self.size >= 0
        headers = {}  # TODO 添加其他必要的headers
        file_mode = "ab" if os.path.exists(self.chunk_path) else "wb"
        async with (
            aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(sock_read=30)
            ) as session,
            aiofiles.open(self.chunk_path, file_mode) as f,
        ):
            for _ in range(self.parent.parent.retry):
                if os.path.exists(self.chunk_path) and self._is_complete():
                    return self
                while True:
                    try:
                        self._apply_range_header(headers)
                        if headers.get("break"):
                            break
                        self.parent._logger.debug(
                            f"Downloading chunk: {self}, with headers: {headers}"
                        )
                        if self._needs_download():
                            async with session.get(
                                self.parent.url,
                                headers=headers,
                                timeout=self.parent.parent.chunk_timeout,
                            ) as response:
                                if response.status in (200, 206):
                                    continue_flag = await self._stream_response(
                                        response, f
                                    )
                                    if continue_flag:
                                        await asyncio.sleep(
                                            self.parent.parent.retry_wait
                                        )
                                        self.parent._logger.debug(
                                            "speed is low restarting..."
                                        )
                                        continue
                    except aiohttp.client_exceptions.ClientPayloadError:
                        await asyncio.sleep(
                            self.parent.parent.retry_wait + random.random() * 5
                        )
                    except asyncio.TimeoutError:
                        self.parent._logger.debug(
                            f"Timeout downloading chunk {self}, retrying..."
                        )
                        await asyncio.sleep(
                            self.parent.parent.retry_wait + random.random() * 5
                        )
                    except ConnectionResetError:
                        self.parent._logger.debug(
                            f"Connection reset downloading chunk {self}, retrying..."
                        )
                        await asyncio.sleep(
                            self.parent.parent.retry_wait + random.random() * 5
                        )
                    except Exception as e:
                        self.parent._logger.debug(
                            f"Error downloading chunk {self}: {e}"
                        )
                        # traceback.print_exc()
                        await asyncio.sleep(self.parent.parent.retry_wait)
                        break
                    if self._is_complete():
                        return self
                if self.end is None:
                    self.parent._logger.debug(
                        f"completed download chunk (unknown size): {self}"
                    )
                    self.parent._downloaded = True
                    return self
                elif self._needs_download():
                    self.parent._logger.debug(f"retrying download chunk: {self}")
                else:
                    self.parent._logger.debug(f"completed download chunk: {self}")
        await self._split_incomplete()
        return self
