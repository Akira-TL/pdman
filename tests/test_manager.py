import asyncio
import types

from pdman.downloader import Downloader
from pdman.manager import Manager


def test_max_downloads_limits_url_task_concurrency(tmp_path):
    async def run_case():
        manager = Manager(max_downloads=1, log_path=None)
        state = {"active": 0, "max_seen": 0}

        async def fake_start_download(self):
            state["active"] += 1
            state["max_seen"] = max(state["max_seen"], state["active"])
            await asyncio.sleep(0.02)
            state["active"] -= 1
            return self.url

        for index in range(3):
            url = f"https://example.com/file-{index}.bin"
            downloader = Downloader(manager, url, str(tmp_path))
            downloader.start_download = types.MethodType(fake_start_download, downloader)
            manager._urls[url] = downloader

        await manager._download_once()

        assert state["max_seen"] == 1
        assert manager._downloaders == []

    asyncio.run(run_case())
