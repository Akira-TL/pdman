import asyncio

import pytest

from pdman.downloader import ConnectionTimeoutSkip, Downloader
from pdman.manager import Manager


def test_connection_timeout_shows_progress_then_skips(tmp_path):
    async def run_case():
        manager = Manager(
            connect_timeout=0.05,
            connect_progress_delay=0.01,
            summary_interval=0.01,
            log_path=None,
        )
        downloader = Downloader(manager, "https://example.com/slow.bin", str(tmp_path))

        async def never_connects():
            await asyncio.sleep(60)

        with manager._progress:
            with pytest.raises(ConnectionTimeoutSkip):
                await downloader._await_connection(
                    never_connects(), label="https://example.com/slow.bin"
                )

        assert len(manager._progress.tasks) == 0

    asyncio.run(run_case())


def test_default_connection_timeout_is_30_seconds():
    manager = Manager(log_path=None)

    assert manager.connect_timeout == 30
    assert manager.connect_progress_delay == 5.0
