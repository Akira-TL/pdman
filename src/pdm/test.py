from .utils import *


def test_auto_sync():
    @auto_sync
    async def test_async():
        print("This is an async function.")

    async def main():
        await test_async()

    asyncio.run(main())
