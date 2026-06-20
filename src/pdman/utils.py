import asyncio


def auto_sync(func):
    """
    装饰器：使异步函数在同步和异步上下文中都能正确执行。
    - 在同步上下文中（无运行中的事件循环）：使用 asyncio.run() 执行
    - 在异步上下文中（有运行中的事件循环）：返回协程，由调用者 await

    注意：在异步上下文中调用时，调用者必须 await 返回值，否则操作不会实际执行。
    """

    def wrapper(*args, **kwargs):
        try:
            asyncio.get_running_loop()
            return func(*args, **kwargs)  # 在事件循环中，返回协程供 await
        except RuntimeError:
            return asyncio.run(
                func(*args, **kwargs)
            )  # 不在事件循环中，同步执行

    return wrapper
