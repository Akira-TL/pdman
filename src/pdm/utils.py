import asyncio


def auto_sync(func):
    """
    装饰器：将异步函数转换为同步函数。
    适用于需要在同步上下文中调用异步函数的场景。
    args:
        func: async function to be wrapped
    """

    def wrapper(*args, **kwargs):
        # 判断上下文是否已经在事件循环中
        try:
            asyncio.get_running_loop()
            return func(*args, **kwargs)  # 如果已经在事件循环中，直接返回原函数
        except RuntimeError:
            return asyncio.run(
                func(*args, **kwargs)
            )  # 否则，使用 asyncio.run 执行异步函数

    return wrapper
