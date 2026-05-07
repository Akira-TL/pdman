---
name: "logger-skill"
description: "Provides FlexibleLogger pattern integrating loguru + rich for Python logging. Invoke when implementing logging utilities, CLI tools needing progress bars, or backend service logging."
---

# FlexibleLogger Skill - 代码写法指南

## 核心设计思想

### 设计目标

实现 loguru 与 rich 的无缝集成，支持两种模式灵活切换：

| 场景 | 推荐模式 | 特点 |
|------|----------|------|
| 后端服务/无进度需求 | Simple 模式 | 轻量、无额外依赖 |
| CLI 工具/需要进度条 | Rich 模式 | 美观、支持进度展示 |

### 核心原理

通过创建**独立的 loguru Logger 实例**，避免全局配置污染：

```python
from loguru._logger import Logger, Core

logger = Logger(
    core=Core(),      # 关键：创建独立核心，不影响全局
    colors=True,      # 保留 ANSI 颜色码
    record=False,
    lazy=False,
)
```

---

## 代码写法

### 写法一：纯 loguru 模式（Simple Mode）

适合不需要进度条的场景，轻量级实现：

```python
import sys
from loguru._logger import Logger, Core

class SimpleLogger:
    def __init__(self, log_path=None, debug=False):
        self._logger = Logger(
            core=Core(),
            colors=True,
            record=False,
            lazy=False,
        )
        self._logger.remove()
        
        log_format = "<green>{time:MM-DD HH:mm:ss}</green> [<level>{level}</level>] <cyan>{name}</cyan> | {message}"
        
        self._logger.add(
            sys.stdout,
            level="DEBUG" if debug else "INFO",
            colorize=True,
            format=log_format,
        )
        
        if log_path:
            self._logger.add(
                log_path,
                level="DEBUG" if debug else "INFO",
                colorize=True,
                format=log_format,
            )
    
    def __getattr__(self, name):
        return getattr(self._logger, name)

# 使用示例
logger = SimpleLogger(debug=True)
logger.info("Hello, Simple Mode!")
```

### 写法二：loguru + rich 模式（Rich Mode）

适合需要进度条的 CLI 工具：

```python
import sys
from loguru._logger import Logger, Core
from rich.console import Console
from rich.text import Text
from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn

class RichLogger:
    def __init__(self, log_path=None, debug=False):
        self._logger = Logger(
            core=Core(),
            colors=True,
            record=False,
            lazy=False,
        )
        self._logger.remove()
        
        self._console = Console()
        self._progress = None
        
        log_format = "<green>{time:MM-DD HH:mm:ss}</green> [<level>{level}</level>] <cyan>{name}</cyan> | {message}"
        
        # 关键：通过 rich Console 输出
        self._logger.add(
            lambda msg: self._console.print(Text.from_ansi(str(msg))),
            level="DEBUG" if debug else "INFO",
            colorize=True,
            format=log_format,
        )
        
        if log_path:
            self._logger.add(
                log_path,
                level="DEBUG" if debug else "INFO",
                colorize=True,
                format=log_format,
            )
    
    def init_progress(self, *columns):
        if not columns:
            columns = (
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                DownloadColumn(binary_units=True),
            )
        self._progress = Progress(*columns, console=self._console)
        return self._progress
    
    def __getattr__(self, name):
        return getattr(self._logger, name)

# 使用示例
logger = RichLogger(debug=True)
logger.info("Hello, Rich Mode!")

progress = logger.init_progress()
progress.start()
task_id = progress.add_task("Downloading...", total=100)
for i in range(100):
    progress.update(task_id, completed=i + 1)
progress.stop()
```

### 写法三：双模式切换（推荐）

根据配置自动切换模式：

```python
import sys
from loguru._logger import Logger, Core
from rich.console import Console
from rich.text import Text
from rich.progress import Progress, TextColumn, BarColumn

class FlexibleLogger:
    def __init__(self, mode="simple", log_path=None, debug=False):
        self.mode = mode.lower()
        self._logger = Logger(
            core=Core(),
            colors=True,
            record=False,
            lazy=False,
        )
        self._logger.remove()
        self._console = None
        self._progress = None
        
        log_format = "<green>{time:MM-DD HH:mm:ss}</green> [<level>{level}</level>] <cyan>{name}</cyan> | {message}"
        
        if self.mode == "rich":
            self._console = Console()
            self._logger.add(
                lambda msg: self._console.print(Text.from_ansi(str(msg))),
                level="DEBUG" if debug else "INFO",
                colorize=True,
                format=log_format,
            )
        else:
            self._logger.add(
                sys.stdout,
                level="DEBUG" if debug else "INFO",
                colorize=True,
                format=log_format,
            )
        
        if log_path:
            self._logger.add(
                log_path,
                level="DEBUG" if debug else "INFO",
                colorize=True,
                format=log_format,
            )
    
    def init_progress(self, *columns):
        if self.mode != "rich":
            self.warning("Progress bar only available in 'rich' mode")
            return None
        
        if not columns:
            columns = (
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
            )
        self._progress = Progress(*columns, console=self._console)
        return self._progress
    
    def __getattr__(self, name):
        return getattr(self._logger, name)

# 使用示例
logger = FlexibleLogger(mode="simple", debug=True)  # 简单模式
logger = FlexibleLogger(mode="rich", debug=True)    # 富文本模式
```

---

## 使用场景示例

### 场景 1：后端服务日志

```python
logger = FlexibleLogger(mode="simple", log_path="/var/log/app.log")

def process_request(request):
    logger.debug(f"Received request: {request}")
    try:
        result = do_something(request)
        logger.info(f"Request processed: {result}")
        return result
    except Exception as e:
        logger.exception(f"Error processing request")
        raise
```

### 场景 2：CLI 下载工具

```python
import time

logger = FlexibleLogger(mode="rich", debug=True)

def download_file(url, save_path):
    logger.info(f"Starting download: {url}")
    
    progress = logger.init_progress()
    progress.start()
    task_id = progress.add_task(f"Downloading {url}", total=100)
    
    try:
        for i in range(100):
            progress.update(task_id, completed=i + 1)
            time.sleep(0.05)
        logger.info(f"Download completed: {save_path}")
    finally:
        progress.stop()
```

### 场景 3：异步任务日志

```python
import asyncio

logger = FlexibleLogger(mode="rich", debug=True)

async def async_task(task_name):
    logger.info(f"Starting task: {task_name}")
    try:
        await some_async_operation()
        logger.info(f"Task completed: {task_name}")
    except Exception as e:
        logger.exception(f"Task failed: {task_name}")

async def main():
    tasks = [async_task("Task 1"), async_task("Task 2"), async_task("Task 3")]
    await asyncio.gather(*tasks)

asyncio.run(main())
```

---

## 最佳实践

### 1. 日志级别控制

```python
# 开发环境
logger = FlexibleLogger(debug=True)  # DEBUG 级别

# 生产环境
logger = FlexibleLogger(debug=False)  # INFO 级别
```

### 2. 多输出目标

```python
logger = FlexibleLogger(
    mode="rich",
    log_path="/var/log/app.log",
    debug=True
)
```

### 3. 自定义进度条

```python
from rich.progress import TextColumn, BarColumn, TransferSpeedColumn

columns = (
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    TransferSpeedColumn(),
)

logger = FlexibleLogger(mode="rich")
progress = logger.init_progress(*columns)
```

### 4. 异常处理

```python
try:
    risky_operation()
except Exception as e:
    logger.exception("Operation failed")
```

---

## 核心要点总结

| 要点 | 说明 |
|------|------|
| **独立实例** | 使用 `Logger(Core())` 创建独立 logger |
| **Rich 桥接** | 通过 `lambda msg: console.print(Text.from_ansi(str(msg)))` 桥接 |
| **格式统一** | 使用 loguru 的格式化语法 |
| **模式切换** | 根据需求选择 simple/rich 模式 |
| **进度条集成** | Rich 模式下通过 `Progress` 类实现 |
