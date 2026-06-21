# PDMAN 功能增强实施计划

## Context

PDMAN 是与 aria2c 对齐的异步并行分段下载器。当前已实现核心下载功能，但在网络层（认证、代理、Cookie、限速、自定义头等）存在大量缺失。本计划参照 aria2c 的功能集，按优先级补齐特性。

## 涉及文件

- `src/pdman/manager.py` — 配置中心 + RateLimiter 类
- `src/pdman/downloader.py` — session 工厂方法 + 回调钩子
- `src/pdman/chunk.py` — 限速集成 + session 构造替换
- `src/pdman/cli.py` — 新增 CLI 参数

---

## Manager 新增参数

在 `Manager.__init__` 和 `config()` 中新增（`_reparse_download_params` 中解析）：

| 参数名 | 类型 | 默认值 | 功能 |
|--------|------|--------|------|
| `http_auth` | str | None | HTTP Basic/Digest 认证 `user:pass` |
| `cookie_file` | str | None | Netscape/Mozilla cookie 文件路径 |
| `max_download_limit` | str\|int | None | 单任务下载限速（支持 K/M/G） |
| `max_overall_download_limit` | str\|int | None | 全局限速 |
| `proxy` | str | None | 代理 URL |
| `proxy_auth` | str | None | 代理认证 `user:pass` |
| `headers` | list[str] | None | 自定义 HTTP 头列表 |
| `connect_timeout` | int | None | 连接超时（秒） |
| `max_connection_per_server` | int | 0 | 单 host 最大连接数，0=不限制 |
| `on_download_complete` | str | None | 下载完成回调命令 |
| `referer` | str | None | Referer 头 |
| `check_certificate` | bool | True | 是否验证 SSL 证书 |
| `ca_certificate` | str | None | 自定义 CA 证书路径 |
| `conf_path` | str | None | 配置文件路径 |
| `quit_if_exists` | bool | False | 文件已存在则跳过 |
| `summary_interval` | float | 1.0 | 进度刷新间隔（秒） |

---

## 实施步骤

### 步骤 1：manager.py — 新增 RateLimiter 类 + 所有新参数

**RateLimiter 类**（令牌桶算法，放在 manager.py 顶部）：

```python
class RateLimiter:
    """令牌桶限速器"""
    def __init__(self, max_rate: int | None):
        self.max_rate = max_rate
        self._tokens = float(max_rate) if max_rate else float('inf')
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, byte_count: int) -> None:
        if self.max_rate is None:
            return
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self.max_rate),
                               self._tokens + self.max_rate * elapsed)
            self._last_refill = now
            if byte_count > self._tokens:
                wait_time = (byte_count - self._tokens) / self.max_rate
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
                self._last_refill = time.monotonic()
            else:
                self._tokens -= byte_count
```

**Manager.__init__ 新增**：
- 全部 17 个新参数（含默认值）
- `self._global_limiter = None`（稍后在 _reparse 中初始化）
- `self.summary_interval = summary_interval`

**_reparse_download_params 新增**：
- `self.max_download_limit = self._parse_size(...)`
- `self.max_overall_download_limit = self._parse_size(...)`
- `self._global_limiter = RateLimiter(self.max_overall_download_limit)`
- headers 列表解析为 dict（`self.headers_dict`）
- http_auth 字符串解析为 `aiohttp.BasicAuth` 对象
- proxy_auth 字符串解析为 `aiohttp.BasicAuth` 对象

**Progress 构造修改**：
- `refresh_per_second=1.0 / max(self.summary_interval, 0.1)`

### 步骤 2：cli.py — 新增全部 CLI 参数

| CLI 参数 | 简写 | 格式 |
|----------|------|------|
| `--http-auth` | - | `user:pass` |
| `--cookie-file` | - | 文件路径 |
| `--max-download-limit` | - | 带单位大小 |
| `--max-overall-download-limit` | - | 带单位大小 |
| `--proxy` | - | URL |
| `--proxy-auth` | - | `user:pass` |
| `--header` | - | `Key: Value`，action=append |
| `--connect-timeout` | - | 整数秒 |
| `--max-connection-per-server` | - | 整数 |
| `--on-download-complete` | - | shell 命令 |
| `--referer` | - | URL 字符串 |
| `--no-check-certificate` | - | store_false |
| `--ca-certificate` | - | 文件路径 |
| `--conf-path` | - | 文件路径 |
| `-q` | `-q` | store_true |
| `--summary-interval` | - | 浮点数秒 |

传给 Manager 时 headers 需要先从 list[dict] 合并为单个 dict。

### 步骤 3：downloader.py — _build_client_session 工厂方法

在 Downloader 类中新增方法，统一构造 ClientSession：

```python
def _build_client_session(self, **overrides) -> aiohttp.ClientSession:
    mgr = self.parent
    timeout = aiohttp.ClientTimeout(
        total=mgr.timeout or 300,
        connect=mgr.connect_timeout,
        sock_read=mgr.chunk_timeout or 30,
    )
    # SSL
    ssl_context = None
    if mgr.ca_certificate:
        import ssl
        ssl_context = ssl.create_default_context(cafile=mgr.ca_certificate)
    connector_kw = {
        "limit": 0,
        "verify_ssl": mgr.check_certificate,
        "ssl": ssl_context,
    }
    if mgr.max_connection_per_server:
        connector_kw["limit_per_host"] = mgr.max_connection_per_server
    connector = aiohttp.TCPConnector(**{k:v for k,v in connector_kw.items() if v is not None})
    # Cookie
    cookie_jar = aiohttp.CookieJar()
    if mgr.cookie_file and os.path.exists(mgr.cookie_file):
        cookie_jar.load(mgr.cookie_file)
    # 组装
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
    return aiohttp.ClientSession(**{k:v for k,v in kwargs.items() if v is not None})
```

### 步骤 4：chunk.py / downloader.py — 替换 ClientSession 创建

所有 session 创建点改为 `self.parent._build_client_session()`：
- `Chunk.download()`（chunk.py 第 142-144 行）
- `Downloader.get_headers()`（downloader.py 第 156 行）
- `Downloader.get_file_name()`（downloader.py 第 138 行）
- `Downloader.process_md5()`（downloader.py 第 120 行）

### 步骤 5：HTTP 认证 + 自定义头 + Referer 集成

在 `Chunk.download()` 中：
```python
# 合并全局自定义头
if self.parent.parent.headers_dict:
    headers.update(self.parent.parent.headers_dict)
# 添加 Referer
if self.parent.parent.referer:
    headers.setdefault("Referer", self.parent.parent.referer)
```

在 `Downloader.get_headers()` 中也合并 headers_dict。

### 步骤 6：限速集成

在 `Chunk._stream_response()` 数据写入后：
```python
# 单任务限速（通过 Downloader）
if self.parent._per_task_limiter:
    await self.parent._per_task_limiter.acquire(len(data))
# 全局限速（通过 Manager）
if self.parent.parent._global_limiter:
    await self.parent.parent._global_limiter.acquire(len(data))
```

Downloader.parse_config 中：
```python
self._per_task_limiter = RateLimiter(self.parent.max_download_limit) \
    if self.parent.max_download_limit else None
```

### 步骤 7：回调钩子

在 `Downloader.start_download()` 中 `self._done = True` 之后：
```python
if self._done and self.parent.on_download_complete:
    dest = os.path.join(self.filepath, self.filename)
    cmd = self.parent.on_download_complete
    cmd = (cmd.replace('{filename}', self.filename)
              .replace('{filepath}', dest)
              .replace('{url}', self.url)
              .replace('{dir}', self.filepath)
              .replace('{size}', str(self.file_size)))
    asyncio.create_task(self._run_callback(cmd))

async def _run_callback(self, cmd):
    try:
        proc = await asyncio.create_subprocess_shell(cmd)
        await proc.wait()
    except Exception as e:
        self._logger.error(f"Callback failed: {e}")
```

### 步骤 8：quit 模式 + 配置文件 + 进度间隔

**quit 模式**：在 `Downloader.start_download()` 中 `parse_config` 之后：
```python
if self.parent.quit_if_exists and os.path.exists(
    os.path.join(self.filepath, self.filename)
):
    self._logger.info(f"File {self.filename} already exists, skipping.")
    self._done = True
    return self.url
```

**配置文件**：在 `Manager._load_config_file()` 中加载 YAML/JSON，`__init__` 中对 None 参数用配置文件值回填（CLI 优先）。

**进度间隔**：`Progress(refresh_per_second=...)` + `progress_run()` 中 `sleep(summary_interval)`。

---

## 配置传递链路（修改后）

```
CLI args → Manager.__init__
  ├─ RateLimiter（全局限速）
  ├─ headers_dict（解析后的自定义头）
  ├─ http_auth（BasicAuth 对象）
  ├─ _proxy_auth_obj（代理认证对象）
  └─ ... 所有其他属性
       │
       v  (Manager 作为 self.parent)
Downloader.parse_config()
  ├─ _per_task_limiter = RateLimiter(max_download_limit)
  └─ _build_client_session() → 给 Chunk download + get_headers/get_file_name/process_md5
       │
       v  (Downloader 作为 self.parent)
Chunk.download()
  ├─ headers.update(Manager.headers_dict)  ← 自定义头合并
  ├─ headers.setdefault("Referer", ...)    ← Referer
  └─ _stream_response() → RateLimiter.acquire() ← 限速
```

---

## 验证计划

1. **语法检查**：`python3 -m py_compile` 对四个文件分别检查
2. **导入测试**：`from pdman import Manager` + 实例化带全部新参数
3. **CLI 测试**：`python -m pdman.cli --help` 显示所有新参数
4. **限速测试**：小文件下载 + 低限速（100K），观察速度是否被限制
5. **代理测试**：配置本地代理（squid/tinyproxy），验证流量经过
6. **回调测试**：`--on-download-complete "echo '{filename} done'"` 验证执行
7. **resume 场景**：确认断点续传在 http_auth/proxy 场景下正常
8. **边界测试**：
   - cookie_file 指向空文件 / 损坏文件
   - http_auth 格式为 `user`（无冒号）
   - max_download_limit 为 0 或极小值
   - proxy 为无效 URL
