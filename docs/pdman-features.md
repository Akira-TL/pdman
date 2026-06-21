# PDMAN 功能增强文档

## 概述

PDMAN 在现有并行分段下载能力的基础上，参照 aria2c 的功能集，新增了以下特性：

- HTTP 认证（Basic/Digest）
- Cookie 文件加载
- 单任务/全局速度限制（令牌桶算法）
- HTTP/HTTPS 代理及代理认证
- 自定义 HTTP 请求头
- 独立连接超时
- 单服务器连接数限制
- Referer 设置
- 下载完成回调钩子
- SSL 证书验证控制
- 配置文件支持（JSON/YAML）
- quit 模式（文件已存在则跳过）
- 可配置进度刷新间隔

## 使用方式

### 命令行参数

| 参数 | 简写 | 格式 | 说明 |
|------|------|------|------|
| `--http-auth` | — | `user:pass` | HTTP 认证 |
| `--cookie-file` | — | 文件路径 | Netscape/Mozilla cookie 文件 |
| `--max-download-limit` | — | 带单位大小 | 单任务限速，如 `1M` / `500K` |
| `--max-overall-download-limit` | — | 带单位大小 | 全局限速 |
| `--proxy` | — | URL | 代理地址 |
| `--proxy-auth` | — | `user:pass` | 代理认证 |
| `--header` | — | `Key: Value` | 自定义 HTTP 头（可重复） |
| `--connect-timeout` | — | 整数秒 | 连接超时 |
| `--max-connection-per-server` | — | 整数 | 单 host 最大连接数 |
| `--referer` | — | URL | Referer 头 |
| `--on-download-complete` | — | shell 命令 | 下载完成回调 |
| `--no-check-certificate` | — | — | 不验证 SSL 证书 |
| `--ca-certificate` | — | 文件路径 | 自定义 CA 证书 |
| `--conf-path` | — | 文件路径 | 配置文件路径 |
| `-q` / `--quit` | `-q` | — | 文件已存在则跳过 |
| `--summary-interval` | — | 浮点数秒 | 进度刷新间隔 |

### 回调命令占位符

`--on-download-complete` 指定的命令中可用以下占位符：

| 占位符 | 替换为 |
|--------|--------|
| `{filename}` | 文件名 |
| `{filepath}` | 完整输出路径 |
| `{url}` | 下载 URL |
| `{dir}` | 输出目录 |
| `{size}` | 文件大小（字节） |

示例：
```bash
pdman --on-download-complete "echo 'Downloaded {filename} to {filepath}' | mail user@example.com" \
      http://example.com/file.zip
```

### 配置文件格式

支持 JSON 和 YAML 格式，键与 CLI 长参数名一致：

```yaml
# pdman-config.yaml
max_downloads: 8
timeout: 120
retry: 5
debug: true
proxy: "http://127.0.0.1:7890"
check_certificate: false
auto_file_renaming: true
```

使用方式：
```bash
pdman --conf-path pdman-config.yaml http://example.com/file.zip
```

CLI 参数优先级高于配置文件。

### Python API

```python
from pdman import Manager

async with Manager(
    proxy="http://127.0.0.1:7890",
    http_auth="user:pass",
    headers=["X-Custom: value1", "Authorization: Bearer token"],
    max_download_limit="5M",  # 单任务 5 MiB/s 限速
    on_download_complete="notify-send 'Download: {filename} done'",
) as pdman:
    pdman.append("http://example.com/file.zip")
    await pdman.wait()
```

## 技术实现

### 限速机制（令牌桶算法）

限速采用令牌桶算法实现，位于 `manager.py` 的 `RateLimiter` 类：

- 定时补充令牌，补充量与经过时间成正比
- 每次写入数据前消耗对应字节数的令牌
- 令牌不足时通过 `asyncio.sleep()` 等待补足
- 全局限速锁仅在线程安全保护令牌桶，每次 `acquire()` 持锁时间极短

### 配置传递链路

```
CLI args → Manager.__init__
  ├─ RateLimiter（全局限速）
  ├─ headers_dict（解析后的自定义头）
  └─ ... 所有其他属性
       ↓  (Manager 作为 self.parent)
Downloader.parse_config()
  ├─ RateLimiter（单任务限速）
  └─ _build_client_session() → 给所有 HTTP 请求
       ↓  (Downloader 作为 self.parent)
Chunk.download()
  ├─ 合并 headers_dict
  └─ _stream_response() → RateLimiter.acquire()
```

### Session 工厂方法

`Downloader._build_client_session()` 统一构造所有 HTTP 连接所需的 `aiohttp.ClientSession`，确保以下配置一致应用于所有请求（chunk 下载、HEAD 请求、MD5 校验等）：

- 超时（total / connect / sock_read）
- SSL 证书验证
- Cookie Jar
- 代理及代理认证
- HTTP 认证
- 连接数限制
