# pdman — Python Parallel Download Manager

`pdman` 是一个使用 Python 实现的异步多段下载器，目标是提供接近 IDM / aria2c 使用体验的命令行下载工具，同时保留可嵌入 Python 项目的 API。

它适合处理大文件、批量 URL、弱网络续传、代理下载、限速下载、带 Cookie / Header / 认证的 HTTP 资源下载等场景。

---

## 主要特性

- **多连接分块下载**：通过 HTTP Range 请求将同一 URL 拆分为多个分块并发下载。
- **断点续传**：使用 `.pdman.<sha>` 临时目录保存分块和元信息，可通过 `--continue` 恢复下载。
- **动态分块调度**：下载过程中可继续拆分较大的空闲区间，提高连接利用率。
- **低速分片重启**：通过 `--chunk-retry-speed` 检测低速分块并自动重试，避免单个连接拖慢整体任务。
- **批量任务**：支持从纯文本、JSON、YAML 文件读取多个下载任务。
- **并发控制**：支持任务级并发、单 URL 分块并发、单服务器连接数限制。
- **限速控制**：支持单任务限速和全局限速。
- **认证与代理**：支持 HTTP Basic / Digest 认证、HTTP / HTTPS 代理和代理认证。
- **Cookie 与请求头**：支持 Netscape / Mozilla Cookie 文件、自定义 Header、Referer、User-Agent。
- **完整性校验**：支持下载后 MD5 校验。
- **SSL 控制**：支持关闭证书校验或指定自定义 CA 证书。
- **完成回调**：下载完成后可执行自定义 shell 命令。
- **Python API**：可在异步 Python 项目中直接调用 `Manager`。

---

## 安装

从 GitHub 安装：

```bash
pip install git+https://github.com/Akira-TL/pdman.git
```

本地开发安装：

```bash
git clone git@github.com:Akira-TL/pdman.git
cd pdman
uv sync
uv pip install -e .
```

也可以使用普通 venv：

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

---

## 快速开始

### 单 URL 下载

```bash
pdman "https://example.com/file.bin"
```

默认输出目录为当前工作目录下的 `pdman/`。文件名优先从 `Content-Disposition` 解析；如果服务器没有提供文件名，则使用 URL 路径末尾；若仍无法判断，则使用 URL 哈希生成 `.dat` 文件名。

### 指定输出目录和文件名

```bash
pdman -d /data/downloads -o file.bin "https://example.com/file.bin"
```

`--out` 只适用于单 URL 下载。多个 URL 同时下载时，文件名由任务或 URL 自动推断。

### 断点续传

```bash
pdman --continue "https://example.com/file.bin"
```

断点续传依赖目标目录下的 `.pdman.<sha>` 临时目录和 `.pdman` 元数据。如果元数据与当前任务不匹配，pdman 会重新初始化对应任务。

### 批量下载

纯文本任务文件：

```text
https://example.com/a.iso
https://example.com/b.zip
```

执行：

```bash
pdman -i urls.txt
```

JSON 任务文件：

```json
{
  "https://example.com/a.iso": {
    "file_name": "linux.iso",
    "dir_path": "/data/downloads",
    "md5": "0123456789abcdef0123456789abcdef",
    "log_path": "/data/logs/a.log"
  }
}
```

YAML 任务文件：

```yaml
https://example.com/a.iso:
  file_name: linux.iso
  dir_path: /data/downloads
  md5: 0123456789abcdef0123456789abcdef
  log_path: /data/logs/a.log
```

执行：

```bash
pdman -i tasks.json
pdman -i tasks.yaml
```

---

## 常用命令

### 并发与分块

```bash
pdman -N 4 -x 8 "https://example.com/file.bin"
```

- `-N, --max-downloads`：同时下载的 URL 数量。
- `-x, --max-concurrent-downloads`：单个 URL 内部分块并发数。
- `-k, --min-split-size`：最小分块大小，例如 `1M`、`512K`。
- `--max-connection-per-server`：单服务器最大连接数，`0` 表示不限制。
- `-Z, --force-sequential`：强制顺序下载。

### 重试与超时

```bash
pdman --retry 5 --retry-wait 3 --timeout 120 --connect-timeout 30 --connect-progress-delay 5 "https://example.com/file.bin"
```

- `--retry`：任务失败重试次数。
- `--retry-wait`：重试等待时间。
- `--timeout`：请求总超时。
- `--connect-timeout`：连接建立超时，默认 30 秒；超时后跳过该 URL。
- `--connect-progress-delay`：连接等待提示延迟，默认 5 秒；超过该时间仍未连通时显示不确定进度条和剩余时间。
- `--chunk-timeout`：分块下载超时。
- `--chunk-retry-speed`：分块低速阈值，例如 `100K`。

### 限速

```bash
pdman --max-download-limit 5M --max-overall-download-limit 20M -i urls.txt
```

- `--max-download-limit`：单任务限速。
- `--max-overall-download-limit`：全局限速。

### 认证、Cookie、代理和请求头

```bash
pdman \
  --http-auth user:pass \
  --cookie-file cookies.txt \
  --proxy http://127.0.0.1:7890 \
  --proxy-auth proxy_user:proxy_pass \
  --header "Authorization: Bearer TOKEN" \
  --referer "https://example.com" \
  "https://example.com/file.bin"
```

### SSL 与证书

```bash
pdman --no-check-certificate "https://example.com/file.bin"
pdman --ca-certificate /path/to/ca.pem "https://example.com/file.bin"
```

### 完整性校验

任务文件中提供 `md5` 字段后，可启用校验：

```bash
pdman --check-integrity -i tasks.yaml
```

`md5` 可以是 32 位 MD5 字符串、本地文件路径，或返回 MD5 文本的 URL。启用完整性校验后，MD5 不匹配会记录为 `failed`，并使 CLI 返回非零退出码。

### Runtime 目录与历史记录

v0.4.0 开始，pdman 默认把分块临时文件放在系统临时目录下的 run 目录中，而不是下载目标目录：

```text
/tmp/pdman/runs/<run-id>/chunks/<task-id>/
```

非 payload 元数据会写入 cache 目录：

```text
~/.cache/pdman/history.jsonl
~/.cache/pdman/runs/<run-id>.json
~/.cache/pdman/active/<run-id>.json
```

`active/<run-id>.json` 只在运行中存在；运行结束后会写入最终 run summary，并追加任务历史记录。

临时目录策略：

```bash
pdman --tmp-policy auto   "https://example.com/file.bin"
pdman --tmp-policy target "https://example.com/file.bin"
pdman --tmp /data/tmp     "https://example.com/file.bin"
pdman --cache-dir /data/cache/pdman "https://example.com/file.bin"
```

- `--tmp` 优先级最高。
- `--tmp-policy auto` 是默认值，优先使用系统临时目录。
- `--tmp-policy system` 强制使用系统临时目录。
- `--tmp-policy target` 保留旧行为，在目标目录旁创建 `.pdman.<sha>`。
- `--cache-dir` 覆盖默认 `~/.cache/pdman`。

### 任务结果与退出码

每次运行结束后，pdman 会汇总任务结果：

```text
Summary:
  completed: 1
  skipped: 1
  failed: 0
  downloaded: 128.0 MiB
```

`skipped` 只表示用户显式启用 `--quit-if-exists` 且目标文件已存在。网络错误、HTTP header 状态异常、连接超时、完整性校验失败等“没有完成下载目标”的情况都会记录为 `failed`；批量任务会继续执行，但最终 CLI 返回 `1`。

当前最小退出码规则：

| 退出码 | 含义 |
| --- | --- |
| `0` | 没有 failed 任务 |
| `1` | 一个或多个任务 failed |
| `130` | 用户中断 |

### 下载完成回调

```bash
pdman --on-download-complete "echo Downloaded {filename} to {filepath}" "https://example.com/file.bin"
```

可用占位符：`{filename}`、`{filepath}`、`{url}`、`{dir}`、`{size}`。

---

## 配置文件

`--conf-path` 支持 JSON / YAML 配置。配置文件中的键与 CLI 长参数名保持一致；命令行参数优先级高于配置文件。

```yaml
max_downloads: 4
max_concurrent_downloads: 8
retry: 5
retry_wait: 3
timeout: 120
connect_timeout: 20
proxy: "http://127.0.0.1:7890"
max_overall_download_limit: "20M"
check_certificate: true
auto_file_renaming: true
summary_interval: 1.0
```

使用：

```bash
pdman --conf-path pdman-config.yaml -i urls.txt
```

---

## Python API

```python
import asyncio
from pdman import Manager

async def main():
    async with Manager(
        max_downloads=4,
        max_concurrent_downloads=8,
        proxy="http://127.0.0.1:7890",
        max_overall_download_limit="20M",
        on_download_complete="echo Downloaded {filename}",
    ) as pdman:
        pdman.append("https://example.com/file.bin", dir_path="/data/downloads")
        await pdman.wait()

asyncio.run(main())
```

---

## 开发与发布

本项目推荐使用 `uv` 管理本地开发环境：

```bash
uv sync
uv pip install -e .
uv pip install pytest build twine
```

测试：

```bash
uv run python -m pytest -q src/pdman/test.py
```

构建发布产物：

```bash
uv run python -m build
uv run python -m twine check dist/*
```

发布前只需要在 `pyproject.toml` 中更新 `[project].version`，CLI 的 `pdman --version` 会从安装包 metadata 自动读取该版本。随后确认 Git tag 与项目版本对应，例如 `0.3.1` 对应 `v0.3.1`。

仓库中的 `.github/workflows/pypi.yml` 会在 GitHub Release 发布后自动构建并发布到 PyPI。

---

## 仓库内容约定

`README.md` 面向用户，说明安装、常用命令和核心能力；`docs/` 面向开发者和高级用户，记录功能细节、配置链路和发布检查事项。

`discuss/` 仅用于本地讨论稿、规划草稿和临时设计内容，不应进入版本控制，也不作为正式文档发布。正式、可维护的内容应整理到 `README.md` 或 `docs/` 后再提交。

`.venv/`、`.vscode/` 配置、`uv.lock*`、构建产物和下载临时目录均不纳入仓库。

---

## License

本项目使用 GPL-3.0-only 许可证，完整文本见 [LICENSE](LICENSE)。
