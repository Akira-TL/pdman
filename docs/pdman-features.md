# pdman 功能与开发说明

本文档面向开发者和高级用户，补充说明 `pdman` 的功能边界、参数含义、配置链路、发布检查和仓库维护约定。普通用户优先阅读根目录 `README.md`。

---

## 1. 功能定位

`pdman` 是一个异步多段下载器，核心目标是把常见下载器能力整理为一个轻量、可脚本化、可嵌入 Python 项目的工具。

核心场景包括：

- 大文件多连接下载
- 下载中断后的断点续传
- 批量 URL 下载
- 代理、Cookie、认证、Header 等复杂 HTTP 下载
- 下载限速与并发控制
- 下载完成后的自动回调或后处理
- 在 Python 异步任务中作为下载管理器调用

---

## 2. CLI 参数总览

### 2.1 基础参数

| 参数 | 说明 |
| --- | --- |
| `-v, --version` | 输出版本号后退出 |
| `-l, --log PATH` | 指定日志文件；`-` 表示输出到 stdout |
| `--debug` | 启用 DEBUG 级别日志 |
| `-d, --dir DIR` | 指定下载目录 |
| `-o, --out NAME` | 指定单 URL 下载的输出文件名 |
| `urls...` | 一个或多个待下载 URL |

### 2.2 任务与批量输入

| 参数 | 说明 |
| --- | --- |
| `-i, --input-file FILE` | 从纯文本、JSON 或 YAML 文件读取任务；可重复传入 |
| `-q, --quit` | 目标文件已存在时跳过任务 |
| `--no-auto-file-renaming` | 关闭同名文件自动重命名 |
| `--conf-path PATH` | 从 JSON / YAML 配置文件加载默认参数 |

### 2.3 并发、分块与续传

| 参数 | 说明 |
| --- | --- |
| `-N, --max-downloads INT` | 同时下载的 URL 数量 |
| `-x, --max-concurrent-downloads INT` | 单个 URL 内部最大分块并发数 |
| `-Z, --force-sequential` | 强制顺序下载 |
| `-k, --min-split-size SIZE` | 最小分块大小，支持 `K` / `M` 后缀 |
| `--max-connection-per-server INT` | 单服务器最大连接数；`0` 表示不限制 |
| `-c, --continue` | 启用断点续传 |
| `--tmp DIR` | 指定分块临时文件根目录 |

### 2.4 重试、超时与低速检测

| 参数 | 说明 |
| --- | --- |
| `-r, --retry INT` | 任务失败重试次数 |
| `-W, --retry-wait SECONDS` | 每次重试前等待秒数 |
| `--timeout SECONDS` | HTTP 请求总超时 |
| `--connect-timeout SECONDS` | 连接建立超时 |
| `--chunk-timeout SECONDS` | 分块下载超时 |
| `--chunk-retry-speed SIZE` | 分块速度低于阈值时重启该分块，支持 `K` / `M` 后缀 |

### 2.5 网络、认证与代理

| 参数 | 说明 |
| --- | --- |
| `--http-auth user:pass` | HTTP Basic / Digest 认证 |
| `--cookie-file PATH` | Netscape / Mozilla 格式 Cookie 文件 |
| `--proxy URL` | HTTP / HTTPS 代理地址 |
| `--proxy-auth user:pass` | 代理认证 |
| `--header "Key: Value"` | 自定义 HTTP 请求头；可重复传入 |
| `--referer URL` | 设置 Referer |
| `-ua, --user-agent STRING` | 设置 User-Agent |

### 2.6 限速、校验、SSL 与回调

| 参数 | 说明 |
| --- | --- |
| `--max-download-limit SIZE` | 单任务限速 |
| `--max-overall-download-limit SIZE` | 全局限速 |
| `-V, --check-integrity` | 启用 MD5 完整性校验 |
| `--no-check-certificate` | 关闭 SSL 证书校验 |
| `--ca-certificate PATH` | 使用自定义 CA 证书 |
| `--on-download-complete CMD` | 下载完成后执行 shell 命令 |
| `--summary-interval SECONDS` | 进度刷新间隔 |

---

## 3. 批量任务格式

### 3.1 纯文本

每行一个 URL：

```text
https://example.com/a.iso
https://example.com/b.zip
```

### 3.2 JSON

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

### 3.3 YAML

```yaml
https://example.com/a.iso:
  file_name: linux.iso
  dir_path: /data/downloads
  md5: 0123456789abcdef0123456789abcdef
  log_path: /data/logs/a.log
```

任务字段说明：

| 字段 | 说明 |
| --- | --- |
| `file_name` | 最终输出文件名 |
| `dir_path` | 下载目录 |
| `md5` | 32 位 MD5、本地 MD5 文件路径，或返回 MD5 文本的 URL |
| `log_path` | 当前 URL 的独立日志文件路径 |

---

## 4. 配置文件

`--conf-path` 支持 JSON / YAML。配置文件中的 key 与 CLI 长参数名保持一致，命令行参数优先级高于配置文件。

```yaml
max_downloads: 4
max_concurrent_downloads: 8
retry: 5
retry_wait: 3
timeout: 120
connect_timeout: 20
chunk_timeout: 30
chunk_retry_speed: "100K"
proxy: "http://127.0.0.1:7890"
max_download_limit: "5M"
max_overall_download_limit: "20M"
check_certificate: true
auto_file_renaming: true
summary_interval: 1.0
```

---

## 5. 回调命令

`--on-download-complete` 支持以下占位符：

| 占位符 | 替换内容 |
| --- | --- |
| `{filename}` | 文件名 |
| `{filepath}` | 完整输出路径 |
| `{url}` | 下载 URL |
| `{dir}` | 输出目录 |
| `{size}` | 文件大小，单位为字节 |

示例：

```bash
pdman --on-download-complete "echo Downloaded {filename} to {filepath}" \
  "https://example.com/file.zip"
```

回调命令会通过 shell 执行。不要在不可信任务文件中使用任意回调命令。

---

## 6. 内部配置链路

当前 CLI 到下载执行的大致链路为：

```text
cli.py
  ↓ argparse 解析参数
Manager.__init__
  ↓ 保存全局配置、限速器、日志、任务队列
Manager.add_urls() / Manager.load_input_file()
  ↓ 创建 Downloader
Downloader.parse_config()
  ↓ 解析单任务配置、文件名、目录、MD5、日志路径
Downloader._build_client_session()
  ↓ 构造 aiohttp.ClientSession
Chunk.download()
  ↓ Range 请求、分块写入、限速、低速检测、重试
Downloader.merge()
  ↓ 合并分块、校验、清理临时目录、执行回调
```

其中：

- `Manager` 负责全局任务管理和 URL 队列调度。
- `Downloader` 负责单个 URL 的配置解析、分块规划、合并和完整性校验。
- `Chunk` 负责具体 Range 请求和分块文件写入。
- `RateLimiter` 负责单任务和全局限速。

---

## 7. 本地开发环境

推荐使用 `uv`：

```bash
uv sync
uv pip install -e .
uv pip install pytest build twine
```

运行测试：

```bash
uv run python -m pytest -q src/pdman/test.py
```

构建与检查：

```bash
uv run python -m build
uv run python -m twine check dist/*
```

当前仓库约定：

- `.venv/` 不提交。
- `uv.lock*` 不提交。
- `.vscode/launch.json` 和 `.vscode/settings.json` 不提交。
- `build/`、`dist/`、`*.egg-info/` 不提交。
- `discuss/` 只保留本地草稿和讨论内容，不提交。

---

## 8. 发布前检查

发布前至少完成以下检查：

```bash
git status --short --branch
grep -n 'version =' pyproject.toml
git tag --list --sort=-v:refname | head
uv run python -m pytest -q src/pdman/test.py
uv run pdman --version
uv run python -m build
uv run python -m twine check dist/*
```

需要确认：

- 工作区干净。
- `pyproject.toml` 是唯一版本源，CLI 通过安装包 metadata 读取版本。
- `pdman --version` 输出与 `pyproject.toml` 版本一致。
- 新版本 tag 尚不存在，例如 `v0.3.1`。
- 测试通过。
- `dist/` 中的 sdist 和 wheel 构建成功。
- `twine check` 通过。
- README 和 docs 不包含本地草稿、临时讨论、私有规划内容。

发布命令参考：

```bash
git push origin main
git tag -a v0.3.1 -m "Release v0.3.1"
git push origin v0.3.1
gh release create v0.3.1 \
  --title "v0.3.1" \
  --generate-notes \
  --verify-tag \
  --fail-on-no-commits
```

GitHub Release 发布后，`.github/workflows/pypi.yml` 会触发自动构建并发布 PyPI。

---

## 9. 文档维护原则

- `README.md` 只放面向用户的安装、快速开始、常用命令和项目约定。
- `docs/` 放更完整的功能说明、内部链路、开发与发布检查。
- `discuss/`、临时计划、未整理的 feature plan、个人笔记不进入仓库。
- 新功能合并前，应同步检查 CLI 参数、README、docs、版本号和测试。
