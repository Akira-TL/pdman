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
| `--connect-timeout SECONDS` | 连接建立超时，默认 30 秒；超时后跳过该 URL |
| `--connect-progress-delay SECONDS` | 连接等待提示延迟，默认 5 秒；超过该时间仍未连通时显示不确定进度条和剩余时间 |
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
connect_timeout: 30
connect_progress_delay: 5
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

## 6. 任务状态、结果汇总与退出码

v0.3.3 引入运行时任务结果模型，用于区分任务是完成、按用户策略跳过，还是失败后继续批处理。

| 状态 | 含义 |
| --- | --- |
| `completed` | 最终文件已经完成下载，并通过当前启用的后处理或校验 |
| `skipped` | 仅表示 `--quit-if-exists` 命中已有目标文件，这是用户显式要求的跳过 |
| `failed` | 任务没有完成下载目标；pdman 会记录原因并继续处理后续任务 |

`failed` 原因包括但不限于：

- HTTP header 阶段返回不可接受状态，例如 403、404、503。
- 连接超时或连接失败。
- 分块下载、合并或文件系统处理失败。
- 启用 `--check-integrity` 后 MD5 不匹配。
- 重试耗尽后的未处理异常。

运行结束后，`Manager` 会输出 completed / skipped / failed 汇总，并在有 skipped 或 failed 时列出对应任务原因。当前最小退出码规则如下：

| 退出码 | 含义 |
| --- | --- |
| `0` | 没有 failed 任务；completed 和 `--quit-if-exists` skipped 都可接受 |
| `1` | 一个或多个任务 failed |
| `130` | 用户中断 |

### 6.1 v0.3.3 手动验证说明

本节记录维护者手动执行的 v0.3.3 验证项，用来补充自动化测试。保留这些用例的目的是明确确认 CLI 退出码传递和 MD5 mismatch 失败语义。

推荐手动执行：

```bash
/usr/bin/git diff --check
uv run python -m pytest -q tests src/pdman/test.py
uv run pdman --version
```

额外关注的测试项：

- CLI 单元测试：确认 `pdman.cli.main()` 返回 `Manager.exit_code`，即存在 failed 时 CLI 返回 `1`。
- MD5 mismatch 集成测试：使用本地 HTTP server 下载正常 payload，但提供不匹配的 MD5 文件，确认任务结果为 `failed`、原因码为 `integrity_mismatch`、最终 `Manager.exit_code == 1`。

---

## 7. Runtime 目录、history 与 current run

v0.4.0 引入 runtime 目录管理，目标是把 payload 临时文件和非 payload 元数据分开：

```text
/tmp/pdman/
  runs/
    <run-id>/
      chunks/
        <task-id>/
      locks/

~/.cache/pdman/
  history.jsonl
  active/
    <run-id>.json
  runs/
    <run-id>.json
  metadata/
```

目录职责：

| 目录 | 内容 |
| --- | --- |
| `/tmp/pdman/runs/<run-id>/chunks/<task-id>/` | 当前 run 的 chunk 文件和 `.pdm` 元数据 |
| `~/.cache/pdman/history.jsonl` | 每个任务结束后追加一行任务结果 |
| `~/.cache/pdman/active/<run-id>.json` | 当前运行中的 run 状态，只在运行中存在 |
| `~/.cache/pdman/runs/<run-id>.json` | run 结束后的最终 summary |
| `~/.cache/pdman/metadata/` | 后续版本预留的非 payload 元数据目录 |

临时目录策略：

| 参数 | 行为 |
| --- | --- |
| `--tmp DIR` | 最高优先级，使用用户指定目录下的 `.pdman.<task-id>`；空间不足时 failed，不自动回退 |
| `--tmp-policy auto` | 默认，优先使用系统临时目录；已知文件大小且空间不足时回退到目标目录 |
| `--tmp-policy system` | 强制使用系统临时目录；空间不足时 failed |
| `--tmp-policy target` | 保留 v0.3.x 行为，在目标目录创建 `.pdman.<task-id>` |
| `--cache-dir DIR` | 覆盖默认 `~/.cache/pdman` |
| `--keep-tmp` | run failed 或 interrupted 时保留 runtime tmp 目录 |

兼容性说明：启用 `--continue` 且目标目录中存在旧版 `.pdman.<task-id>/.pdm` 时，pdman 会优先使用旧目录继续下载，避免破坏 v0.3.x 已存在的续传状态。

v0.4.2 对已知大小任务执行 tmp 空间检查。检查使用文件大小加固定安全余量，避免刚好够用但实际写入失败。未知大小任务仍会使用 system tmp，因为无法提前估算空间需求。

当 tmp 空间不足时，history 会记录明确原因：

| reason_code | 含义 |
| --- | --- |
| `tmp_space_insufficient` | 临时目录空间不足 |
| `tmp_dir_create_failed` | 临时目录创建失败 |

final run metadata 会写入 `tmp_cleanup`：

```json
{
  "tmp_cleanup": {
    "policy": "cleanup_on_finish",
    "kept": false,
    "run_dir": "/tmp/pdman/runs/<run-id>",
    "error": null
  }
}
```

`--keep-tmp` 只用于 debug 和人工恢复。它不是稳定的 resume metadata 接口；严格 resume 仍放在 v0.6.x。

v0.4.x 只提供 runtime 目录和 history/current-run 基础，不提供 queue 命令、daemon 模式、JSON/JSONL 输出或 agent event stream。这些内容放到后续 0.4.x/0.9.x 修订版本。

### 7.1 History 查询命令

v0.4.1 提供只读查询命令，用于查看 `history.jsonl` 和 `runs/<run-id>.json`：

```bash
pdman history
pdman history --last 50
pdman history --failed
pdman history --status completed
pdman history --run-id <run-id>
pdman runs
pdman runs --last 10
pdman run <run-id>
```

所有查询命令都支持 `--cache-dir DIR`，用于读取非默认 cache 目录。查询命令只读 history/run metadata，不会启动下载任务，也不会修改任务状态。

v0.4.1 不提供 history 删除、时间范围过滤、active run 查询或 retry failed。

### 7.2 Queue foundation

v0.4.3 引入本地 JSONL 队列基础：

```text
~/.cache/pdman/queue.jsonl
```

队列命令：

```bash
pdman queue add "https://example.com/file.bin"
pdman queue add -i tasks.yaml
pdman queue add -d /data/downloads --file-name file.bin "https://example.com/file.bin"
pdman queue list
pdman queue list --status pending
pdman queue start
pdman queue start --limit 5
pdman queue retry-failed
pdman queue retry-failed --limit 5
pdman queue validate
pdman queue repair
pdman queue recover
pdman queue remove <queue-id>
pdman queue clear --status completed
pdman queue clear --all
```

queue record schema：

```json
{
  "schema_version": 1,
  "queue_id": "20260701T120000Z-a1b2c3d4",
  "url": "https://example.com/file.bin",
  "file_name": "file.bin",
  "dir_path": "/data/downloads",
  "md5": null,
  "status": "pending",
  "created_at": "...",
  "updated_at": "...",
  "last_run_id": null,
  "last_error": null,
  "attempts": 0
}
```

queue status：

| 状态 | 含义 |
| --- | --- |
| `pending` | 等待执行 |
| `running` | 已被当前 `queue start` 取出执行 |
| `completed` | 最近一次执行成功完成 |
| `skipped` | 最近一次执行被成功跳过 |
| `failed` | 最近一次执行失败 |

`queue start` 会读取 queue 中的任务，真实创建 `Manager` 并执行下载，然后根据 `Manager.results` 更新 queue 状态。测试使用本地 HTTP server 覆盖成功下载和 HTTP 失败路径，不使用 mock Manager。

v0.4.5 增加 `attempts` 字段和 `queue retry-failed`。每次 queue record 被 `queue start` 或 `queue retry-failed` 取出执行时，`attempts += 1`。`retry-failed` 只选取 `failed` 记录，成功后写回 `completed` 且清空 `last_error`，失败后保持 `failed` 并更新 `last_error`。`queue start --status failed` 仍保留，但文档主推 `queue retry-failed`。

v0.4.4 加固内容：

| 项目 | 行为 |
| --- | --- |
| `schema_version` | 当前为 `1`；无版本号的 v0.4.3 legacy record 会按 v1 读取 |
| future schema | `schema_version > 1` 会被跳过，`validate` 会报告 unsupported schema |
| queue lock | 写路径使用 `~/.cache/pdman/queue.lock` |
| POSIX backend | Linux/macOS/BSD 使用 `fcntl.flock` |
| Windows backend | Windows 使用 `msvcrt.locking` |
| fallback backend | 其他平台使用 atomic directory lock |
| `queue validate` | 报告 malformed JSON、缺字段、非法状态、重复 ID、future schema |
| `queue repair` | 丢弃坏行/不可修复记录，补 schema/timestamp/ID，修复非法状态 |
| `queue recover` | 将 stale `running` 恢复为 `pending` |
| `queue remove` | 按 queue_id 删除记录 |
| `queue clear` | 按状态或 `--all` 清理记录 |

限制：v0.4.5 不提供自动 retry scheduler、max-attempts policy、backoff、优先级、daemon、SQLite、网络文件系统强一致锁或 agent event stream。

---

## 8. 内部配置链路

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

## 9. 本地开发环境

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

## 10. 发布前检查

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

## 11. 文档维护原则

- `README.md` 只放面向用户的安装、快速开始、常用命令和项目约定。
- `docs/` 放更完整的功能说明、内部链路、开发与发布检查。
- `discuss/`、临时计划、未整理的 feature plan、个人笔记不进入仓库。
- 新功能合并前，应同步检查 CLI 参数、README、docs、版本号和测试。
