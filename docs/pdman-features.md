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
| `--segment-mode static|dynamic|auto` | 分段模式；默认 `static`，`dynamic` 为显式实验性 range allocator 模式，`auto` 为 v0.5.6 实验性自动选择模式 |
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
segment_mode: static
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

## 5. 分段下载模式

v0.5.0 引入显式分段模式参数：

```bash
pdman --segment-mode static "https://example.com/file.bin"
pdman --segment-mode dynamic -x 4 -k 1M "https://example.com/file.bin"
pdman --segment-mode auto -x 4 -k 1M "https://example.com/file.bin"
```

| 模式 | 行为 |
| --- | --- |
| `static` | 默认行为，保留原有静态 chunk slicing 和中途拆分路径 |
| `dynamic` | 显式实验性动态 range allocator；根据文件大小、worker 数和 `--min-split-size` 生成 ranges，多 worker 反复领取 range，完成后按 offset 合并 |
| `auto` | 实验性 selector；仅在满足 dynamic eligibility 时使用 dynamic，否则回退 static |

`dynamic` / `auto` 使用同一组 eligibility 判断。以下情况会回退到 `static` 路径，并记录稳定的 fallback reason：

- `continue_not_supported`：用户启用了 `--continue`。
- `unknown_file_size`：文件大小未知，或 `Content-Length` 缺失、非法、非正。
- `accept_ranges_not_bytes`：服务端未声明 `Accept-Ranges: bytes`。
- `force_sequential_enabled`：用户启用了 `--force-sequential`。
- `insufficient_workers`：单 URL worker 数不大于 1。
- `file_too_small`：文件小于 `--min-split-size * 2`。

v0.5.8 起，`Accept-Ranges` 判断会按逗号分隔 token 并大小写不敏感匹配 `bytes`，例如 `Bytes`、`BYTES`、`bytes, none` 都视为支持 byte ranges；缺失、空值或不包含 `bytes` 的值仍回退为 `accept_ranges_not_bytes`。

v0.5.1 起，dynamic range size 使用以下策略：

```text
base = file_size // (worker_count * 4)
range_size = max(min_split_size, base)
range_size 向下对齐到 64 KiB 边界
```

这样可以避免大文件因为 `--min-split-size` 太小而生成过多 range，同时仍保留每个 worker 多次领取 range 的空间。`RangeAllocator` 也暴露 `total_ranges`、`pending_count`、`active_count`、`completed_count`、`failed_count`、`retried_count`、`requeue_count`、`split_count`，用于测试和 debug。dynamic 启动时会在 debug 日志中记录 file size、range size、worker 数和 range 数。

v0.5.2 起，dynamic range worker 会记录每个 range 的 `downloaded_bytes`、`last_speed_bps` 和 `last_error`。如果 range 下载失败，或流式下载速度低于 `--chunk-retry-speed`，pdman 会删除该 range 的 partial 文件、修正当前任务进度字节数，并按 `--retry` 重新入队。超过 retry limit 后，整个任务记录为 failed。

v0.5.3 起，`SlowRangeError` 会优先尝试 split remaining：已下载的 partial bytes 会重命名为较短的 completed range，剩余区间会作为 child range 插回 pending 队列继续下载。普通网络错误仍保持 v0.5.2 的删除 partial 后 retry 语义。

v0.5.4 起，dynamic mode 会校验 Range 响应：`206` 必须包含 `Content-Range`，且 start/end 必须匹配请求区间；total 如果不是 `*`，必须等于已知文件大小。`200` 只允许用于 full-file range；partial range 收到 `200`、缺失或不匹配的 `Content-Range`、短 body 都会作为 range failure 处理，并进入已有 retry / failed 流程。

v0.5.5 起，dynamic mode 会在任务 tmp 目录写入 `dynamic-ranges.json` debug metadata draft，记录 schema version、range size、allocator stats、每个 range 的 state、attempts、last_error、downloaded bytes、existing size、expected size、last_speed_bps 和 path。该文件只用于调试和后续 resume 设计参考，不是稳定 resume contract；成功清理 tmp 时会随 tmp 删除，失败且保留 tmp 时可直接查看。

v0.5.7 起，`pdman debug ranges <dynamic-ranges.json>` 可以直接检查该 metadata：

```bash
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json --state failed
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json --json
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json --jsonl
pdman debug ranges --latest
pdman debug ranges --latest --state failed --json
pdman debug ranges --latest --search-root /path/to/tmp --jsonl
```

默认输出 readable 诊断摘要；`--json` 输出包含 `filter`、`stats`、`state_counts`、`source_path` 和过滤后 `ranges` 的结构化 payload；`--jsonl` 每个 range 输出一行 JSON，方便脚本和 agent 管道消费。`--state` 支持 `pending`、`active`、`completed`、`failed`、`unknown`。v0.5.8 起，metadata 可包含向后兼容的 `selector` 诊断对象，记录 `requested_mode`、`selected_mode`、`reason` 和 `fallback_reason`；`pdman debug ranges --json` 会保留该字段，readable 输出也会显示 selector 摘要。metadata 写入改为原子替换，降低多 worker 同时更新 debug JSON 时产生损坏文件的风险。v0.5.9 起，`--latest` 曾从默认系统 tmp root、cache root 以及额外 `--search-root` 中递归查找最新的有效 `dynamic-ranges.json`；v0.7.6 起，新运行 metadata 写入 cache metadata 目录，默认 `--latest` 只搜索 cache root，显式 `--search-root` 是严格搜索边界。该命令只读取 v0.5.5+ 的 dynamic debug metadata，不恢复下载、不修改 metadata 或下载文件，也不保证跨大版本 metadata 兼容。

v0.5.x 的 dynamic mode 仍是实验路径，不包含 dynamic resume、严格 resume metadata v2，也不会作为默认模式启用。

### Strict resume metadata v2

v0.6.0 引入独立的 resume metadata v2 模型，用于后续 static / dynamic recovery。它和 v0.5.x 的 `dynamic-ranges.json` debug metadata 明确分离：debug metadata 只用于诊断，resume metadata 才是未来恢复下载时允许读取的稳定 contract。

resume metadata v2 的基础结构如下：

```json
{
  "schema_version": 2,
  "kind": "resume",
  "mode": "static",
  "url": "https://example.com/file.bin",
  "filename": "file.bin",
  "target_path": "/downloads/file.bin",
  "file_size": 2048,
  "etag": "abc123",
  "last_modified": "Wed, 01 Jan 2025 00:00:00 GMT",
  "created_at": "2026-06-30T00:00:00Z",
  "updated_at": "2026-06-30T00:00:00Z",
  "segments": [
    {
      "index": 0,
      "start": 0,
      "end": 1023,
      "path": "/tmp/pdman/file.bin.0",
      "expected_size": 1024,
      "existing_size": 1024,
      "state": "completed"
    }
  ]
}
```

v0.6.0 只提供 metadata 层能力：`write_resume_metadata`、`load_resume_metadata`、`validate_resume_metadata` 和 `inspect_resume_segments`。校验会拒绝不支持的 `schema_version`、错误的 `kind`、不支持的 `mode`、缺失或非法的 segment、文件大小不匹配、segment layout 不匹配，以及 JSON 或磁盘上的 partial 文件大于 `expected_size` 的情况。`inspect_resume_segments` 会只读检查 partial 路径，缺失文件记为 `existing_size=0` / `pending`，大小等于期望记为 `completed`，介于两者之间记为 `partial`。

v0.6.0 不自动恢复下载，不修复 corrupted partial，不做 URL refresh，不做 HEAD fallback GET，也不把该 metadata 接入 CLI resume 流程。

v0.6.1 将该 contract 接入 static `--continue` 路径：static 下载会在任务 tmp 目录写入 `resume-metadata.json`，记录当前 chunk layout 和 partial size。继续下载时，如果该文件存在，pdman 会优先使用 metadata 内记录的旧 layout 重建 chunk 链，并严格校验 URL、target path、file size、etag、last-modified 和磁盘 partial size。改变 `-x` / `-k` 只影响新任务切块，不会单独触发 resume mismatch。校验失败时会清理旧 tmp 并重新开始，避免错误复用旧 partial。缺失 `resume-metadata.json` 时仍保留 legacy `.pdm` fallback，用于兼容旧 tmp。

v0.6.2 将同一 contract 扩展到 dynamic metadata emission：dynamic mode 会继续写 `dynamic-ranges.json` debug metadata，同时额外写 `resume-metadata.json`，其中 `mode=dynamic`，segments 直接来自当前 `RangeAllocator.ranges`。split 后的 parent / child range 会按真实 runtime layout 记录，不会重新按 range size 推导。`dynamic-ranges.json` 仍服务 readable / JSON / JSONL 诊断；`resume-metadata.json` 只服务未来 recovery contract。v0.6.2 不从 dynamic resume metadata 自动恢复下载，也不尝试修复 corrupted partial。

v0.6.3 为 resume rejection 增加稳定 reason code 和统一可读日志，例如 `Resume rejected [file_size_mismatch]: ...`。这用于让用户和后续 agent 知道为什么拒绝复用 tmp。v0.6.3 不改变 exit code、不新增 JSON/JSONL resume 输出、不自动修复 partial，也不清理 legacy `.pdm` fallback。

v0.6.4 开始收紧 legacy fallback 边界：缺失 `resume-metadata.json` 时仍允许 `.pdm` 兼容恢复，但会输出 warning，提醒这是旧兼容路径。若 v2 metadata 存在但校验拒绝，不会再退回 `.pdm`，而是清理旧 tmp 并重新开始，避免旧 fallback 绕过严格的 URL、target、file size、etag、last-modified 和 partial size 检查。

v0.6.5 将 static resume rejection 提升到用户可见层：`TaskResult`、runtime history 和 human summary 都会记录拒绝原因，summary 中会出现 `Resume:` 小节。该版本只改善可见性，不改变 exit code，不新增专门 JSON/JSONL resume 输出，也不启用 dynamic recovery。

v0.6.6 补齐 history 可见性：runtime history JSONL 稳定写入 `resume_rejection_code` / `resume_rejection_reason` 字段，`pdman history` 和 `pdman run <run_id>` 的 human 输出会显示 completed 任务的 resume rejection。该版本仍不新增独立 resume debug 命令，也不改变 dynamic recovery 未启用的边界。

v0.6.7 新增内部 JSON payload helper：`resume_rejection_payload(...)` 可从 `TaskResult` 或 history record 生成 `{present, code, reason}` 结构。它只暴露最小诊断字段，不输出完整 resume metadata、本地 partial 路径或 dynamic debug ranges；该版本不新增 CLI 参数，也不改变恢复行为。

v0.6.8 为 `pdman history` 增加 `--json` / `--jsonl`，输出 records/count，并在每条 history record 中包含 `resume_rejection` payload。该版本只扩展 history 输出，不扩展 `pdman run`、queue 或 debug ranges，也不暴露完整 resume metadata。

v0.6.9 为 `pdman run <run_id>` 增加 `--json`，输出 run 摘要、tasks 和每个 task 的 `resume_rejection` payload。该版本只扩展 run detail JSON，不新增 `pdman run --jsonl`，不改变 human run detail、queue 或 debug ranges 输出。

v0.6.10 新增 `docs/resume-diagnostics.md`，集中说明 resume diagnostics contract、rejection code、history/run JSON 读取方式，以及 `resume-metadata.json` 和 `dynamic-ranges.json` 的职责边界。该版本只做文档收束，不改变恢复、history、run 或 debug ranges 行为。

v0.6.11 新增 `pdman debug resume --metadata <path>`，用于只读 inspect `resume-metadata.json`，并支持 readable、`--json`、`--jsonl` 输出。该命令不恢复下载、不修改 tmp、不自动发现 metadata，也不读取 `dynamic-ranges.json`。

v0.6.12 为 `pdman debug resume` 增加 `--latest` 和 `--search-root`。`--latest` 曾在 system tmp root、cache root 和额外 search root 中寻找最新有效的 `resume-metadata.json`，但仍保持只读，不读取 `dynamic-ranges.json`，不恢复、修复或迁移 tmp。v0.7.6 起，默认 latest 改为只搜索 cache root，显式 `--search-root` 是严格搜索边界。

v0.6.13 为 `pdman debug resume` 增加 `--state completed|partial|pending|failed`。readable 输出显示 filter 与 filtered 统计，`--json` 输出包含 `filter`、`count`、`filtered_stats` 和匹配 segments，`--jsonl` 只输出匹配 segment。

v0.6.14 收束 `pdman debug resume` contract：增加 helper 级 summary/filter/latest 字段测试、CLI help/error 测试，并明确 `--metadata` 与 `--latest` 互斥。该版本不新增恢复行为。

v0.6.15 新增 `docs/releases/v0.6.md`，汇总 0.6.x resume diagnostics release notes、稳定输出面、non-goals 和 v0.7 过渡边界。该版本只做发布收束，不改变运行行为。

### Network request strategy

v0.7.0 开始，pdman 的 header inspection 使用两阶段探测：

1. 默认使用 `HEAD`，成功状态只接受 `200` 或 `206`。
2. 如果 `HEAD` 返回常见的 HEAD 不兼容状态（`403`、`404`、`405`、`501`），或 HEAD 请求出现连接类错误，自动 fallback 到 GET probe。
3. GET probe 使用 `Range: bytes=0-0`，只用于读取响应头，不替代后续实际下载请求。
4. 如果 GET probe 返回 `206`，pdman 会从 `Content-Range` 解析完整 total，并把内部 `Content-Length` 归一为完整文件大小；这样 dynamic selector、resume metadata 和 summary 都不会把 1-byte probe 误认为真实文件大小。
5. 如果 GET probe 也返回非 `200/206`，任务继续按 header check failure 进入 failed，reason 仍为 `HTTP <status> during header check`。
6. 如果 `HEAD` 返回 retryable 5xx、408、425 或 429，不做 GET fallback，保留原有 retry / failed 语义，避免把临时服务端错误误判成可下载。

这个策略用于兼容禁用或错误实现 HEAD 的服务器，不新增用户控制参数，也不改变后续 static chunk、dynamic range、resume metadata、history 或 debug JSON/JSONL 的输出结构。

v0.7.1 对 probe 边界做了补强：

- HEAD 请求被服务端直接断开连接时，会按 `head_connection_error` 记录内部 fallback reason，并进入 GET probe。
- GET probe 如果被服务端忽略 Range 并返回 `200`，保留该响应的 `Content-Length`，用于已知大小下载。
- GET probe 如果返回 `206` 且 `Content-Range` total 为 `*`，会丢弃 probe 响应自身的 `Content-Length`，按未知文件大小处理，避免把 `Range: bytes=0-0` 的 1-byte probe 误当成完整文件。
- `Downloader.header_probe_method` 与 `Downloader.header_probe_fallback_reason` 目前是内部诊断字段，不进入稳定 JSON/JSONL 输出 contract。

v0.7.2 将 request probe fallback 诊断接入任务结果和历史记录：

- `TaskResult` 新增 `header_probe_method` 和 `header_probe_fallback_reason`。
- runtime history / run task record 会写入这两个原始字段。
- history JSON 和 run JSON 的 task payload 会增加 `header_probe` 对象：`{method, fallback_used, fallback_reason}`。
- human summary、history readable 和 run detail readable 只在发生 fallback 时显示 `Probe: GET fallback=<reason>`。
- queue record 暂不复制该诊断字段，避免扩大 queue schema；失败队列仍以 status、attempts、last_error 为主。

v0.7.2 只扩展诊断面，不新增 CLI 参数，不改变 v0.7.0/v0.7.1 的 HEAD/GET 请求策略，也不改变 resume metadata 或 dynamic recovery 边界。

v0.7.3 固定 `header_probe_fallback_reason` 的 reason code contract：

| reason code | 触发条件 | 行为 |
| --- | --- | --- |
| `head_http_403` | HEAD 返回 403 | fallback 到 GET probe |
| `head_http_404` | HEAD 返回 404 | fallback 到 GET probe |
| `head_http_405` | HEAD 返回 405 | fallback 到 GET probe |
| `head_http_501` | HEAD 返回 501 | fallback 到 GET probe |
| `head_connection_error` | HEAD 请求发生连接类错误，例如服务端直接断连 | fallback 到 GET probe |

明确不 fallback 的状态包括 `408`、`425`、`429`、`500`、`502`、`503`、`504`。这些状态保留为 header check failure / retry 语义，避免把临时服务端故障或限流误判成可下载资源。

`header_probe` JSON 对象结构稳定为：

```json
{
  "method": "GET",
  "fallback_used": true,
  "fallback_reason": "head_http_405"
}
```

未发生 fallback 时，`fallback_used=false`，`fallback_reason=null`；`method` 可能为 `HEAD` 或 `null`，取决于记录来源是否包含 probe method。v0.7.3 不把这些字段写入 queue record，也不新增用户控制开关。

### Network failure taxonomy

v0.7.4 将网络失败诊断拆成三个字段，并写入 `TaskResult`、runtime history 和 run task record：

- `network_error_phase`：失败发生阶段。
- `network_error_kind`：失败类型。
- `network_http_status`：HTTP 失败时的状态码；非 HTTP 失败为 `null`。

history/run JSON payload 会额外提供归一化对象：

```json
{
  "network_error": {
    "present": true,
    "phase": "header_get_probe",
    "kind": "http_status",
    "http_status": 500
  }
}
```

当前稳定 phase：

| phase | 含义 |
| --- | --- |
| `connect` | 连接建立、等待连接或连接类失败 |
| `header_head` | HEAD header inspection 阶段失败 |
| `header_get_probe` | HEAD fallback 之后的 GET probe 阶段失败 |

当前稳定 kind：

| kind | 含义 |
| --- | --- |
| `connection_timeout` | 连接或等待连接超时 |
| `connection_failed` | socket / TCP / aiohttp connection 类失败 |
| `http_status` | HTTP status 不可接受 |

`header_probe_*` 与 `network_error_*` 的职责不同：probe 字段描述是否发生 HEAD→GET fallback；network 字段描述任务失败发生在哪个网络阶段。一个任务可以有 `header_probe_fallback_reason=head_http_405`，同时因为 GET probe 返回 500 而记录 `network_error_phase=header_get_probe`、`network_http_status=500`。

v0.7.4 不改变 retry 策略，不改变 queue schema，也不把这些字段复制进 queue record。queue 仍用于调度和重试候选；完整网络诊断以 history/run 为准。

v0.7.5 将 range 下载阶段纳入同一个 taxonomy，并固定两个 range phase：

| phase | 含义 |
| --- | --- |
| `range_static` | static chunk 下载阶段失败 |
| `range_dynamic` | dynamic range 下载阶段失败 |

v0.7.5 额外稳定两个 range kind：

| kind | 含义 |
| --- | --- |
| `range_incomplete` | range body 不完整，或 static chunk 汇总大小与目标文件大小不一致 |
| `range_response` | Content-Range / range response 校验失败，且不能归类为普通 HTTP status |

range HTTP status 仍使用既有 `http_status` kind，例如 static Range GET 503 会记录：

```json
{
  "network_error": {
    "present": true,
    "phase": "range_static",
    "kind": "http_status",
    "http_status": 503
  }
}
```

dynamic bad `Content-Range` 会记录 `phase=range_dynamic`、`kind=range_response`，并保留 HTTP status，例如 `206`。dynamic short body 会记录 `phase=range_dynamic`、`kind=range_incomplete`。

v0.7.5 还修正 static chunk task 的异常收割：static chunk task 完全未写入且遇到 range HTTP 错误时会向任务层冒泡，避免调度层吞掉异常后继续拆分；static 下载在 merge 前会检查所有 chunk 汇总大小是否等于目标大小，防止 short-body / 重叠 chunk 被错误 merge 成成功文件。

边界：task-level `network_error_*` 只记录最终失败阶段和类别，不复制每个 range 的完整错误列表。dynamic per-range 细节继续由 `dynamic-ranges.json` 和 dynamic resume metadata 承担；queue schema 仍不扩展，queue `last_error` 文案不改变。

### Metadata storage hygiene

v0.7.6 起，新运行不再把 metadata 写入 task tmp 目录：

- 不再创建新的 `.pdm`。
- `resume-metadata.json` 写入 cache metadata 目录。
- `dynamic-ranges.json` 写入 cache metadata 目录。
- task tmp 目录只保留 chunk / partial 文件。

`pdman debug resume --latest` 和 `pdman debug ranges --latest` 默认只搜索 cache root；如果传入 `--cache-dir`，则搜索该 cache root。显式传入 `--search-root` 时，`--search-root` 是严格边界，只搜索用户指定目录，不再混入默认 cache 或 system tmp。

兼容边界：旧 tmp 中已经存在的 `resume-metadata.json` 仍可被 `--continue` 读取；旧 `.pdm` 仍作为最后 fallback 读取。但新运行不会再写这些 tmp metadata。cache metadata 如果校验失败，只会被忽略，不会直接清理 tmp partial；只有 legacy tmp v2 metadata 校验失败时才清理 tmp。

v0.7.7 固定 cache metadata 生命周期和 fallback 优先级：

1. 优先尝试 cache `resume-metadata.json`。
2. cache metadata valid 时使用该 layout。
3. cache metadata stale / invalid 时只忽略，不清理 tmp partial。
4. 继续尝试 legacy tmp `resume-metadata.json`。
5. legacy tmp v2 metadata invalid 时清理 tmp，不 fallback 到 `.pdm`。
6. 最后才尝试 legacy `.pdm`。

如果先遇到 stale cache metadata，随后成功使用 legacy tmp metadata，pdman 会清除此前 stale cache rejection 诊断，避免成功恢复后携带误导性 rejection 字段。`pdman debug resume --latest --cache-dir <dir>` 和 `pdman debug ranges --latest --cache-dir <dir>` 只搜索指定 cache dir；latest discovery 会跳过 mtime 更新但无效的 metadata，选择最新 valid metadata。

cache metadata 当前 layout：

```text
cache_root/
  metadata/
    <url-hash>/
      resume-metadata.json
      dynamic-ranges.json
```

`<url-hash>` 当前由 URL hash 前缀生成。该目录结构是 cache layout，不是 database schema；后续 records/database 查询层应单独版本规划。

v0.7.8 为 `debug --latest` readable UX 增加 latest search diagnostics。成功找到 metadata 时，readable 输出会先显示：

```text
Latest search:
  root: /path/to/cache
  valid: 1
  skipped_invalid: 2
```

未找到 metadata 时，会显示实际搜索过的 roots：

```text
No resume metadata found.
Searched:
  /path/to/cache
```

`debug ranges --latest` 使用同样格式。该版本不改变 `--json` / `--jsonl` payload，不新增命令，不接入 history/database 查询层。

v0.7.9 为 diagnostics helpers 和 legacy `find_latest_*` helpers 增加直接测试，固定 `roots`、`valid`、`skipped_invalid` 与 `selected_path` 行为。自动化脚本仍应优先使用 `--json` / `--jsonl`；latest diagnostics 在 v0.7.x 中保持 readable-only。

v0.7.10 新增 `docs/releases/v0.7.md`，汇总 0.7.x request probe、network taxonomy、range taxonomy、metadata hygiene、debug latest diagnostics、stable surfaces 和 non-goals。该版本只做 release readiness、文档审计和 readable-only smoke tests，不新增运行能力。

v0.7.11 修复下载进度生命周期：同一个 Downloader 在 static / dynamic 下载、task-level retry 和 merge 期间只复用一条 Rich progress task，终态时停止该 task。这个 hotfix 用于避免 retry 后出现多条同名 `Downloading ...` 进度、旧进度 elapsed 继续增加以及重复 completed download 日志。该版本不改变 retry/backoff、分块调度、request probe、queue schema、history/run JSON 或 metadata contract。

v0.7.12 发布 v0.7.11 tag 之后累积的 hotfix：static chunk 写入前会重建缺失父目录，static range 的连接类错误保持在 chunk-local retry 内，已完成 chunk 不再重新调度，static resume metadata 在关键 retry 边界刷新，控制台日志不再额外插入空行。该版本只做 0.7.x 可靠性与发布对齐修复，不引入动态 chunk 分配、自适应并发、queue schema、history/run JSON 或 metadata contract 变化。

---

## 6. 回调命令

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

## 7. 任务状态、结果汇总与退出码

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

### 7.1 v0.3.3 手动验证说明

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

## 8. Runtime 目录、history 与 current run

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

### 8.1 History 查询命令

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

### 8.2 Records query foundation

v0.8.0 新增只读 records query entrypoint：

```bash
pdman records list
pdman records list --last 50
pdman records list --limit 20
pdman records list --status failed
pdman records list --url https://example.com/file.bin
pdman records list --target /downloads/file.bin
pdman records list --run-id <run-id>
pdman records list --json
pdman records list --jsonl
pdman records list --cache-dir /path/to/cache --json
pdman records metadata --url https://example.com/file.bin
pdman records metadata --target /downloads/file.bin
pdman records metadata --run-id <run-id>
pdman records metadata --url https://example.com/file.bin --json
pdman records metadata --run-id <run-id> --jsonl
pdman records show --run-id <run-id> --task-id <task-id>
pdman records show --run-id <run-id> --task-id <task-id> --json
pdman records doctor
pdman records doctor --json
pdman records doctor --jsonl
pdman records schema
pdman records schema --surface show --json
```

`records list` 当前只读取 runtime history，不读取完整 `resume-metadata.json` 或 `dynamic-ranges.json`，也不会启动下载任务、修改 queue、恢复下载、清理 tmp、迁移旧历史或创建 database/index。它的定位是 agent-oriented view：在不要求 agent grep cache 文件的前提下，提供稳定的最近 task record summary。

`records list --json` 输出：

```json
{
  "records": [
    {
      "run_id": "...",
      "task_id": "...",
      "url": "...",
      "filename": "...",
      "target_path": null,
      "status": "completed",
      "file_size": 123456,
      "created_at": "...",
      "completed_at": "...",
      "resume_rejection": {
        "present": false,
        "code": null,
        "reason": null
      },
      "header_probe": {
        "method": "HEAD",
        "fallback_used": false,
        "fallback_reason": null
      },
      "network_error": {
        "present": false,
        "phase": null,
        "kind": null,
        "http_status": null
      }
    }
  ],
  "count": 1
}
```

`--jsonl` 每行输出一个同形 record。readable 输出允许面向人类阅读优化；脚本和 agent 应使用 `--json` 或 `--jsonl`。

v0.8.1 为 `records list` 增加基础过滤 contract：

| 参数 | 行为 |
| --- | --- |
| `--status completed|skipped|failed` | 精确匹配 task status。 |
| `--url URL` | 精确匹配 history record 中的 `url`。 |
| `--target PATH` | 精确匹配 history record 中的 `target_path` 或兼容字段 `filepath`，不做绝对/相对路径推断。 |
| `--run-id RUN_ID` | 精确匹配 `run_id`。 |
| `--limit N` | 过滤完成后保留最近 N 条；`0` 表示不限制数量。 |

`--last` 作为 v0.8.0 的兼容参数保留，语义同 `--limit`；如果二者同时出现，以 `--limit` 为准。v0.8.1 不支持 fuzzy search、regex、contains、URL normalization、path normalization、时间范围查询或 SQL-like query。

v0.8.2 新增 metadata locator foundation：

```bash
pdman records metadata --url https://example.com/file.bin
pdman records metadata --target /downloads/file.bin
pdman records metadata --run-id <run-id>
pdman records metadata --url https://example.com/file.bin --json
pdman records metadata --run-id <run-id> --jsonl
```

`records metadata` 查询参数互斥且必须提供一个。`--url` 可直接根据当前 cache layout 推导 metadata 路径；如果 history 中也有该 URL，会为每个匹配 record 返回一个 match。`--target` 和 `--run-id` 先通过 history records 精确匹配，再根据匹配 record 的 URL 推导 locator。record 缺失 URL 时不会生成 locator。

JSON 输出：

```json
{
  "query": {
    "url": "https://example.com/file.bin",
    "target_path": null,
    "run_id": null
  },
  "matches": [
    {
      "run_id": "...",
      "task_id": "...",
      "url": "https://example.com/file.bin",
      "target_path": "/downloads/file.bin",
      "metadata": {
        "resume": {
          "path": ".../resume-metadata.json",
          "exists": true,
          "source": "cache"
        },
        "dynamic_ranges": {
          "path": ".../dynamic-ranges.json",
          "exists": false,
          "source": "cache"
        }
      }
    }
  ],
  "count": 1
}
```

`--jsonl` 每行输出一个 match。locator 只报告路径是否存在，不读取、validate 或嵌入完整 `resume-metadata.json` / `dynamic-ranges.json` 内容。当前 locator 使用 `cache_root/metadata/<url-hash>/` 推导路径；这是 cache layout，不是 database schema，也不是永久 ID contract。

v0.8.3 新增 single record inspection：

```bash
pdman records show --run-id <run-id> --task-id <task-id>
pdman records show --run-id <run-id> --task-id <task-id> --json
```

`records show` 通过 `run_id + task_id` 精确定位一个 history task record，输出基础 task summary、compact error、`resume_rejection`、`header_probe`、`network_error`、metadata locator 和 `suggested_commands`。JSON 输出形态：

```json
{
  "run_id": "...",
  "task_id": "...",
  "url": "...",
  "filename": "...",
  "target_path": "...",
  "status": "failed",
  "file_size": 123,
  "created_at": "...",
  "completed_at": "...",
  "resume_rejection": {},
  "header_probe": {},
  "network_error": {},
  "error": {
    "reason": "...",
    "reason_code": "...",
    "error": "..."
  },
  "metadata": {
    "resume": {
      "path": ".../resume-metadata.json",
      "exists": true,
      "source": "cache"
    },
    "dynamic_ranges": {
      "path": ".../dynamic-ranges.json",
      "exists": false,
      "source": "cache"
    }
  },
  "suggested_debug": [
    {
      "kind": "resume_metadata",
      "metadata_key": "resume",
      "metadata_path": ".../resume-metadata.json",
      "source": "cache",
      "reason": "metadata_exists",
      "argv": ["pdman", "debug", "resume", "--metadata", ".../resume-metadata.json"],
      "command": "pdman debug resume --metadata .../resume-metadata.json"
    }
  ],
  "suggested_commands": [
    "pdman debug resume --metadata ..."
  ]
}
```

v0.8.4 将 debug bridge 明确为结构化建议而非执行入口。`suggested_debug` 每项包含 `kind`、`metadata_key`、`metadata_path`、`source`、`reason`、`argv` 和 shell-quoted `command`；agent 优先使用 `argv`，人类可直接复制 `command`。`suggested_commands` 作为兼容字段保留，仍只在对应 metadata 文件真实存在时生成。`records show` 不读取完整 metadata，不执行 debug 命令，不恢复或修复下载，也不改变 history/run/debug 旧 contract。

v0.8.5 新增 records schema contract：

```bash
pdman records schema
pdman records schema --surface list --json
pdman records schema --surface metadata --json
pdman records schema --surface show --json
```

`records schema` 输出 records surface 的机器可读 contract，包括 `list`、`metadata`、`show` 的 selector、输出格式、共享 payload 和 non-goals。默认 `--surface all` 输出全部 surface；`--surface list|metadata|show` 只输出单个 surface。readable 输出面向人工快速确认；`--json` 用于脚本和 agent。

v0.8.6 稳定 records diagnostics 边界：

- `metadata_locator.resume` 与 `metadata_locator.dynamic_ranges` 固定包含 `path`、`exists`、`source`、`status`、`reason`。
- metadata 文件存在时 `status=available`，不存在时 `status=missing reason=file_missing`。
- record 缺失 URL 时，`records show` 使用 `status=unavailable reason=url_missing` 的固定 locator 结构，而不是返回空对象。
- `records metadata` 对匹配到但缺失 URL 的 record 输出 `skipped` 和 `skipped_count`。
- `records show --json` 找不到 task 时输出结构化 `{error: {code, message, run_id, task_id}}`，readable 输出保持原来的简短错误文本。

v0.8.7 新增 records doctor：

```bash
pdman records doctor
pdman records doctor --json
pdman records doctor --jsonl
pdman records doctor --limit 100 --json
pdman records doctor --fail-on warning
pdman records doctor --severity warning --code invalid_status --jsonl
```

`records doctor` 只读 history records，输出 `records_checked`、`status_counts`、`metadata_state_counts` 和结构化 `issues`。当前 issue code 包含 `invalid_status`、`run_id_missing`、`task_id_missing`、`url_missing`。它只定位 records 层健康状况，不修复历史、不迁移 schema、不执行 debug、不读取完整 metadata。

v0.8.8 起，`records doctor` 支持 exit policy：`--fail-on never|warning|error`。默认 `never` 永远按检查命令成功返回 0；`warning` 在 doctor 状态为 warning 或 error 时返回 1；`error` 只在状态为 error 时返回 1。该选项只影响 CLI exit code，不改变 JSON payload。

v0.8.9 起，`records doctor` 支持 repeatable issue filters：`--severity info|warning|error` 和 `--code ISSUE_CODE`。过滤会影响输出的 `issues`、`issue_count` 和 doctor `status`，也会影响 `--fail-on` 判断；`total_issue_count` 保留过滤前的问题总数，便于 agent 判断是否只是被过滤隐藏。

v0.8.10 起，`records doctor` 的 issue payload 增加 `impact` 和 `suggested_action`。这两个字段只提供诊断解释与下一步建议，不会自动修改 history，不会修复 metadata，不会触发 queue 或 debug command。

v0.8.11 起，`records doctor --json` 增加 `issue_groups`。它基于过滤后的 issues 按 code 聚合，输出 `code`、`severity`、`count`、`impact`、`suggested_action` 和最多 3 条 `sample_records`。readable 输出会先显示分组摘要，JSONL 继续保持逐 issue 输出。

v0.8.12 起，doctor contract 提供稳定示例 payload：`ok`、`warning_grouped`、`filtered_warning`。示例由纯内存 helper 生成，只用于测试、文档和 agent contract 对照，不读取 cache，不启动下载。精简 JSON 示例：

```json
{
  "schema_version": 1,
  "status": "warning",
  "records_checked": 2,
  "issue_count": 1,
  "total_issue_count": 3,
  "filters": {"severities": ["warning"], "codes": ["invalid_status"]},
  "issue_groups": [{"code": "invalid_status", "severity": "warning", "count": 1}],
  "issues": [{"code": "invalid_status", "impact": "...", "suggested_action": "..."}]
}
```

v0.8.13 起，doctor 输出模式边界固定：`--json` 输出完整 doctor report；`--jsonl` 只输出逐 issue stream，不包含 `issue_groups`、`status_counts`、`metadata_state_counts` 等 summary 字段；readable 输出顺序固定为 summary、status counts、metadata counts、issue groups、issues。

Records 与 history/run 的边界：

| Surface | 定位 |
| --- | --- |
| `pdman history` | 原始 runtime history 查询视角，保留既有 history contract。 |
| `pdman run <run_id>` | 单个 run 的 summary + task 详情视角。 |
| `pdman records list` | 面向 agent 的 compact task record summary，聚合最小关键字段和诊断 payload。 |

v0.8.13 明确不提供：database/index engine、完整 metadata 内容嵌入、旧历史迁移、dynamic recovery、metadata validation、metadata repair、自动执行 debug bridge、自动 doctor repair 或 queue schema 变更。这些能力按 v0.8 后续小版本或 v0.9 单独规划。

### 8.3 Queue foundation

v0.4.3 引入本地 JSONL 队列基础：

```text
~/.cache/pdman/queue.jsonl
```

队列命令：

```bash
pdman queue add "https://example.com/file.bin"
pdman queue add --json "https://example.com/file.bin"
pdman queue add -i tasks.yaml
pdman queue add -d /data/downloads --file-name file.bin "https://example.com/file.bin"
pdman queue list
pdman queue list --last 0
pdman queue list --status pending
pdman queue list --status failed --attempts-ge 3
pdman queue list --json
pdman queue list --jsonl
pdman queue start
pdman queue start --limit 5
pdman queue retry-failed
pdman queue retry-failed --limit 5
pdman queue retry-failed --dry-run
pdman queue retry-failed --dry-run --json
pdman queue retry-failed --dry-run --jsonl
pdman queue retry-failed --max-attempts 3
pdman queue retry-failed --error-contains "HTTP 503"
pdman queue validate
pdman queue validate --json
pdman queue repair
pdman queue repair --json
pdman queue recover
pdman queue recover --json
pdman queue remove <queue-id>
pdman queue remove <queue-id> --json
pdman queue clear --status completed
pdman queue clear --status completed --json
pdman queue clear --all
pdman queue clear --all --json
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
  "last_status_reason": null,
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

v0.4.7 增加 queue structured output 基础，目标是让人、脚本和后续 agent 能稳定读取 queue 查询结果，但不提前引入全局输出协议或 event stream。

| 命令 | 输出契约 |
| --- | --- |
| `queue list --json` | 输出 `{records, count}`；`records` 是 queue record 数组 |
| `queue list --jsonl` | 每行一个 queue record，适合管道和 `jq` |
| `queue retry-failed --dry-run --json` | 输出 `{candidates, count, dry_run}`；不修改 queue |
| `queue retry-failed --dry-run --jsonl` | 每行一个 retry candidate；不修改 queue |
| `queue validate --json` | 输出 `{ok, valid, malformed, invalid, duplicate_ids, unsupported_schema, issues}` |

v0.4.8 继续补齐 queue 维护命令的 JSON 输出，但仍限制在不会启动真实下载的 queue 文件操作范围内。

| 命令 | 输出契约 |
| --- | --- |
| `queue add --json` | 输出 `{added, records, count}` |
| `queue repair --json` | 输出 `{kept, dropped_malformed, dropped_invalid, dropped_unsupported_schema, fixed}` |
| `queue recover --json` | 输出 `{recovered}` |
| `queue remove --json` | 输出 `{requested, removed}` |
| `queue clear --json` | 输出 `{cleared, status, all}` |
| `queue list --last 0` | 不限制数量，返回匹配的全部 records；可和 `--json/--jsonl` 搭配 |

限制：`retry-failed --json/--jsonl` 只支持和 `--dry-run` 搭配；实际执行下载、普通 download summary、history/runs 查询仍保持人类文本输出。完整 agent event stream 仍属于后续 v0.9.x 范围。

v0.4.6 增加 retry policy 人工控制边界：

| 参数/字段 | 行为 |
| --- | --- |
| `last_status_reason` | 保存最近一次 completed/skipped/failed 的原因，不污染 `last_error` |
| `queue list --attempts-ge N` | 查询 attempts >= N 的记录 |
| `queue list --attempts-lt N` | 查询 attempts < N 的记录 |
| `retry-failed --dry-run` | 只预览候选，不修改 queue，不递增 attempts，不创建 Manager |
| `retry-failed --max-attempts N` | 只重试 attempts < N 的 failed records |
| `retry-failed --error-contains TEXT` | 只重试 `last_error` 包含 TEXT 的 failed records，大小写不敏感 |

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

限制：v0.4.8 不提供自动 retry scheduler、backoff、per-record retry policy、优先级、daemon、SQLite、网络文件系统强一致锁、全局 JSON/JSONL 输出协议或 agent event stream。

---

## 9. 内部配置链路

当前 CLI 到下载执行的大致链路为：

```text
cli.py
  ↓ argparse 解析参数
Manager.__init__
  ↓ 保存全局配置、限速器、日志、任务队列
Manager.add_urls() / Manager.load_input_file()
  ↓ 创建 Downloader
Downloader.parse_config()
  ↓ 解析单任务配置、HEAD/GET probe、文件名、目录、MD5、日志路径
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

## 10. 本地开发环境

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

## 11. 发布前检查

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

## 12. 文档维护原则

- `README.md` 只放面向用户的安装、快速开始、常用命令和项目约定。
- `docs/` 放更完整的功能说明、内部链路、开发与发布检查。
- `discuss/`、临时计划、未整理的 feature plan、个人笔记不进入仓库。
- 新功能合并前，应同步检查 CLI 参数、README、docs、版本号和测试。
