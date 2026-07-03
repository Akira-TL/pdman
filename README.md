# pdman — Python Parallel Download Manager

`pdman` 是一个使用 Python 实现的异步多段下载器，目标是提供接近 IDM / aria2c 使用体验的命令行下载工具，同时保留可嵌入 Python 项目的 API。

它适合处理大文件、批量 URL、弱网络续传、代理下载、限速下载、带 Cookie / Header / 认证的 HTTP 资源下载等场景。

---

## 主要特性

- **多连接分块下载**：通过 HTTP Range 请求将同一 URL 拆分为多个分块并发下载。
- **断点续传**：使用 `.pdman.<sha>` 临时目录保存分块和元信息，可通过 `--continue` 恢复下载。
- **动态分块调度**：默认 static 模式保留原有中途拆分能力；v0.5.0 起可用 `--segment-mode dynamic` 启用实验性 range allocator，v0.5.6 起可用 `--segment-mode auto` 试验自动选择。
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

v0.8.17 起，YAML 任务文件也支持 schema v2，用于批量数据集任务的 defaults 和 groups：

```yaml
version: 2

defaults:
  dir_path: /data/downloads
  retry: 5

groups:
  nt-db:
    dir_path: /data/blast/nt
    tasks:
      - url: https://example.com/nt.171.tar.gz
        file_name: nt.171.tar.gz
        md5: https://example.com/nt.171.tar.gz.md5
```

执行：

```bash
pdman -i tasks.json
pdman -i tasks.yaml
pdman -i tasks.yaml --list-groups
pdman -i tasks.yaml --group nt-db --dry-run
pdman -i tasks.yaml --group nt-db
```

schema v2 的 `file_name`、`dir_path`、`md5`、`log_path` 会进入实际 task；其他字段会先保存在解析后的 `options` 中，当前版本不把它们映射成 per-task 下载行为。v0.8.19 起，可用 `pdman input schema` 或 `pdman input schema --json` 查看输入格式和 YAML schema v2 的稳定 contract。

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

v0.5.0 开始提供实验性的动态分段模式。默认仍是兼容旧行为的 static 模式；可以显式启用 dynamic，也可以用 v0.5.6 引入的 auto selector 试验自动选择：

```bash
pdman --segment-mode dynamic -x 4 -k 1M "https://example.com/file.bin"
pdman --segment-mode auto -x 4 -k 1M "https://example.com/file.bin"
```

`--segment-mode dynamic` 会使用 range allocator 生成可领取的 ranges，多 worker 持续领取 range 并按 offset 合并。`--segment-mode auto` 会在文件大小已知、服务端声明 `Accept-Ranges: bytes`、未启用 `--continue`、未强制顺序下载、worker 数大于 1 且文件足够大时选择 dynamic，否则回退 static；默认仍是 static。v0.5.1 起 dynamic range size 会按文件大小、worker 数和 `--min-split-size` 计算：至少不小于 `--min-split-size`，目标约为每个 worker 4 个 ranges，并按 64 KiB 边界对齐，避免大文件产生过多小 range。v0.5.2 起，dynamic range 下载失败或低于 `--chunk-retry-speed` 时会删除该 range 的 partial 文件并重新入队，超过 `--retry` 后任务失败。v0.5.3 起，低速 range 会优先把已下载 partial 保留为 completed range，并把剩余区间拆成 child range 继续下载；普通网络失败仍保持删除 partial 后 retry。v0.5.4 起，dynamic mode 会校验 206 响应的 `Content-Range` start/end/total，partial range 收到 200、缺失或不匹配的 `Content-Range`、短 body 都会作为 range failure 处理。v0.5.5 起，dynamic mode 会在任务 tmp 目录写入 `dynamic-ranges.json` debug metadata draft，记录 range states、attempts、errors、speed 和 allocator stats；v0.5.7 起可用 `pdman debug ranges` 读取该文件并输出 readable / JSON / JSONL 诊断结果。v0.5.8 起，auto selector 对 `Accept-Ranges` 做大小写不敏感的逗号 token 判断，非法或非正 `Content-Length` 会按未知大小回退 static；dynamic metadata 会附带 selector 诊断字段，并使用原子替换写入，降低并发 worker 写坏 JSON 的风险。该 metadata 只用于调试，不是 resume contract。v0.5.x 暂不做 dynamic resume 或默认启用 dynamic。

```bash
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json --state failed
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json --json
pdman debug ranges /tmp/pdman-xxx/dynamic-ranges.json --jsonl
pdman debug ranges --latest
pdman debug ranges --latest --state failed --json
pdman debug ranges --latest --search-root /path/to/tmp --jsonl
```

`pdman debug ranges` 只读取 v0.5.5+ 的 dynamic debug metadata；不会恢复下载，不会修改 metadata 或下载文件，也不承诺跨大版本 metadata 兼容。v0.5.9 起，`--latest` 曾从默认系统 tmp root、cache root 以及额外 `--search-root` 中递归查找最新的有效 `dynamic-ranges.json`；v0.7.6 起，新运行 metadata 写入 cache metadata 目录，默认 `--latest` 只搜索 cache root，显式 `--search-root` 是严格搜索边界。

v0.6.0 开始新增独立的 resume metadata v2 模型，用于后续严格恢复下载。它和 `dynamic-ranges.json` debug metadata 分离，字段包含 `schema_version=2`、`kind=resume`、`mode=static|dynamic`、URL、目标文件信息、file size、etag / last modified 以及 segments。当前版本只提供 metadata 读写、schema / layout 校验和 partial 文件大小检查；不会自动恢复下载，也不会修复 corrupted partial。static resume 接入和 dynamic recovery 会放到后续 v0.6.x。

v0.6.1 起，static 下载路径会在任务 tmp 目录写入 `resume-metadata.json`。使用 `--continue` 时，pdman 会优先读取该 metadata，并在复用 partial 前校验 URL、target path、file size、etag / last-modified 和磁盘 partial size；校验失败会丢弃旧 tmp，避免错误复用 stale partial。恢复时沿用 metadata 记录的旧 segment layout，因此改变 `-x` / `-k` 不会单独触发 mismatch。dynamic recovery 仍未启用。

v0.6.2 起，dynamic 下载也会写入 `resume-metadata.json`，并继续同时写入 `dynamic-ranges.json`。二者职责不同：`dynamic-ranges.json` 面向 debug inspection，包含 allocator stats、attempts、last_error、selector 等诊断字段；`resume-metadata.json` 面向未来 recovery contract，只记录 URL、目标文件身份、file size、etag / last-modified 和可恢复 segments。v0.6.2 只负责 emission 和校验，不会从 dynamic resume metadata 自动恢复下载。

v0.6.3 起，resume metadata 被拒绝时会带稳定 reason code，并使用类似 `Resume rejected [file_size_mismatch]: ...` 的可读日志。当前仅改善诊断表达，不改变 exit code，不新增 JSON/JSONL resume diagnostics，也不自动修复或清理 legacy `.pdm` 行为。

v0.6.4 起，缺失 `resume-metadata.json` 但存在旧 `.pdm` 时，pdman 仍会兼容恢复，但会输出 legacy fallback warning。若 `resume-metadata.json` 存在但被拒绝，pdman 会清理旧 tmp 并重新开始，不会退回 `.pdm`，避免绕过 v2 的严格拒绝原因。

v0.6.5 起，static resume rejection 会进入 `TaskResult`、runtime history 和 human summary 的 `Resume:` 小节，方便用户看到本次任务为什么没有复用旧 tmp。该可见性不改变 exit code，不新增专门的 JSON/JSONL resume diagnostics，也不启用 dynamic recovery。

v0.6.6 起，runtime history JSONL 稳定包含 `resume_rejection_code` / `resume_rejection_reason` 字段，`pdman history` 和 `pdman run <run_id>` 的 human 输出也会显示 completed 任务的 resume rejection。该版本只是补齐 history 可见性，不新增独立 resume debug 命令。

v0.6.7 起，内部 JSON payload helper `resume_rejection_payload(...)` 会把 `TaskResult` 或 history record 归一化为 `{present, code, reason}`。这是后续 JSON/JSONL 或 agent 输出的基础，不会暴露完整 resume metadata，不新增 CLI 参数，也不改变下载恢复行为。

v0.6.8 起，`pdman history` 支持 `--json` / `--jsonl`，输出 history records、count，以及每条记录的 `resume_rejection` 诊断 payload。该版本只扩展 history 输出，不扩展 `pdman run`、queue 或 debug ranges，也不暴露完整 resume metadata。

v0.6.9 起，`pdman run <run_id>` 支持 `--json`，输出 run 摘要、tasks 和每个 task 的 `resume_rejection` payload。该版本只扩展 run detail JSON，不新增 `pdman run --jsonl`，不改变 human run detail、queue 或 debug ranges。

v0.6.10 起，`docs/resume-diagnostics.md` 集中整理 resume diagnostics contract：包括 `resume-metadata.json`、`dynamic-ranges.json`、legacy `.pdm`、history JSON、run JSON 的职责边界，rejection code 含义，以及 agent/script 推荐读取路径。该版本只补文档，不改变运行行为。

v0.6.11 起，`pdman debug resume --metadata <path>` 可只读 inspect `resume-metadata.json`，并支持 readable、`--json`、`--jsonl` 输出。该命令不恢复下载、不修改 tmp、不自动发现 metadata，也不读取 `dynamic-ranges.json`。

v0.6.12 起，`pdman debug resume --latest` 曾在 system tmp root、cache root 和额外 `--search-root` 中寻找最新有效的 `resume-metadata.json`。该发现逻辑仍然只读，不读取 `dynamic-ranges.json`，也不恢复、修复或迁移 tmp。v0.7.6 起，默认 latest 改为只搜索 cache root，显式 `--search-root` 是严格搜索边界。

v0.6.13 起，`pdman debug resume` 支持 `--state completed|partial|pending|failed`。readable 输出会显示 filter 和 filtered 统计，`--json` 输出包含 `filter`、`count`、`filtered_stats` 和匹配 segments，`--jsonl` 只输出匹配 segment。

v0.6.14 起，`pdman debug resume` 的 inspect contract 增加直接 helper 测试和 CLI help/error 测试，固定 summary/filter/latest 字段边界；`--metadata` 与 `--latest` 明确互斥。该版本是 0.6 收束，不新增恢复行为。

v0.6.15 起，`docs/releases/v0.6.md` 汇总 0.6.x release notes、稳定 diagnostics 输出面、明确 non-goals，并给出进入 v0.7 的边界。该版本只做发布收束，不改变代码行为。

v0.7.0 起，header 探测策略改为默认先发 `HEAD`；如果 `HEAD` 返回常见的 HEAD 不兼容状态（`403/404/405/501`），或 HEAD 请求发生连接类错误，pdman 会自动 fallback 到轻量 `GET` probe。GET probe 会带 `Range: bytes=0-0`，用于读取 `Content-Disposition`、`Content-Length`、`Accept-Ranges` 等元数据；当服务端返回 `206` 时，会从 `Content-Range` 中还原完整文件大小，避免 dynamic selector 把 probe body 大小误判为真实文件大小。该行为不新增 CLI 参数，不改变后续实际下载的 static/dynamic Range 请求；如果 GET probe 仍失败，或 HEAD 返回 retryable 5xx/限流状态，任务继续按 header check failure 记录为 failed。

v0.7.1 起，请求探测边界进一步收紧：HEAD 直接断连会进入 GET probe fallback；如果 GET probe 被服务端忽略 Range 并返回 `200`，pdman 使用该响应的完整 `Content-Length`；如果 GET probe 返回 `206` 但 `Content-Range` total 为 `*`，pdman 会忽略 probe 自身的 `Content-Length: 1` 并按未知文件大小处理，避免只下载 1 byte。

v0.7.2 起，HEAD→GET fallback 会进入任务诊断：`TaskResult`、runtime history 和 history/run JSON 输出会包含 `header_probe_method`、`header_probe_fallback_reason`，JSON payload 还会提供归一化的 `header_probe` 对象。human summary、history 和 run detail 会在发生 fallback 时显示 `Probe: GET fallback=<reason>`。该版本只暴露诊断，不新增 CLI 参数，也不改变请求策略。

v0.7.3 起，probe fallback reason code 被固定为公开诊断 contract：`head_http_403`、`head_http_404`、`head_http_405`、`head_http_501` 和 `head_connection_error`。HTTP reason 只会在这些允许 fallback 的 HEAD 状态中出现；`408/425/429/5xx` 仍然不会 fallback，会保留 retry / failed 语义。

v0.7.4 起，网络失败会写入结构化诊断字段：`network_error_phase`、`network_error_kind`、`network_http_status`，history/run JSON 还会提供归一化 `network_error` 对象。当前稳定阶段包括 `connect`、`header_head`、`header_get_probe`；稳定类型包括 `connection_timeout`、`connection_failed`、`http_status`。该诊断用于区分“HEAD 不 fallback 的 HTTP 失败”和“GET probe fallback 后的 HTTP 失败”，不改变 retry、queue 或下载行为。

v0.7.5 起，range 下载阶段也进入 network taxonomy：static chunk 失败记录为 `network_error_phase=range_static`，dynamic range 失败记录为 `network_error_phase=range_dynamic`。range HTTP status 继续使用 `network_error_kind=http_status`，range body 不完整使用 `range_incomplete`，Content-Range / range response 校验失败使用 `range_response`。该版本不改变 queue schema，也不把每个 range 的详细错误复制进 task result；dynamic per-range 细节仍以 `dynamic-ranges.json` 和 resume metadata 为准。

v0.7.6 起，新运行不再把 `.pdm`、`resume-metadata.json` 或 `dynamic-ranges.json` 写入 task tmp 目录；tmp 只保留 chunk / partial 文件。resume 与 dynamic range metadata 改写入 cache metadata 目录，`pdman debug resume --latest` 和 `pdman debug ranges --latest` 默认只搜索 cache root。显式传入 `--search-root` 时，该 root 是严格搜索边界，不再混入默认 tmp/cache root。旧 tmp 中已有的 `resume-metadata.json` 和 `.pdm` 仍可作为兼容 fallback 读取。

v0.7.7 起，cache metadata 生命周期边界进一步固定：stale cache resume metadata 校验失败时只会被忽略，不会清理 tmp partial，也不会阻断旧 tmp `resume-metadata.json` / `.pdm` fallback；如果随后成功使用 legacy tmp metadata，会清除此前 stale cache rejection 诊断。`debug --latest --cache-dir <dir>` 只搜索指定 cache dir，latest 会跳过更新但无效的 metadata，选择最新 valid metadata。cache metadata 当前位于 `cache_root/metadata/<url-hash>/`，这是 cache layout，不是数据库 schema。

v0.7.8 起，`pdman debug resume --latest` 和 `pdman debug ranges --latest` 的 readable 输出会显示 latest search diagnostics：搜索 root、valid candidate 数、skipped invalid metadata 数；未找到 latest metadata 时会显示实际搜索过的 roots。`--json` / `--jsonl` 输出 contract 不变。v0.7.9 固定这些 diagnostics helper 和 readable 格式的测试边界；自动化脚本仍应优先使用 `--json` / `--jsonl`，latest diagnostics 在 v0.7.x 中保持 readable-only。

v0.7.10 起，`docs/releases/v0.7.md` 汇总 0.7.x 的 request probe、network taxonomy、range taxonomy、metadata hygiene、debug latest diagnostics、stable surfaces 和 non-goals。该版本是 0.7 release readiness 收束，不新增运行能力。

v0.7.11 起，下载任务的 Rich progress 生命周期收束为“一个 Downloader 只创建一个下载进度 task”。static / dynamic 下载、task-level retry 和 merge 会复用同一条进度；任务进入 completed / skipped / failed 等终态时停止该进度，避免 retry 后出现多条同名下载进度、旧进度 elapsed 继续增加或重复显示 completed download 日志。该版本不改变 retry/backoff、分块调度、请求策略、queue schema、history/run JSON 或 metadata contract。

v0.7.12 起，发布 v0.7.11 tag 之后累积的 static hotfix：chunk 写入前重建缺失父目录，static range 连接类错误保持 chunk-local retry，已完成 chunk 不再重复调度，static resume metadata 在关键 retry 边界刷新，控制台日志不再额外插入空行。该版本只做 0.7.x 可靠性与发布对齐修复，不引入动态 chunk 分配、自适应并发、queue schema、history/run JSON 或 metadata contract 变化。

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
pdman --tmp-policy system "https://example.com/file.bin"
pdman --tmp-policy target "https://example.com/file.bin"
pdman --tmp /data/tmp     "https://example.com/file.bin"
pdman --cache-dir /data/cache/pdman "https://example.com/file.bin"
pdman --keep-tmp          "https://example.com/file.bin"
```

- `--tmp` 优先级最高。
- `--tmp-policy auto` 是默认值，优先使用系统临时目录；已知大小且空间不足时回退到目标目录。
- `--tmp-policy system` 强制使用系统临时目录；空间不足会把任务标记为 `failed`。
- `--tmp-policy target` 保留旧行为，在目标目录旁创建 `.pdman.<sha>`。
- `--tmp DIR` 是显式指定，不会在空间不足时偷偷回退。
- `--cache-dir` 覆盖默认 `~/.cache/pdman`。
- `--keep-tmp` 会在 run failed 或中断时保留 runtime tmp 目录，便于 debug 和人工恢复；它不是稳定的 resume metadata 接口。

### 历史查询命令

v0.4.1 开始，可以直接查询 runtime history 和 run summary。

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

这些命令只读取 `~/.cache/pdman` 中的历史记录，不会启动下载任务。也可以用 `--cache-dir DIR` 查询自定义 cache 目录。

### Records 查询命令

v0.8.0 开始，`pdman records list` 提供面向 agent 和脚本的只读 records summary。它读取现有 runtime history，但输出的是压缩后的 task record view，用于稳定回答“最近有哪些任务、属于哪个 run/task、URL 和文件名是什么、状态和关键诊断是什么”。

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
pdman records list --cache-dir /data/cache/pdman --json
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
pdman records doctor --fail-on warning
pdman records doctor --severity warning --code invalid_status --jsonl
pdman records schema
pdman records schema --surface show --json
```

当前 `records doctor` contract 总览：它是只读健康检查，不修复 history，不迁移 schema，不读取完整 metadata，不触发 debug 或 queue。`--json` 输出完整 report，包含 summary、`issue_groups` 和逐条 `issues`；`--jsonl` 只输出逐 issue stream；readable 输出固定为 summary、status counts、metadata counts、issue groups、issues。`records schema --surface doctor --json` 可在不运行 doctor 的情况下读取这些输出边界。

`records list --json` 输出 `{records, count}`；每个 record 包含 `run_id`、`task_id`、`url`、`filename`、`target_path`、`status`、`file_size`、`created_at`、`completed_at`、`resume_rejection`、`header_probe` 和 `network_error`。`--jsonl` 每行输出一个 record，方便 agent 或管道逐条处理。

v0.8.1 起，`records list` 支持基础 exact-match filters：`--status completed|skipped|failed`、`--url URL`、`--target PATH`、`--run-id RUN_ID` 和 `--limit N`。这些过滤不做 fuzzy search、regex、contains 或 URL/path normalization；`--target` 按记录中的目标路径字符串精确匹配。`--limit 0` 表示不过滤数量；保留 `--last` 兼容 v0.8.0，若同时传入 `--limit`，以 `--limit` 为准。

v0.8.2 起，`pdman records metadata` 提供 metadata locator foundation，可按 `--url`、`--target` 或 `--run-id` 查询对应 cache metadata 路径。输出只包含 `resume` 与 `dynamic_ranges` 的 `path`、`exists` 和 `source=cache`，不会读取或输出完整 metadata 内容。`--url` 可以直接根据当前 cache layout 推导路径；`--target` 和 `--run-id` 先通过 history records 匹配，再根据 record URL 推导路径。同一 URL 多次下载到不同 target 时会返回多个 matches。

v0.8.3 起，`pdman records show --run-id <run-id> --task-id <task-id>` 输出单个 task 的 agent-readable summary，包括基础字段、compact error、resume/header/network 诊断、metadata locator 和 `suggested_commands`。建议命令只在对应 metadata 文件真实存在时生成，例如 `pdman debug resume --metadata ...` 或 `pdman debug ranges ...`。

v0.8.4 起，`records show --json` 额外提供结构化 `suggested_debug` bridge，供 agent 不解析 shell 字符串也能理解下一步 debug 入口。每个 item 包含 `kind`、`metadata_key`、`metadata_path`、`source`、`reason`、`argv` 和 shell-quoted `command`；`suggested_commands` 作为兼容字段继续保留。

v0.8.5 起，`pdman records schema` 输出 records surface 的 agent-readable contract，描述 `list`、`metadata`、`show` 的 selector、输出格式、共享 payload 和明确 non-goals。`--surface list|metadata|show` 可聚焦单个 surface；`--json` 供脚本和 agent 读取。

v0.8.6 起，records JSON 的诊断边界更稳定：metadata locator 固定包含 `path`、`exists`、`source`、`status` 和 `reason`；`records metadata` 会用 `skipped/skipped_count` 报告因缺失 URL 无法定位 metadata 的记录；`records show --json` 找不到 task 时返回结构化 `record_not_found` 错误。

v0.8.7 起，`pdman records doctor` 提供只读 records 健康检查，统计 history record 数量、status 分布、metadata locator 状态，并用结构化 `issues` 报告 `invalid_status`、`run_id_missing`、`task_id_missing`、`url_missing` 等问题。它不修复历史、不迁移 schema、不读取完整 metadata。

v0.8.8 起，`records doctor` 支持 `--fail-on never|warning|error`。默认 `never` 保持 exit code 0；`warning` 会在 doctor 状态为 warning/error 时返回 1；`error` 只在状态为 error 时返回 1，方便 CI 或 agent workflow 按阈值失败。

v0.8.9 起，`records doctor` 支持 repeatable `--severity info|warning|error` 和 `--code ISSUE_CODE` 过滤 issues。过滤会影响输出的 `issues`、`issue_count` 和 doctor `status`，但会保留 `total_issue_count` 表示过滤前问题总数。

v0.8.10 起，`records doctor` 的每个 issue 增加 `impact` 和 `suggested_action`，让 agent 和人类能直接理解问题影响与下一步建议。该建议仍然只是诊断提示，不会自动修复 history、metadata 或 queue。

v0.8.11 起，`records doctor --json` 额外输出 `issue_groups`，按 issue code 聚合 `count`、`impact`、`suggested_action` 和少量 `sample_records`。readable 输出也会先显示 `issue_groups` 摘要；`--jsonl` 仍保持逐 issue 输出。

v0.8.12 起，doctor contract 提供稳定示例 payload：`ok`、`warning_grouped`、`filtered_warning`。示例用于测试和文档，不读取 cache，不启动下载。精简 JSON 结构如下：

```json
{
  "schema_version": 1,
  "status": "warning",
  "issue_count": 1,
  "total_issue_count": 3,
  "filters": {"severities": ["warning"], "codes": ["invalid_status"]},
  "issue_groups": [{"code": "invalid_status", "count": 1, "sample_records": [{"run_id": "run-2", "task_id": "task-2"}]}],
  "issues": [{"code": "invalid_status", "impact": "...", "suggested_action": "..."}]
}
```

v0.8.13 起，doctor 输出模式边界固定：`--json` 输出完整 doctor report；`--jsonl` 只输出逐 issue stream，不包含 `issue_groups`、`status_counts`、`metadata_state_counts` 等 summary 字段；readable 输出顺序固定为 summary、status counts、metadata counts、issue groups、issues。

v0.8.14 起，`pdman records schema --surface doctor` 会自描述 doctor 三种输出模式：readable 是人类摘要，`--json` 是完整 report，`--jsonl` 是逐 issue stream。agent 可先读取 schema 判断消费哪种输出，而不需要实际运行 doctor。

v0.8.15 起，records doctor 子阶段收口：README 增加当前 contract 总览，测试覆盖 doctor JSON、JSONL 和 schema contract 的一致性，后续不建议继续向 doctor 增加新功能，除非进入新的主题版本。

v0.8.16 起，`docs/releases/v0.8.md` 汇总 v0.8 records query、metadata locator、single-record inspection、schema、doctor contract、stable surfaces、non-goals 和 release-readiness 检查。该版本只做发布收束和边界文档，不改变运行行为。

`records` 与 `history` 的边界不同：`history` 保留原始运行历史视角和已有 contract；`records` 是 agent-oriented query view，不读取完整 resume/dynamic metadata，不嵌入 metadata 内容，不改变下载、queue 或 run 行为。当前 `cache_root/metadata/<url-hash>/` 只是 locator 使用的 cache layout，不是 database schema；v0.8.x 仍不引入 database/index engine。

### 本地队列命令

v0.4.3 开始，可以把任务写入本地队列后再启动下载：

```bash
pdman queue add "https://example.com/file.bin"
pdman queue add --json "https://example.com/file.bin"
pdman queue add -i tasks.yaml
pdman queue add -i tasks.yaml --list-groups
pdman queue add -i tasks.yaml --group nt-db --dry-run
pdman queue add -i tasks.yaml --group nt-db --dry-run --json
pdman queue add -i tasks.yaml --group nt-db
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

v0.8.18 起，`queue add` 支持 YAML schema v2 的 group 入口：`--list-groups` 只列出 groups，不写 queue；`--dry-run` 只预览将要入队的 records，不写 queue；`--group NAME` 只解析指定 group。queue record schema 仍保持 v1，不保存 `TaskInput.options`，也不把 schema v2 的 `retry`、`headers` 等额外字段映射到 queue record。

v0.8.19 起，`pdman input schema` 提供输入格式 contract inspection；`--json` 输出供脚本和 agent 读取。该命令只描述 legacy 输入、YAML schema v2 字段、优先级、mapped fields、preserved option fields 和 non-goals；不会读取具体 input file、不会启动下载、不会写 queue。

v0.8.20 起，YAML schema v2 解析错误会带稳定 reason code，例如 `schema_v2_missing_version`、`schema_v2_defaults_not_mapping`、`schema_v2_group_not_found` 和 `task_mapping_url_missing`。main CLI `--dry-run` 的错误会显示 `[code] message`；`queue add --json` 解析失败时会输出 `{ "error": { "code", "message", "field" } }`，且不会写 queue。

v0.8.21 起，主下载入口也支持 JSON dry-run：`pdman -i tasks.yaml --group NAME --dry-run --json` 会输出 `{ "dry_run": true, "count", "group", "tasks" }`；每个 task 包含 `url`、`file_name`、`dir_path`、`md5`、`log_path`，schema v2 group task 还会包含 `group` 和 `options`。解析失败时复用 `{ "error": { "code", "message", "field" } }`。

v0.8.22 起，`pdman input examples` 和 `pdman input examples --json` 提供稳定 in-memory examples：`minimal`、`grouped`、`group_selected`、`with_options`、`invalid_defaults`、`invalid_missing_url`。这些 examples 用于测试、人类文档和 agent contract 读取，不读取文件、不启动下载、不写 queue。v0.8 YAML schema v2 线到此收口；后续不继续扩展下载行为，per-task retry/header 映射和 queue schema 扩展进入后续主题版本。

队列文件位于 `~/.cache/pdman/queue.jsonl`。`queue start` 会读取 `pending` 任务，使用真实 `Manager` 执行下载，并把队列状态更新为 `completed`、`skipped` 或 `failed`。

v0.4.5 开始，queue record 会记录 `attempts`。每次 `queue start` 或 `queue retry-failed` 真正取出任务执行时，`attempts` 会递增。`queue retry-failed` 会读取 failed 任务重新执行；成功后状态变成 `completed` 且 `last_error=None`，再次失败则保持 `failed` 并更新 `last_error`。

v0.4.6 开始，queue record 会记录 `last_status_reason`，用于保存最近一次 completed/skipped/failed 的原因。`retry-failed --dry-run` 只预览候选，不修改 queue；`--max-attempts N` 只重试 `attempts < N` 的失败记录；`--error-contains TEXT` 只重试 `last_error` 包含指定文本的失败记录，匹配不区分大小写。

v0.4.7 开始，queue 查询/预览/校验命令提供最小结构化输出：`queue list --json` 输出 `{records, count}`，`queue list --jsonl` 每行输出一个 queue record；`queue retry-failed --dry-run --json` 输出 `{candidates, count, dry_run}`，`--jsonl` 每行输出一个候选 record；`queue validate --json` 输出 `{ok, valid, malformed, invalid, duplicate_ids, unsupported_schema, issues}`。`retry-failed --json/--jsonl` 只允许和 `--dry-run` 搭配，普通下载和实际 retry 执行输出仍保持人类文本。

v0.4.8 开始，queue 的非下载类维护命令继续补齐 JSON 输出：`queue add --json` 输出 `{added, records, count}`，`queue repair --json` 输出 repair stats，`queue recover --json` 输出 `{recovered}`，`queue remove --json` 输出 `{requested, removed}`，`queue clear --json` 输出 `{cleared, status, all}`。`queue list --last 0` 表示不限制数量，返回匹配的全部 queue records。

v0.4.4 开始，queue record 会写入 `schema_version=1`，queue 写操作会使用 `~/.cache/pdman/queue.lock`。锁 backend 会按平台自动选择：Linux/macOS/BSD 使用 `fcntl`，Windows 使用 `msvcrt`，其他平台使用原子目录锁 fallback。

维护命令用于检查、修复和清理 queue：`validate` 只报告问题，`repair` 会重写为可用 queue，`recover` 会把 stale `running` 任务恢复为 `pending`，`remove` 按 ID 删除，`clear` 按状态或 `--all` 清理。

当前队列不提供自动 retry scheduler、backoff、per-record retry policy、优先级、daemon、SQLite、全局 JSON/JSONL 输出协议或 agent event stream，这些放到后续版本。

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
