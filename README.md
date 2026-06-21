# pdman — 模拟 IDM 下载流程的 Python 工具

一个使用 Python 实现的异步多段下载器，支持多连接分块下载、断点续传、下载进度展示、低速分片自动重启、批量任务等功能。

---

## 功能概览

- **多连接分块下载**：同一 URL 通过 Range 头拆分为多个分块并发下载，动态分块提升利用率。
- **断点续传**：每个任务使用 `.pdman.<sha>` 目录保存分块和 `.pdman` 元信息，配合 `--continue` 恢复下载。
- **下载进度展示**：使用 rich 展示每个文件的进度条、速度、已用时间和剩余时间，支持可配置刷新间隔。
- **失败重试**：支持整体任务重试（`--retry`）和分块级重试 + 低速重启（`--chunk-retry-speed`），滑动窗口平均速度避免误触发。
- **并发控制**：
  - `--max-downloads` 控制同时下载的 URL 个数；
  - `--max-concurrent-downloads` 控制单个 URL 内部并发的分块数；
  - `--force-sequential` 强制单 URL 顺序下载；
  - `--max-connection-per-server` 限制单服务器并发连接数。
- **速度限制**：支持单任务（`--max-download-limit`）及全局（`--max-overall-download-limit`）令牌桶限速，避免占满带宽。
- **HTTP 认证**：支持 Basic/Digest 认证（`--http-auth user:pass`）。
- **代理支持**：HTTP/HTTPS 代理及代理认证（`--proxy` / `--proxy-auth`）。
- **Cookie 支持**：从 Netscape/Mozilla 格式文件加载 Cookie（`--cookie-file`），兼容浏览器导出格式。
- **自定义请求头**：可重复 `--header "Key: Value"` 添加任意 HTTP 头，常用于 Referer / Authorization 等场景。
- **连接与 SSL 控制**：独立连接超时（`--connect-timeout`）、SSL 证书验证开关（`--no-check-certificate`）、自定义 CA 证书（`--ca-certificate`）。
- **下载完成回调**：`--on-download-complete` 指定 shell 命令，支持 `{filename}` 等占位符，可用于通知、后处理等自动流程。
- **配置文件支持**：`--conf-path` 加载 JSON/YAML 配置，CLI 参数优先级更高。
- **日志**：
  - 全局日志输出到终端或指定文件（`-l/--log`）；
  - 每个 URL 额外有一个独立的 `.pdman.<sha>.log` 日志（位于对应下载目录内）；
  - `--debug` 启用详细调试日志。
- **完整性校验（MD5）**：
  - 若任务中提供 `md5` 字段并启用 `--check-integrity`，下载完成后会对合并后的文件做 MD5 校验。
  - `md5` 可为 32 位 MD5 字符串、本地文件路径或一个返回 MD5 字符串的 URL。
- **批量下载任务文件**：支持 JSON / YAML / 纯文本三种格式（`-i/--input-file`）。
- **quit 模式**：`-q` 目标文件已存在则跳过，适合增量备份场景。

---

## 安装依赖

```bash
pip install -r requirements.txt
pip install git+https://github.com/Akira-TL/pdman.git
```

---

## 快速开始

### 1. 单 URL 下载

```bash
pdman "https://example.com/file.bin"
```

行为说明：

- 输出目录：默认当前工作目录。
- 文件名：优先使用服务器返回的 `Content-Disposition` 中的 `filename`；否则取 URL 路径末尾；若无法获取则使用 URL 的哈希值生成一个 `.dat` 文件名。
- 分块与临时文件：
  - 会在目标目录下创建一个 `.pdman.<sha>` 目录（`<sha>` 为 URL 的短哈希），
  - 其中保存所有分块文件以及一个 `.pdman` 元信息文件。

下载完成后，分块会被合并为最终文件，`.pdman.<sha>` 目录会被删除。

### 2. 断点续传

按下 Ctrl+C 或进程异常结束后，可以使用 `--continue` 继续下载：

```bash
pdman --continue "https://example.com/file.bin"
```

行为：

- 若对应的 `.pdman.<sha>` 目录和 `.pdman` 元数据存在且信息匹配（URL、文件名、文件大小、MD5 等），则会在现有分块基础上继续下载。
- 如果元数据与当前任务不一致，则会清空该临时目录并重新开始。

### 3. 批量下载

使用 `-i/--input-file` 可以从文件中读取任务。该参数可以重复出现，多个文件会顺序加载。

#### 3.1 纯文本

`urls.txt`：

```text
https://example.com/a.iso
https://example.com/b.zip
```

执行：

```bash
pdman -i urls.txt
```

每个 URL 会使用当前工作目录作为下载目录，文件名按前文规则自动推断。

#### 3.2 JSON 任务文件

JSON 结构为：

```json
{
  "https://example.com/a.iso": {
    "file_name": "linux.iso",
    "dir_path": "/data/downloads",
    "md5": "0123456789abcdef0123456789abcdef",
    "log_path": "/data/logs/a.log"
  },
  "https://example.com/b.zip": {
    "dir_path": "/data/downloads/b",
    "md5": "https://example.com/b.zip.md5"
  }
}
```

```bash
pdman -i tasks.json
```

字段说明：

- `file_name`：最终合并后的文件名（可选）。
- `dir_path`：该 URL 的下载目录（可选，不填则为当前工作目录）。
- `md5`：
  - 32 位 MD5 字符串，或
  - 本地文件路径（从文件读取 MD5），或
  - 以 http/https/ftp 开头的 URL（从响应内容读取 MD5）。
- `log_path`：该 URL 的日志文件路径（可选）。

#### 3.3 YAML 任务文件

YAML 结构与 JSON 类似：

```yaml
https://example.com/a.iso:
  file_name: linux.iso
  dir_path: /data/downloads
  md5: 0123456789abcdef0123456789abcdef
  log_path: /data/logs/a.log

https://example.com/b.zip:
  dir_path: /data/downloads/b
  md5: https://example.com/b.zip.md5
```

执行：

```bash
pdman -i tasks.yaml
```

---

## 下载行为与分块策略

- 文件大小获取：
  - 先通过 HEAD 请求读取 `Content-Length`；若不存在则记为 `-1`（表示未知大小）。
- 初始分块：
  - 默认将文件按 `max_concurrent_downloads` 等分；
  - 分块大小不少于 `min_split_size`（`-k/--min-split-size`），并对齐到 10 KiB 的整数倍。
- 动态分块：
  - 下载过程中，程序会在空隙较大的分块中间再拆分出新的分块，提高利用率。
- 低速重启：
  - 每个分块在下载时会统计瞬时速度；
  - 若 `--chunk-retry-speed` 设置了阈值，且速度低于该值，会中断当前分块请求，稍后重试该分块。

---

## 命令行参数

### 通用

- [x] `-v, --version`：打印版本号后退出。
- [x] `-l, --log PATH`：日志文件路径（`-` 表示 stdout）。
- [x] `--debug`：启用调试模式，日志级别提升为 DEBUG。
- [x] `--conf-path PATH`：从 JSON/YAML 配置文件加载默认参数（CLI 参数优先级更高）。

### 下载目标与输出

- [x] `-d, --dir DIR`：指定下载目录。
- [x] `-o, --out NAME`：指定输出文件名，若 url 多于 1 无效。
- [x] `-q, --quit`：如果目标文件已存在则跳过下载。
- [x] 位置参数 `urls...`：要下载的 URL，可以传多个。

### 下载控制

- [x] `-N, --max-downloads INT`：同时下载的 URL 最大数量（默认 4）。
- [x] `-x, --max-concurrent-downloads INT`：每个 URL 内部并发的分块下载数量（默认 5）。
- [x] `-Z, --force-sequential`：强制顺序下载。
- [x] `-k, --min-split-size SIZE`：分块最小尺寸（默认 `1M`；支持 K/M/G 后缀）。
- [x] `--max-connection-per-server INT`：单服务器最大连接数（默认 0 = 不限制）。
- [x] `-r, --retry INT`：失败重试次数（默认 3）。
- [x] `-W, --retry-wait SECONDS`：重试等待时间（默认 5 秒）。
- [x] `--timeout SECONDS`：HTTP 请求超时时间（默认 60 秒）。
- [x] `--connect-timeout SECONDS`：连接建立超时，独立于读写超时。
- [x] `--chunk-timeout SECONDS`：分块下载超时时间（默认 10 秒）。
- [x] `--chunk-retry-speed SIZE`：分块低速阈值（字节/秒），低于该值重启分块。
- [x] `--max-download-limit SIZE`：单任务下载限速（支持 K/M/G 后缀）。
- [x] `--max-overall-download-limit SIZE`：全局下载限速（所有任务合计）。
- [x] `-c, --continue`：启用断点续传。
- [x] `--tmp DIR`：分块临时文件根目录。

### 认证与代理

- [x] `--http-auth AUTH`：HTTP 认证，格式 `user:pass`。
- [x] `--cookie-file PATH`：从 Netscape/Mozilla 格式文件加载 Cookie。
- [x] `--proxy URL`：HTTP/HTTPS 代理地址。
- [x] `--proxy-auth AUTH`：代理认证，格式 `user:pass`。

### 请求头

- [x] `--header "Key: Value"`：自定义 HTTP 请求头，可重复使用。
- [x] `--referer URL`：设置 HTTP Referer 头。
- [x] `-ua, --user-agent STRING`：设置 User-Agent（默认 `PDMAN-Downloader/1.0`）。

### SSL / TLS

- [x] `--no-check-certificate`：不验证 SSL 证书。
- [x] `--ca-certificate PATH`：使用自定义 CA 证书文件。

### 完整性与校验

- [x] `-V, --check-integrity`：启用 MD5 完整性校验。

### 回调与进度

- [x] `--on-download-complete CMD`：下载完成后执行的 shell 命令，支持占位符。
- [x] `--summary-interval SECS`：进度刷新间隔（默认 1.0 秒）。

### 批量任务与日志

- [x] `-i, --input-file FILE`：从 FILE 读取下载任务；支持 JSON/YAML/纯文本，可重复。
- [x] `--auto-file-renaming BOOL`：同名文件自动追加序号重命名（默认 True）。

---

## 当前进度

- [x] 异步下载框架与分块调度
- [x] 动态分块与最小分块尺寸控制
- [x] 断点续传元信息与分块重建
- [x] 批量任务（JSON / YAML / 纯文本）
- [x] MD5 完整性校验
- [x] 分块低速自动重启（滑动窗口平均速度）
- [x] 并发控制（任务级 / 分块级 / 单 host 级）
- [x] HTTP 认证（Basic/Digest）
- [x] Cookie 文件加载（Netscape/Mozilla 格式）
- [x] 单任务 / 全局令牌桶限速
- [x] HTTP/HTTPS 代理及认证
- [x] 自定义请求头（可重复 --header）
- [x] 独立连接超时
- [x] SSL 证书验证控制
- [x] 下载完成回调钩子
- [x] 配置文件支持（JSON/YAML）
- [x] quit 模式
- [x] 可配置进度刷新间隔
- [x] rich 进度条与 loguru 日志集成

---
