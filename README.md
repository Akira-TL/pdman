# 多线程断点续传下载管理器 / Multi-threaded Download Manager

这是一个Python实现的多线程下载管理器，模拟IDM（互联网下载管理器）的功能，支持断点续传、多线程下载和自动速度监控。

This is a Python-based multi-threaded download manager that simulates IDM (Internet Download Manager) functionality, with support for resume capability, multi-threaded downloads, and automatic speed monitoring.

## 功能特性 / Features

### 核心功能 / Core Features

1. **多线程下载** / Multi-threaded Download
   - 默认支持32线程下载
   - 可配置更多线程数
   - 基于HTTP Range请求实现分块下载
   - Default 32 threads
   - Configurable thread count
   - Chunk-based download using HTTP Range requests

2. **断点续传** / Resume Capability
   - 自动保存下载状态到 `download.state` 文件
   - 脚本重启后可从上次中断处继续下载
   - 每个线程独立记录下载进度
   - Automatically saves download state to `download.state` file
   - Resume from interruption point on restart
   - Independent progress tracking for each thread

3. **速度监控与自动重启** / Speed Monitoring & Auto-restart
   - 实时监控每个线程的下载速度
   - 当线程速度低于设定阈值（默认10KB/s）时自动重启
   - 显示总体下载速度和进度
   - Real-time speed monitoring for each thread
   - Auto-restart threads when speed drops below threshold (default 10KB/s)
   - Display overall download speed and progress

4. **文件合并** / File Merging
   - 下载完成后自动合并所有数据块
   - 删除临时文件和状态文件
   - Automatically merge all chunks after completion
   - Clean up temporary files and state files

5. **日志记录** / Logging
   - 详细的日志记录到 `download.log` 文件
   - 同时输出到控制台
   - 记录线程状态、错误信息和进度
   - Detailed logging to `download.log` file
   - Console output
   - Thread status, errors, and progress tracking

## 安装依赖 / Installation

```bash
pip install requests
```

## 使用方法 / Usage

### 基本使用 / Basic Usage

```bash
# 直接指定URL
python download_manager.py <URL>

# 或者运行后输入URL
python download_manager.py
```

### 指定输出文件 / Specify Output File

```bash
python download_manager.py <URL> -o /path/to/output.file
```

### 自定义线程数 / Custom Thread Count

```bash
# 使用64线程
python download_manager.py <URL> -t 64

# 使用128线程
python download_manager.py <URL> -t 128
```

### 设置速度阈值 / Set Speed Threshold

```bash
# 设置速度阈值为20KB/s (20480字节/秒)
python download_manager.py <URL> -s 20480
```

### 完整示例 / Complete Example

```bash
python download_manager.py https://example.com/file.zip -o myfile.zip -t 64 -s 20480
```

## 命令行参数 / Command Line Arguments

| 参数 / Argument | 说明 / Description | 默认值 / Default |
|----------------|-------------------|-----------------|
| `url` | 下载链接 / Download URL | (必需 / Required) |
| `-o, --output` | 输出文件路径 / Output file path | 从URL提取 / Extract from URL |
| `-t, --threads` | 线程数量 / Number of threads | 32 |
| `-s, --speed-threshold` | 速度阈值（字节/秒）/ Speed threshold (bytes/s) | 10240 (10KB/s) |
| `--chunk-size` | 数据块大小（字节）/ Chunk size (bytes) | 8192 (8KB) |
| `--timeout` | 请求超时时间（秒）/ Request timeout (seconds) | 30 |

## 断点续传 / Resume Download

如果下载过程中被中断（Ctrl+C或其他原因），只需使用相同的命令重新运行脚本，它会自动从上次中断的位置继续下载。

If the download is interrupted (Ctrl+C or other reasons), simply rerun the script with the same command, and it will automatically resume from where it left off.

```bash
# 第一次运行 / First run
python download_manager.py https://example.com/large-file.zip

# 中断后重新运行（会自动继续）/ Rerun after interruption (auto-resume)
python download_manager.py https://example.com/large-file.zip
```

## 工作原理 / How It Works

1. **下载前准备** / Pre-download Preparation
   - 发送HEAD请求获取文件大小
   - 检查服务器是否支持Range请求
   - 根据文件大小和线程数计算每个线程的下载范围

2. **多线程下载** / Multi-threaded Download
   - 每个线程负责下载文件的一个特定范围
   - 使用HTTP Range请求头指定下载范围
   - 每个线程将数据写入独立的临时文件（.part文件）

3. **状态监控** / Status Monitoring
   - 监控线程每5秒检查一次所有下载线程的状态
   - 计算每个线程的下载速度
   - 如果速度低于阈值，自动重启该线程
   - 定期保存下载状态到state文件

4. **断点续传** / Resume
   - 状态文件记录每个线程的当前位置
   - 重启时读取状态文件，从上次位置继续下载
   - 支持部分线程完成、部分线程未完成的情况

5. **文件合并** / File Merging
   - 所有线程完成后，按顺序合并所有.part文件
   - 生成最终的完整文件
   - 清理临时文件和状态文件

## 技术实现 / Technical Implementation

### 类结构 / Class Structure

- **DownloadConfig**: 下载配置类，管理线程数、速度阈值等参数
- **DownloadThread**: 下载线程类，负责单个线程的下载任务
- **DownloadManager**: 下载管理器类，协调所有线程和整体下载流程

### 关键特性 / Key Features

- **线程安全** / Thread Safety: 使用锁机制保护共享数据
- **异常处理** / Exception Handling: 每个线程独立重试，不影响其他线程
- **进度跟踪** / Progress Tracking: 实时显示下载进度、速度和预计剩余时间
- **资源清理** / Resource Cleanup: 自动清理临时文件和状态文件

## 生产环境部署 / Production Deployment

该脚本已按照生产环境标准编写：

This script is written according to production standards:

- ✅ 完善的错误处理和重试机制
- ✅ 详细的日志记录
- ✅ 线程安全的数据访问
- ✅ 清晰的代码结构和注释
- ✅ 可配置的参数
- ✅ 优雅的中断处理

## 示例输出 / Sample Output

```
2024-01-01 12:00:00 - INFO - 开始下载: https://example.com/file.zip
2024-01-01 12:00:00 - INFO - 输出文件: file.zip
2024-01-01 12:00:00 - INFO - 线程数: 32
2024-01-01 12:00:01 - INFO - 文件大小: 104857600 字节
2024-01-01 12:00:01 - INFO - 初始化 32 个下载线程
2024-01-01 12:00:06 - INFO - 进度: 15.23% (15990784/104857600 字节) | 速度: 3198.15 KB/s | 预计剩余时间: 27秒
2024-01-01 12:00:11 - INFO - 进度: 45.67% (47890432/104857600 字节) | 速度: 3179.93 KB/s | 预计剩余时间: 17秒
2024-01-01 12:00:16 - INFO - 线程 5: 下载完成
2024-01-01 12:00:28 - INFO - 线程 31: 下载完成
2024-01-01 12:00:28 - INFO - 开始合并文件块...
2024-01-01 12:00:29 - INFO - 文件合并完成: file.zip
2024-01-01 12:00:29 - INFO - 下载完成！
```

## 注意事项 / Notes

1. 某些服务器可能不支持Range请求，此时会自动切换为单线程下载
2. 建议根据网络环境和服务器能力调整线程数
3. 下载大文件时请确保有足够的磁盘空间
4. 状态文件和日志文件会在当前目录生成

1. Some servers may not support Range requests; single-threaded download will be used automatically
2. Adjust thread count based on network conditions and server capacity
3. Ensure sufficient disk space when downloading large files
4. State files and log files are generated in the current directory

## License

This project is licensed under the same license as the PDM repository.
