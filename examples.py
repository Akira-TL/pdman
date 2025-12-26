#!/usr/bin/env python3
"""
使用示例 - Usage Examples for Download Manager

演示如何使用下载管理器的各种功能
Demonstrates various features of the download manager
"""

from download_manager import DownloadManager, DownloadConfig


def example_1_basic_download():
    """示例1: 基本下载 - Basic Download"""
    print("=" * 60)
    print("示例1: 基本下载 (使用默认32线程)")
    print("Example 1: Basic Download (using default 32 threads)")
    print("=" * 60)
    
    url = "https://speed.hetzner.de/100MB.bin"  # 示例文件
    
    manager = DownloadManager(url)
    print(f"URL: {url}")
    print(f"输出文件: {manager.output_path}")
    print(f"线程数: {manager.config.num_threads}")
    print("\n开始下载...")
    
    # 取消注释以下行来实际执行下载
    # manager.start()
    print("(示例代码，未实际执行下载)")
    print()


def example_2_custom_threads():
    """示例2: 自定义线程数 - Custom Thread Count"""
    print("=" * 60)
    print("示例2: 自定义线程数 (使用64线程)")
    print("Example 2: Custom Thread Count (using 64 threads)")
    print("=" * 60)
    
    url = "https://speed.hetzner.de/100MB.bin"
    config = DownloadConfig(num_threads=64)
    
    manager = DownloadManager(url, config=config)
    print(f"URL: {url}")
    print(f"线程数: {manager.config.num_threads}")
    print("\n开始下载...")
    
    # 取消注释以下行来实际执行下载
    # manager.start()
    print("(示例代码，未实际执行下载)")
    print()


def example_3_custom_output():
    """示例3: 指定输出文件 - Custom Output File"""
    print("=" * 60)
    print("示例3: 指定输出文件")
    print("Example 3: Custom Output File")
    print("=" * 60)
    
    url = "https://speed.hetzner.de/100MB.bin"
    output_path = "/tmp/my_download.bin"
    
    manager = DownloadManager(url, output_path=output_path)
    print(f"URL: {url}")
    print(f"输出文件: {manager.output_path}")
    print("\n开始下载...")
    
    # 取消注释以下行来实际执行下载
    # manager.start()
    print("(示例代码，未实际执行下载)")
    print()


def example_4_custom_speed_threshold():
    """示例4: 自定义速度阈值 - Custom Speed Threshold"""
    print("=" * 60)
    print("示例4: 自定义速度阈值 (20KB/s)")
    print("Example 4: Custom Speed Threshold (20KB/s)")
    print("=" * 60)
    
    url = "https://speed.hetzner.de/100MB.bin"
    config = DownloadConfig(
        num_threads=32,
        speed_threshold=20480  # 20KB/s
    )
    
    manager = DownloadManager(url, config=config)
    print(f"URL: {url}")
    print(f"速度阈值: {manager.config.speed_threshold} 字节/秒 ({manager.config.speed_threshold/1024:.1f} KB/s)")
    print("\n当某线程速度低于阈值时，会自动重启该线程")
    
    # 取消注释以下行来实际执行下载
    # manager.start()
    print("(示例代码，未实际执行下载)")
    print()


def example_5_full_configuration():
    """示例5: 完整配置 - Full Configuration"""
    print("=" * 60)
    print("示例5: 完整配置")
    print("Example 5: Full Configuration")
    print("=" * 60)
    
    url = "https://speed.hetzner.de/100MB.bin"
    output_path = "/tmp/custom_file.bin"
    
    config = DownloadConfig(
        num_threads=128,           # 128线程
        speed_threshold=20480,     # 20KB/s 速度阈值
        chunk_size=16384,          # 16KB 块大小
        timeout=60,                # 60秒超时
        max_retries=5              # 最多重试5次
    )
    
    manager = DownloadManager(url, output_path=output_path, config=config)
    
    print(f"URL: {url}")
    print(f"输出文件: {manager.output_path}")
    print(f"配置:")
    print(f"  - 线程数: {config.num_threads}")
    print(f"  - 速度阈值: {config.speed_threshold/1024:.1f} KB/s")
    print(f"  - 块大小: {config.chunk_size/1024:.1f} KB")
    print(f"  - 超时时间: {config.timeout} 秒")
    print(f"  - 最大重试: {config.max_retries} 次")
    
    # 取消注释以下行来实际执行下载
    # manager.start()
    print("\n(示例代码，未实际执行下载)")
    print()


def example_6_resume_download():
    """示例6: 断点续传 - Resume Download"""
    print("=" * 60)
    print("示例6: 断点续传")
    print("Example 6: Resume Download")
    print("=" * 60)
    
    print("断点续传功能说明:")
    print("1. 下载过程中，状态会自动保存到 .state 文件")
    print("2. 如果下载被中断（Ctrl+C 或其他原因）")
    print("3. 重新运行相同命令，会自动从上次中断处继续")
    print()
    print("示例:")
    print("  第一次运行:")
    print("    python download_manager.py https://example.com/file.zip")
    print()
    print("  中断后重新运行（会自动继续）:")
    print("    python download_manager.py https://example.com/file.zip")
    print()
    print("状态文件位置: <输出文件>.state")
    print("例如: file.zip.state")
    print()


def main():
    """运行所有示例"""
    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + " " * 10 + "多线程断点续传下载管理器" + " " * 10 + "║")
    print("║" + " " * 5 + "Multi-threaded Download Manager Examples" + " " * 6 + "║")
    print("╚" + "═" * 58 + "╝")
    print("\n")
    
    # 运行所有示例
    example_1_basic_download()
    example_2_custom_threads()
    example_3_custom_output()
    example_4_custom_speed_threshold()
    example_5_full_configuration()
    example_6_resume_download()
    
    print("=" * 60)
    print("所有示例已展示完毕")
    print("All examples completed")
    print("=" * 60)
    print()
    print("要实际运行下载，请使用以下命令:")
    print("To actually run a download, use:")
    print()
    print("  python download_manager.py <URL>")
    print()
    print("或查看帮助信息:")
    print("Or view help:")
    print()
    print("  python download_manager.py --help")
    print()


if __name__ == '__main__':
    main()
