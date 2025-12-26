#!/usr/bin/env python3
"""
测试脚本 - Test Script for Download Manager
用于测试下载管理器的基本功能
"""

import os
import sys
import time
import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from download_manager import DownloadConfig, DownloadThread, DownloadManager


class TestDownloadConfig(unittest.TestCase):
    """测试下载配置类"""
    
    def test_default_config(self):
        """测试默认配置"""
        config = DownloadConfig()
        self.assertEqual(config.num_threads, 32)
        self.assertEqual(config.speed_threshold, 10240)
        self.assertEqual(config.chunk_size, 8192)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.max_retries, 3)
    
    def test_custom_config(self):
        """测试自定义配置"""
        config = DownloadConfig(
            num_threads=64,
            speed_threshold=20480,
            chunk_size=16384,
            timeout=60,
            max_retries=5
        )
        self.assertEqual(config.num_threads, 64)
        self.assertEqual(config.speed_threshold, 20480)
        self.assertEqual(config.chunk_size, 16384)
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.max_retries, 5)


class TestDownloadThread(unittest.TestCase):
    """测试下载线程类"""
    
    def setUp(self):
        """设置测试环境"""
        self.url = "https://example.com/test.file"
        self.output_file = "/tmp/test_output.file"
        self.config = DownloadConfig()
        self.headers = {'User-Agent': 'Test'}
        
    def test_thread_initialization(self):
        """测试线程初始化"""
        thread = DownloadThread(0, 0, 1023, self.url, self.output_file, 
                               self.config, self.headers)
        self.assertEqual(thread.thread_id, 0)
        self.assertEqual(thread.start, 0)
        self.assertEqual(thread.end, 1023)
        self.assertEqual(thread.current_pos, 0)
        self.assertFalse(thread.completed)
        self.assertFalse(thread.running)
    
    def test_get_chunk_file(self):
        """测试获取临时文件路径"""
        thread = DownloadThread(5, 0, 1023, self.url, self.output_file,
                               self.config, self.headers)
        chunk_file = thread.get_chunk_file()
        self.assertEqual(chunk_file, f"{self.output_file}.part5")
    
    def test_get_state(self):
        """测试获取线程状态"""
        thread = DownloadThread(0, 0, 1023, self.url, self.output_file,
                               self.config, self.headers)
        thread.current_pos = 512
        thread.downloaded = 512
        
        state = thread.get_state()
        self.assertEqual(state['thread_id'], 0)
        self.assertEqual(state['start'], 0)
        self.assertEqual(state['end'], 1023)
        self.assertEqual(state['current_pos'], 512)
        self.assertEqual(state['downloaded'], 512)
        self.assertFalse(state['completed'])
    
    def test_load_state(self):
        """测试加载线程状态"""
        thread = DownloadThread(0, 0, 1023, self.url, self.output_file,
                               self.config, self.headers)
        
        state = {
            'current_pos': 512,
            'completed': False,
            'downloaded': 512
        }
        thread.load_state(state)
        
        self.assertEqual(thread.current_pos, 512)
        self.assertEqual(thread.downloaded, 512)
        self.assertFalse(thread.completed)


class TestDownloadManager(unittest.TestCase):
    """测试下载管理器类"""
    
    def setUp(self):
        """设置测试环境"""
        self.url = "https://example.com/test.file"
        self.output_path = "/tmp/test_download.file"
        self.config = DownloadConfig(num_threads=4)
        
        # 清理可能存在的文件
        if os.path.exists(self.output_path):
            os.remove(self.output_path)
        if os.path.exists(f"{self.output_path}.state"):
            os.remove(f"{self.output_path}.state")
    
    def tearDown(self):
        """清理测试环境"""
        # 清理测试文件
        for f in [self.output_path, f"{self.output_path}.state"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass
    
    def test_manager_initialization(self):
        """测试管理器初始化"""
        manager = DownloadManager(self.url, self.output_path, self.config)
        self.assertEqual(manager.url, self.url)
        self.assertEqual(manager.output_path, self.output_path)
        self.assertEqual(manager.config.num_threads, 4)
        self.assertIsInstance(manager.headers, dict)
    
    def test_get_filename_from_url(self):
        """测试从URL提取文件名"""
        manager = DownloadManager("https://example.com/path/to/file.zip")
        self.assertEqual(manager.output_path, "file.zip")
        
        manager2 = DownloadManager("https://example.com/")
        self.assertEqual(manager2.output_path, "downloaded_file")
    
    def test_initialize_threads(self):
        """测试初始化线程"""
        manager = DownloadManager(self.url, self.output_path, self.config)
        manager.total_size = 1024 * 1024  # 1MB
        manager._initialize_threads()
        
        self.assertEqual(len(manager.threads), 4)
        
        # 验证线程范围
        total_covered = 0
        for thread in manager.threads:
            total_covered += (thread.end - thread.start + 1)
        
        self.assertEqual(total_covered, manager.total_size)
    
    def test_get_downloaded_size(self):
        """测试获取已下载大小"""
        manager = DownloadManager(self.url, self.output_path, self.config)
        manager.total_size = 1024
        manager._initialize_threads()
        
        # 模拟部分下载
        manager.threads[0].downloaded = 100
        manager.threads[1].downloaded = 200
        
        downloaded = manager._get_downloaded_size()
        self.assertEqual(downloaded, 300)
    
    def test_get_total_speed(self):
        """测试获取总速度"""
        manager = DownloadManager(self.url, self.output_path, self.config)
        manager.total_size = 1024
        manager._initialize_threads()
        
        # 模拟下载速度
        manager.threads[0].speed = 1024.0
        manager.threads[1].speed = 2048.0
        
        total_speed = manager._get_total_speed()
        self.assertEqual(total_speed, 3072.0)


def run_basic_functionality_test():
    """运行基本功能测试"""
    print("=" * 60)
    print("基本功能测试 / Basic Functionality Test")
    print("=" * 60)
    
    # 测试配置
    print("\n1. 测试下载配置...")
    config = DownloadConfig(num_threads=16, speed_threshold=20480)
    print(f"   ✓ 配置创建成功: {config.num_threads}线程, 阈值{config.speed_threshold}字节/秒")
    
    # 测试线程
    print("\n2. 测试下载线程...")
    thread = DownloadThread(0, 0, 1023, "https://example.com/test", 
                           "/tmp/test", config, {})
    print(f"   ✓ 线程创建成功: ID={thread.thread_id}, 范围=[{thread.start}, {thread.end}]")
    
    state = thread.get_state()
    print(f"   ✓ 状态获取成功: {state}")
    
    # 测试管理器
    print("\n3. 测试下载管理器...")
    manager = DownloadManager("https://example.com/test.zip", None, config)
    print(f"   ✓ 管理器创建成功: URL={manager.url}")
    print(f"   ✓ 输出文件: {manager.output_path}")
    
    # 测试文件名提取
    print("\n4. 测试文件名提取...")
    test_urls = [
        "https://example.com/file.zip",
        "https://example.com/path/to/document.pdf",
        "https://example.com/"
    ]
    for url in test_urls:
        m = DownloadManager(url)
        print(f"   URL: {url}")
        print(f"   文件名: {m.output_path}")
    
    print("\n" + "=" * 60)
    print("所有基本功能测试通过！")
    print("=" * 60)


if __name__ == '__main__':
    # 运行基本功能测试
    run_basic_functionality_test()
    
    print("\n\n")
    
    # 运行单元测试
    print("=" * 60)
    print("单元测试 / Unit Tests")
    print("=" * 60)
    unittest.main(argv=[''], verbosity=2, exit=False)
