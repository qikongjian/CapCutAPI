"""
导出进度缓存管理模块
用于存储和管理视频导出进度信息，避免频繁唤起剪映应用
"""

import time
import threading
from typing import Dict, Optional

class ExportProgressCache:
    """导出进度缓存管理器"""
    
    def __init__(self, cache_duration: int = 1800):  # 默认缓存30分钟
        self.cache_duration = cache_duration
        self.progress_cache: Dict[str, dict] = {}
        self.lock = threading.Lock()
    
    def set_progress(self, draft_name: str, progress_data: dict) -> None:
        """设置导出进度"""
        with self.lock:
            progress_data["cache_time"] = time.time()
            self.progress_cache[draft_name] = progress_data.copy()
    
    def get_progress(self, draft_name: str) -> Optional[dict]:
        """获取导出进度"""
        with self.lock:
            if draft_name not in self.progress_cache:
                return None
            
            progress_data = self.progress_cache[draft_name]
            
            # 检查缓存是否过期
            if time.time() - progress_data.get("cache_time", 0) > self.cache_duration:
                # 缓存过期，删除记录
                del self.progress_cache[draft_name]
                return None
            
            # 更新elapsed时间（如果任务还在进行中）
            if progress_data["status"] not in ["idle", "finished", "error", "export_success_upload_failed"]:
                progress_data["elapsed"] = time.time() - progress_data["start_time"]
            
            return progress_data.copy()
    
    def get_latest_progress(self) -> Optional[dict]:
        """获取最新的导出进度（开始时间最晚的）"""
        with self.lock:
            if not self.progress_cache:
                return {"status": "idle", "percent": 0.0, "message": "", "start_time": 0, "elapsed": 0}
            
            # 清理过期缓存
            current_time = time.time()
            expired_keys = []
            for key, value in self.progress_cache.items():
                if current_time - value.get("cache_time", 0) > self.cache_duration:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.progress_cache[key]
            
            if not self.progress_cache:
                return {"status": "idle", "percent": 0.0, "message": "", "start_time": 0, "elapsed": 0}
            
            # 找到最新的进度（开始时间最晚的）
            latest_draft_name = max(self.progress_cache.keys(), 
                                  key=lambda k: self.progress_cache[k].get("start_time", 0))
            return self.get_progress(latest_draft_name)
    
    def clear_progress(self, draft_name: str) -> None:
        """清除指定草稿的进度记录"""
        with self.lock:
            if draft_name in self.progress_cache:
                del self.progress_cache[draft_name]
    
    def clear_all(self) -> None:
        """清除所有进度记录"""
        with self.lock:
            self.progress_cache.clear()

# 全局缓存实例
export_progress_cache = ExportProgressCache() 