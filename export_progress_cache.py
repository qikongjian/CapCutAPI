"""
导出进度缓存管理模块
用于存储和管理视频导出进度信息，避免频繁唤起剪映应用
完全基于 Redis 存储，复用 redis_cache.py 中的 Redis 连接
"""

import json
import logging
import time
from typing import Optional

# 导入 Redis 缓存类
from tools.redis_cache import RedisCache

# 配置日志
logger = logging.getLogger(__name__)

class ExportProgressCache:
    """导出进度缓存管理器（完全基于 Redis）"""
    
    def __init__(self, cache_duration: int = 86400):
        """
        初始化导出进度缓存
        
        Args:
            cache_duration: 缓存过期时间（秒），一天
        """
        self.cache_duration = cache_duration
        
        # 使用 RedisCache，如果连接失败直接抛出异常
        self.redis_cache = RedisCache(
            cache_prefix="export_progress",
            ttl_hours=cache_duration / 3600  # 转换为小时
        )
        logger.info(f"导出进度缓存已连接到 Redis，TTL: {cache_duration}秒")
    
    def _get_progress_key(self, draft_name: str) -> str:
        """获取导出进度的 Redis 键名"""
        return f"export_progress:progress:{draft_name}"
    
    def _serialize_progress(self, progress_data: dict) -> str:
        """序列化进度数据"""
        try:
            return json.dumps(progress_data, ensure_ascii=False)
        except Exception as e:
            logger.error(f"序列化进度数据失败: {str(e)}")
            raise
    
    def _deserialize_progress(self, data: str) -> dict:
        """反序列化进度数据"""
        try:
            return json.loads(data)
        except Exception as e:
            logger.error(f"反序列化进度数据失败: {str(e)}")
            raise
    
    def set_progress(self, draft_name: str, progress_data: dict) -> None:
        """设置导出进度"""
        progress_data["cache_time"] = time.time()
        
        cache_key = self._get_progress_key(draft_name)
        serialized_data = self._serialize_progress(progress_data)
        success = self.redis_cache.set_string(cache_key, serialized_data, self.cache_duration)
        
        if success:
            logger.debug(f"成功将导出进度存储到 Redis: {draft_name}")
        else:
            raise Exception(f"存储导出进度到 Redis 失败: {draft_name}")
    
    def get_progress(self, draft_name: str) -> Optional[dict]:
        """获取导出进度"""
        cache_key = self._get_progress_key(draft_name)
        serialized_data = self.redis_cache.get_string(cache_key)
        
        if serialized_data is None:
            logger.debug(f"Redis 中不存在导出进度: {draft_name}")
            return None
        
        progress_data = self._deserialize_progress(serialized_data)
        
        # 检查缓存是否过期
        if time.time() - progress_data.get("cache_time", 0) > self.cache_duration:
            # 缓存过期，删除记录
            self.redis_cache.delete_key(cache_key)
            logger.debug(f"导出进度缓存已过期并删除: {draft_name}")
            return None
        
        # 更新elapsed时间（如果任务还在进行中）
        if progress_data["status"] not in ["idle", "finished", "error", "export_success_upload_failed"]:
            progress_data["elapsed"] = time.time() - progress_data["start_time"]
        
        logger.debug(f"成功从 Redis 获取导出进度: {draft_name}")
        return progress_data
    
    def clear_progress(self, draft_name: str) -> None:
        """清除指定草稿的进度记录"""
        cache_key = self._get_progress_key(draft_name)
        success = self.redis_cache.delete_key(cache_key)
        
        if success:
            logger.debug(f"成功从 Redis 删除导出进度: {draft_name}")
        else:
            logger.debug(f"Redis 中不存在导出进度: {draft_name}")

# 全局缓存实例
export_progress_cache = ExportProgressCache() 