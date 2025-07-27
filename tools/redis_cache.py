import json
import logging
import os
import pickle
import time
from typing import Any, Dict, Optional, Union
import redis
import pyJianYingDraft as draft

# 配置日志
logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self, cache_prefix: str = "draft_cache", host: str = None, port: int = None, 
                 db: int = None, password: str = None, ttl_hours: int = 48):
        """
        Redis缓存类，用于替代内存缓存
        
        Args:
            cache_prefix: 缓存键的前缀
            host: Redis主机地址
            port: Redis端口
            db: Redis数据库编号
            password: Redis密码
            ttl_hours: 缓存过期时间（小时），默认48小时
        """
        self.cache_prefix = cache_prefix
        self.ttl_seconds = ttl_hours * 3600  # 转换为秒
        
        # 从环境变量读取 Redis 配置，如果没有则使用默认值
        self.host = host or os.getenv('REDIS_HOST')
        self.port = port or int(os.getenv('REDIS_PORT'))
        self.db = db if db is not None else int(os.getenv('REDIS_DB'))
        self.password = password or os.getenv('REDIS_PASSWORD')
        
        logger.info(f"Redis缓存配置: host={self.host}, port={self.port}, db={self.db}, ttl={ttl_hours}小时")
        
        try:
            self.redis_client = redis.Redis(
                host=self.host, 
                port=self.port, 
                db=self.db, 
                password=self.password,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            logger.info(f"Redis缓存客户端初始化完成，连接到 {self.host}:{self.port}, db={self.db}")
        except Exception as e:
            logger.error(f"Redis缓存客户端初始化失败: {str(e)}")
            self.redis_client = None

    def check_connection(self) -> bool:
        """检查Redis连接状态"""
        try:
            if self.redis_client:
                return self.redis_client.ping()
            return False
        except Exception as e:
            logger.error(f"Redis连接检查失败: {str(e)}")
            return False

    def _get_cache_key(self, key: str) -> str:
        """获取完整的缓存键名"""
        return f"{self.cache_prefix}:data:{key}"

    def _serialize_value(self, value: draft.Script_file) -> bytes:
        """序列化对象"""
        try:
            return pickle.dumps(value)
        except Exception as e:
            logger.error(f"序列化对象失败: {str(e)}")
            raise

    def _deserialize_value(self, data: bytes) -> draft.Script_file:
        """反序列化对象"""
        try:
            return pickle.loads(data)
        except Exception as e:
            logger.error(f"反序列化对象失败: {str(e)}")
            raise

    def update_cache(self, key: str, value: draft.Script_file) -> bool:
        """
        更新缓存（替代原有的 update_cache 函数）
        
        Args:
            key: 缓存键
            value: 要缓存的 draft.Script_file 对象
            
        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.check_connection():
                logger.error("Redis连接失败，无法更新缓存")
                return False
            
            cache_key = self._get_cache_key(key)
            serialized_value = self._serialize_value(value)
            
            # 设置缓存并添加过期时间
            success = self.redis_client.setex(cache_key, self.ttl_seconds, serialized_value)
            
            if success:
                logger.debug(f"成功更新Redis缓存: {key}, TTL: {self.ttl_seconds}秒")
                return True
            else:
                logger.error(f"更新Redis缓存失败: {key}")
                return False
            
        except Exception as e:
            logger.error(f"更新Redis缓存失败 {key}: {str(e)}")
            return False

    def get_cache(self, key: str) -> Optional[draft.Script_file]:
        """
        获取缓存
        
        Args:
            key: 缓存键
            
        Returns:
            draft.Script_file or None: 缓存的对象，如果不存在或出错则返回None
        """
        try:
            if not self.check_connection():
                logger.error("Redis连接失败，无法获取缓存")
                return None
            
            cache_key = self._get_cache_key(key)
            serialized_value = self.redis_client.get(cache_key)
            
            if serialized_value is None:
                logger.debug(f"缓存中不存在键: {key}")
                return None
            
            value = self._deserialize_value(serialized_value)
            logger.debug(f"成功从Redis缓存获取: {key}")
            return value
            
        except Exception as e:
            logger.error(f"从Redis缓存获取失败 {key}: {str(e)}")
            return None

    def delete_cache(self, key: str) -> bool:
        """
        删除缓存
        
        Args:
            key: 缓存键
            
        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.check_connection():
                logger.error("Redis连接失败，无法删除缓存")
                return False
            
            cache_key = self._get_cache_key(key)
            result = self.redis_client.delete(cache_key)
            
            success = result > 0  # 如果删除了至少一个键，认为成功
            if success:
                logger.debug(f"成功删除Redis缓存: {key}")
            else:
                logger.debug(f"缓存键不存在: {key}")
                
            return success
            
        except Exception as e:
            logger.error(f"删除Redis缓存失败 {key}: {str(e)}")
            return False

    def clear_all_cache(self) -> bool:
        """
        清空所有缓存
        
        Returns:
            bool: 操作是否成功
        """
        try:
            if not self.check_connection():
                logger.error("Redis连接失败，无法清空缓存")
                return False
            
            # 获取所有缓存键
            pattern = f"{self.cache_prefix}:data:*"
            cache_keys = self.redis_client.keys(pattern)
            
            if cache_keys:
                # 删除所有缓存数据
                deleted_count = self.redis_client.delete(*cache_keys)
                logger.info(f"成功清空所有Redis缓存，共删除 {deleted_count} 个缓存项")
            else:
                logger.info("没有找到需要清空的缓存项")
            
            return True
            
        except Exception as e:
            logger.error(f"清空Redis缓存失败: {str(e)}")
            return False

    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        Returns:
            Dict: 缓存统计信息
        """
        try:
            if not self.check_connection():
                return {"error": "Redis连接失败"}
            
            pattern = f"{self.cache_prefix}:data:*"
            cache_keys = self.redis_client.keys(pattern)
            cache_count = len(cache_keys)
            
            return {
                "cache_count": cache_count,
                "ttl_hours": self.ttl_seconds // 3600,
                "redis_connected": True
            }
            
        except Exception as e:
            logger.error(f"获取缓存信息失败: {str(e)}")
            return {"error": str(e), "redis_connected": False}


class RedisDict:
    """
    模拟字典行为的Redis缓存类，用于完全兼容原有的 DRAFT_CACHE 使用方式
    """
    def __init__(self, redis_cache_instance: RedisCache):
        self.redis_cache = redis_cache_instance
    
    def __contains__(self, key: str) -> bool:
        """支持 'key in cache' 语法"""
        try:
            result = self.redis_cache.get_cache(key)
            return result is not None
        except Exception as e:
            logger.error(f"检查键存在性失败 {key}: {str(e)}")
            return False
    
    def __getitem__(self, key: str) -> draft.Script_file:
        """支持 cache[key] 语法"""
        result = self.redis_cache.get_cache(key)
        if result is None:
            raise KeyError(f"缓存中不存在键: {key}")
        return result
    
    def __setitem__(self, key: str, value: draft.Script_file) -> None:
        """支持 cache[key] = value 语法"""
        success = self.redis_cache.update_cache(key, value)
        if not success:
            logger.warning(f"设置缓存失败，键: {key}")
    
    def __delitem__(self, key: str) -> None:
        """支持 del cache[key] 语法"""
        success = self.redis_cache.delete_cache(key)
        if not success:
            # 即使删除失败也不抛出异常，保持与原有行为一致
            logger.warning(f"删除缓存失败，键: {key}")
    
    def get(self, key: str, default=None):
        """获取值，如果不存在返回默认值"""
        result = self.redis_cache.get_cache(key)
        return result if result is not None else default
    
    def pop(self, key: str, default=None):
        """弹出并删除值"""
        result = self.redis_cache.get_cache(key)
        if result is not None:
            self.redis_cache.delete_cache(key)
            return result
        return default
    
    def clear(self) -> None:
        """清空所有缓存"""
        self.redis_cache.clear_all_cache()
    
    def __len__(self) -> int:
        """获取缓存数量"""
        info = self.redis_cache.get_cache_info()
        return info.get("cache_count", 0)


# 创建全局Redis缓存实例
redis_cache = RedisCache()

# 创建字典式接口，完全兼容原有的 DRAFT_CACHE 使用方式
DRAFT_CACHE = RedisDict(redis_cache)

# 为了保持向后兼容，提供与原来 draft_cache.py 相同的接口
def update_cache(key: str, value: draft.Script_file) -> None:
    """向后兼容的更新缓存函数"""
    success = redis_cache.update_cache(key, value)
    if not success:
        logger.warning(f"更新Redis缓存失败，键: {key}")

def get_cache(key: str) -> Optional[draft.Script_file]:
    """获取缓存的便捷函数"""
    return redis_cache.get_cache(key)

def delete_cache(key: str) -> bool:
    """删除缓存的便捷函数"""
    return redis_cache.delete_cache(key)

def clear_all_cache() -> bool:
    """清空所有缓存的便捷函数"""
    return redis_cache.clear_all_cache()

def get_cache_info() -> Dict[str, Any]:
    """获取缓存信息的便捷函数"""
    return redis_cache.get_cache_info() 