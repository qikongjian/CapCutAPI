import os
import time
import tempfile
import threading
import logging
import random
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Optional, Callable

# 尝试加载.env文件
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 处理相对导入问题
try:
    from .qiniu_uploader import QiniuUploader
    from .aliyun_uploader import AliyunUploader
except ImportError:
    from qiniu_uploader import QiniuUploader
    from aliyun_uploader import AliyunUploader

logger = logging.getLogger(__name__)

class UploadManager:
    """多线程上传管理器，支持七牛云和阿里云"""
    
    def __init__(self, max_workers: int = 3):
        """
        初始化上传管理器
        
        Args:
            max_workers: 最大并发上传线程数
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.qiniu_uploader = QiniuUploader()
        self.aliyun_uploader = AliyunUploader()
    
    def upload_async(
        self, 
        data: bytes, 
        file_extension: str = "mp4",
        on_success: Optional[Callable[[str], None]] = None,
        on_failure: Optional[Callable[[str], None]] = None
    ) -> Future:
        """
        异步上传文件（多线程）
        
        Args:
            data: 文件二进制数据
            file_extension: 文件扩展名
            on_success: 成功回调函数，参数为文件URL
            on_failure: 失败回调函数，参数为错误信息
            
        Returns:
            Future对象，可以用来获取上传结果
        """
        return self.executor.submit(
            self._upload_with_fallback, 
            data, 
            file_extension, 
            on_success, 
            on_failure
        )
    
    def upload_sync(self, data: bytes, file_extension: str = "mp4") -> str:
        """
        同步上传文件（兼容原有接口）
        
        Args:
            data: 文件二进制数据
            file_extension: 文件扩展名
            
        Returns:
            文件URL，失败时返回空字符串
        """
        return self._upload_with_fallback(data, file_extension)
    
    def _upload_with_fallback(
        self, 
        data: bytes, 
        file_extension: str,
        on_success: Optional[Callable[[str], None]] = None,
        on_failure: Optional[Callable[[str], None]] = None
    ) -> str:
        """
        带回退机制的上传方法：优先七牛云，失败时使用阿里云
        """
        file_size_mb = len(data) / 1024 / 1024
        logger.info(f"开始上传文件，大小: {file_size_mb:.2f} MB")
        print(f"📤 开始上传文件 ({file_size_mb:.2f} MB)")
        
        # 创建临时文件
        temp_file_path = None
        try:
            # 生成临时文件路径
            temp_file_path = os.path.join(
                tempfile.gettempdir(), 
                f"{int(time.time())}_{random.randint(1000, 9999)}.{file_extension}"
            )
            
            # 写入临时文件
            with open(temp_file_path, 'wb') as f:
                f.write(data)
            
            # 优先尝试七牛云上传
            success, result, info = self.qiniu_uploader.upload_file(temp_file_path)
            
            if success:
                logger.info(f"七牛云上传成功: {result}")
                print(f"🎬 七牛云上传成功: {result}")
                if on_success:
                    on_success(result)
                return result
            else:
                logger.warning(f"七牛云上传失败: {result}")
                print(f"⚠️ 七牛云上传失败，尝试阿里云: {result}")
            
            # 七牛云失败，尝试阿里云
            success, result, info = self.aliyun_uploader.upload_file(temp_file_path)
            
            if success:
                logger.info(f"阿里云上传成功: {result}")
                print(f"☁️ 阿里云上传成功: {result}")
                if on_success:
                    on_success(result)
                return result
            else:
                error_msg = f"所有上传方式都失败了: 七牛云和阿里云都上传失败"
                logger.error(error_msg)
                print(f"❌ {error_msg}")
                if on_failure:
                    on_failure(error_msg)
                return ""
                
        except Exception as e:
            error_msg = f"上传过程中发生异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            print(f"❌ {error_msg}")
            if on_failure:
                on_failure(error_msg)
            return ""
        finally:
            # 清理临时文件
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception as e:
                    logger.warning(f"清理临时文件失败: {e}")
    
    def shutdown(self):
        """关闭上传管理器"""
        self.executor.shutdown(wait=True)

# 全局上传管理器实例
_upload_manager = UploadManager()

def upload_to_qiniu(data: bytes, file_extension: str = "mp4", timeout: int = 300) -> str:
    """
    兼容原有接口的上传函数
    
    Args:
        data: 文件二进制数据
        file_extension: 文件扩展名
        timeout: 超时时间（保留参数兼容性，实际不使用）
        
    Returns:
        文件URL，失败时返回空字符串
    """
    return _upload_manager.upload_sync(data, file_extension)

def upload_async(
    data: bytes, 
    file_extension: str = "mp4",
    on_success: Optional[Callable[[str], None]] = None,
    on_failure: Optional[Callable[[str], None]] = None
) -> Future:
    """
    异步上传文件
    
    Args:
        data: 文件二进制数据
        file_extension: 文件扩展名
        on_success: 成功回调函数
        on_failure: 失败回调函数
        
    Returns:
        Future对象
    """
    return _upload_manager.upload_async(data, file_extension, on_success, on_failure)

def main():
    """测试上传管理器"""
    test_file_path = "/Users/lishuqing/Downloads/1752911484_12fz9i.mp4"
    
    if not os.path.exists(test_file_path):
        print(f"❌ 测试文件不存在: {test_file_path}")
        return
    
    print(f"🧪 开始测试新的上传管理器")
    
    # 读取测试文件
    with open(test_file_path, 'rb') as f:
        video_data = f.read()
    
    print(f"📁 文件大小: {len(video_data) / 1024 / 1024:.2f} MB")
    
    # 测试同步上传
    print("\n--- 测试同步上传 ---")
    url = upload_to_qiniu(video_data, "mp4")
    
    if url:
        print(f"✅ 同步上传成功: {url}")
    else:
        print(f"❌ 同步上传失败")
    
    # 测试异步上传
    print("\n--- 测试异步上传 ---")
    
    def success_callback(url):
        print(f"✅ 异步上传成功: {url}")
    
    def failure_callback(error):
        print(f"❌ 异步上传失败: {error}")
    
    future = upload_async(
        video_data, 
        "mp4",
        on_success=success_callback,
        on_failure=failure_callback
    )
    
    # 等待异步上传完成
    result = future.result()
    print(f"🔄 异步上传结果: {result}")
    
    # 关闭上传管理器
    _upload_manager.shutdown()
    print("\n🏁 测试完成")

if __name__ == "__main__":
    main() 