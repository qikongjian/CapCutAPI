import os
import time
import logging
import requests
from typing import Optional, Tuple, Dict, Any
from qiniu import Auth, put_file, etag, put_data
import qiniu

# 配置日志
logger = logging.getLogger(__name__)
class QiniuUploader:
    """七牛云上传工具类"""
    
    def __init__(
        self, 
        access_key: Optional[str] = None, 
        secret_key: Optional[str] = None,
        bucket_name: Optional[str] = None,
        domain: Optional[str] = None
    ):
        """
        初始化七牛云上传工具类
        
        Args:
            access_key: 七牛云访问密钥，默认从环境变量获取
            secret_key: 七牛云私有密钥，默认从环境变量获取
            bucket_name: 七牛云存储空间名称，默认从环境变量获取
            domain: 七牛云绑定的域名，默认从环境变量获取
        """
        # 优先使用传入的参数，否则从环境变量获取，最后使用默认值
        self.access_key = access_key or os.getenv('QINIU_ACCESS_KEY')
        self.secret_key = secret_key or os.getenv('QINIU_SECRET_KEY')
        self.bucket_name = bucket_name or os.getenv('QINIU_BUCKET_NAME')
        self.domain = domain or os.getenv('QINIU_DOMAIN')
        
        # 创建鉴权对象
        self.auth = Auth(self.access_key, self.secret_key)
    
    def upload_file(
        self, 
        file_path: str, 
        key_prefix: str = "uploads",
        file_key: Optional[str] = None
    ) -> Tuple[bool, str, Dict[Any, Any]]:
        """
        上传文件到七牛云
        
        Args:
            file_path: 本地文件路径或URL
            key_prefix: 文件在七牛云中的前缀路径，默认为 "uploads"
            file_key: 自定义文件名，不提供则使用时间戳+原文件名
            
        Returns:
            Tuple[bool, str, Dict]: (是否成功, URL或错误信息, 详细结果信息)
        """
        # 判断是否为URL
        is_url = file_path.startswith(('http://', 'https://'))
        
        if is_url:
            return self._upload_from_url(file_path, key_prefix, file_key)
        else:
            return self._upload_local_file(file_path, key_prefix, file_key)
    
    def _upload_local_file(
        self, 
        file_path: str, 
        key_prefix: str = "uploads",
        file_key: Optional[str] = None
    ) -> Tuple[bool, str, Dict[Any, Any]]:
        """上传本地文件到七牛云"""
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return False, f"文件不存在: {file_path}", {}
        
        try:
            # 生成上传文件的key（文件名）
            if not file_key:
                file_name = os.path.basename(file_path)
                file_key = f"{key_prefix}/{int(time.time())}_{file_name}"
            else:
                file_key = f"{key_prefix}/{file_key}"
            
            # 生成上传 Token
            token = self.auth.upload_token(self.bucket_name, file_key)
            
            # 上传文件
            logger.info(f"开始上传本地文件到七牛云: {file_path} -> {file_key}")
            ret, info = put_file(token, file_key, file_path)
            
            # 检查上传是否成功
            if ret and ret['key'] == file_key:
                # 构建访问URL
                url = f"https://{self.domain}/{file_key}"
                logger.info(f"文件上传成功: {url}")
                return True, url, ret
            else:
                logger.error(f"文件上传失败: {info}")
                return False, f"上传失败: {info}", {}
                
        except Exception as e:
            error_msg = f"上传文件失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg, {}
    
    def _upload_from_url(
        self, 
        url: str, 
        key_prefix: str = "uploads",
        file_key: Optional[str] = None
    ) -> Tuple[bool, str, Dict[Any, Any]]:
        """从URL下载文件并上传到七牛云"""
        try:
            # 下载文件内容
            logger.info(f"从URL下载文件: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # 获取文件数据
            file_data = response.content
            
            # 从URL中提取文件名
            if not file_key:
                file_name = url.split('/')[-1].split('?')[0]  # 移除查询参数
                if not file_name or '.' not in file_name:
                    file_name = f"image_{int(time.time())}.png"  # 默认文件名
                file_key = f"{key_prefix}/{int(time.time())}_{file_name}"
            else:
                file_key = f"{key_prefix}/{file_key}"
            
            # 生成上传 Token
            token = self.auth.upload_token(self.bucket_name, file_key)
            
            # 上传文件
            logger.info(f"开始上传URL文件到七牛云: {url} -> {file_key}")
            ret, info = put_data(token, file_key, file_data)
            
            # 检查上传是否成功
            if ret and ret['key'] == file_key:
                # 构建访问URL
                result_url = f"https://{self.domain}/{file_key}"
                logger.info(f"URL文件上传成功: {result_url}")
                return True, result_url, ret
            else:
                logger.error(f"URL文件上传失败: {info}")
                return False, f"上传失败: {info}", {}
                
        except Exception as e:
            error_msg = f"从URL上传文件失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg, {}
    
    def upload_video(self, file_path: str) -> Tuple[bool, str, Dict[Any, Any]]:
        """
        上传视频文件到七牛云（使用videos前缀）
        
        Args:
            file_path: 本地视频文件路径
            
        Returns:
            Tuple[bool, str, Dict]: (是否成功, URL或错误信息, 详细结果信息)
        """
        return self.upload_file(file_path, key_prefix="videos") 
    
    def get_upload_token(self) -> str:
        """
        获取七牛云上传token
        
        Returns:
            str: 七牛云上传token，失败时返回None
        """
        try:
            token = self.auth.upload_token(self.bucket_name)
            return token
        except Exception as e:
            logger.error(f"获取七牛云上传token失败: {e}")
            return None

def main():
    """测试上传视频文件"""
    file_path = "/Users/lishuqing/Downloads/1752911484_12fz9i.mp4"
    
    # 创建上传器实例
    uploader = QiniuUploader()
    
    print(f"🎬 开始测试上传文件: {file_path}")
    
    # 上传视频文件
    success, result, info = uploader.upload_video(file_path)
    
    if success:
        print(f"✅ 七牛云上传成功!")
        print(f"📺 视频URL: {result}")
        print(f"📊 详细信息: {info}")
    else:
        print(f"❌ 七牛云上传失败: {result}")
        print(f"📊 错误信息: {info}")

if __name__ == "__main__":
    main() 