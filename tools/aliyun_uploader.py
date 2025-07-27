import os
import time
import logging
from typing import Optional, Tuple, Dict, Any
import oss2

# 尝试加载.env文件
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 配置日志
logger = logging.getLogger(__name__)

class AliyunUploader:
    """阿里云OSS上传工具类"""
    
    def __init__(
        self, 
        access_key_id: Optional[str] = None, 
        access_key_secret: Optional[str] = None,
        bucket_name: Optional[str] = None,
        endpoint: Optional[str] = None,
        region: Optional[str] = None
    ):
        """
        初始化阿里云OSS上传工具类
        
        Args:
            access_key_id: 阿里云访问密钥ID，默认从环境变量获取
            access_key_secret: 阿里云访问密钥密码，默认从环境变量获取
            bucket_name: 阿里云存储空间名称，默认从环境变量获取
            endpoint: 阿里云OSS端点，默认从环境变量获取
            region: 阿里云区域，默认从环境变量获取
        """
        # 优先使用传入的参数，否则从环境变量获取
        self.access_key_id = access_key_id or os.getenv('ALIYUN_ACCESS_KEY_ID')
        self.access_key_secret = access_key_secret or os.getenv('ALIYUN_ACCESS_KEY_SECRET')
        self.bucket_name = bucket_name or os.getenv('ALIYUN_OSS_BUCKET')
        self.endpoint = endpoint or os.getenv('ALIYUN_OSS_ENDPOINT')
        self.region = region or os.getenv('ALIYUN_OSS_REGION')
        
        if not all([self.access_key_id, self.access_key_secret, self.bucket_name, self.endpoint]):
            raise ValueError("Missing required Aliyun OSS configuration")
        
        # 创建认证对象
        self.auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        
        # 获取外部端点用于创建Bucket对象
        # 检查是否是内网端点，如果是则转换为外网端点
        self.external_endpoint = self._get_external_endpoint_with_protocol()
        logger.info(f"使用OSS端点: {self.external_endpoint} (原始端点: {self.endpoint})")
        
        # 创建Bucket对象使用外部端点，确保可以从任何网络访问
        self.bucket = oss2.Bucket(self.auth, self.external_endpoint, self.bucket_name)
    
    def _get_external_endpoint(self) -> str:
        """
        获取外网访问端点，如果是内网端点则转换为外网端点
        
        Returns:
            str: 外网访问端点，不包含协议
        """
        endpoint = self.endpoint.replace('https://', '').replace('http://', '')
        # 如果包含-internal，则移除以获得外网地址
        if '-internal' in endpoint:
            endpoint = endpoint.replace('-internal', '')
        return endpoint
        
    def _get_external_endpoint_with_protocol(self) -> str:
        """
        获取带协议的外网访问端点
        
        Returns:
            str: 带协议的外网访问端点
        """
        # 保留原始协议
        protocol = 'https://' if 'https://' in self.endpoint else 'http://'
        return protocol + self._get_external_endpoint()
    
    def upload_file(
        self, 
        file_path: str, 
        key_prefix: str = "uploads",
        internal_or_external: Optional[str] = "external",
        file_key: Optional[str] = None
    ) -> Tuple[bool, str, Dict[Any, Any]]:
        """
        上传文件到阿里云OSS
        
        Args:
            file_path: 本地文件路径
            key_prefix: 文件在OSS中的前缀路径，默认为 "uploads"
            file_key: 自定义文件名，不提供则使用时间戳+原文件名
            
        Returns:
            Tuple[bool, str, Dict]: (是否成功, URL或错误信息, 详细结果信息)
        """
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
            
            # 上传文件
            logger.info(f"开始上传文件到阿里云OSS: {file_path} -> {file_key}")
            result = self.bucket.put_object_from_file(file_key, file_path)
            
            # 检查上传是否成功
            if result.status == 200:
                # 构建访问URL（始终使用外网地址）
                if internal_or_external == "external":
                    external_endpoint = self._get_external_endpoint()
                    url = f"https://{self.bucket_name}.{external_endpoint}/{file_key}"
                else:
                    url = f"oss://{self.bucket_name}.oss-cn-beijing.aliyuncs.com/{file_key}"
                logger.info(f"文件上传成功: {url}")
                return True, url, {
                    'key': file_key,
                    'etag': result.etag,
                    'request_id': result.request_id
                }
            else:
                error_msg = f"上传失败: HTTP状态码 {result.status}"
                logger.error(error_msg)
                return False, error_msg, {}
                
        except Exception as e:
            error_msg = f"上传文件失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg, {}
        

    #      
    
    def upload_video(self, file_path: str) -> Tuple[bool, str, Dict[Any, Any]]:
        """
        上传视频文件到阿里云OSS（使用videos前缀）
        
        Args:
            file_path: 本地视频文件路径
            
        Returns:
            Tuple[bool, str, Dict]: (是否成功, URL或错误信息, 详细结果信息)
        """
        return self.upload_file(file_path, key_prefix="videos")

    def get_upload_token(self) -> Dict[str, Any]:
        """
        获取上传凭证（为了保持与其他上传器的接口一致性）
        注意：阿里云OSS通常使用STS临时凭证，这里返回基本配置信息
        
        Returns:
            Dict[str, Any]: 包含上传配置信息的字典
        """
        return {
            'bucket': self.bucket_name,
            'endpoint': self._get_external_endpoint(),
            'region': self.region,
            'access_key_id': self.access_key_id,
            # 注意：在生产环境中不应该返回access_key_secret
            'expires': int(time.time()) + 3600  # 1小时后过期
        }

def main():
    """测试上传视频文件"""
    file_path = "/Users/lishuqing/Downloads/1752911484_12fz9i.mp4"
    
    # 创建上传器实例
    uploader = AliyunUploader()
    
    print(f"🎬 开始测试上传文件: {file_path}")
    
    # 上传视频文件
    success, result, info = uploader.upload_video(file_path)
    
    if success:
        print(f"✅ 阿里云OSS上传成功!")
        print(f"📺 视频URL: {result}")
        print(f"📊 详细信息: {info}")
    else:
        print(f"❌ 阿里云OSS上传失败: {result}")
        print(f"📊 错误信息: {info}")

if __name__ == "__main__":
    main() 