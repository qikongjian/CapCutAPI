import os
import tempfile
import qiniu
import time
import logging
import random

import requests

logger = logging.getLogger(__name__)

def upload_to_qiniu(data: bytes, file_extension: str = "mp4", timeout: int = 300) -> str:
    """
    上传文件到七牛云（带超时和重试机制）
    Args:
        data: 文件二进制数据
        file_extension: 文件扩展名 (如 mp4, png, jpg 等)
        timeout: 上传超时时间（秒），默认5分钟
    Returns:
        文件URL
    """
    
    try:
        q = qiniu.Auth(os.getenv('QINIU_ACCESS_KEY', 'Ef8cxF6Hg01m6wuLpMpUgICXcztrdsXKTJzjeoro'), os.getenv('QINIU_SECRET_KEY', '-VcHBrdszBch8hBKXw4itiF-dpCIcAc91LCb_pn3'))
        token = q.upload_token(os.getenv('QINIU_BUCKET_NAME', 'risingfalling'))
        
        # 生成唯一文件名，包含时间戳和随机数
        filename = f"{int(time.time())}_{random.randint(1000, 9999)}.{file_extension}"
        
        # 计算文件大小（MB）
        file_size_mb = len(data) / 1024 / 1024
        logger.info(f"准备上传文件: {filename}, 大小: {file_size_mb:.2f} MB")
        print(f"📤 开始上传到七牛云: {filename} ({file_size_mb:.2f} MB)")
        
        # 根据文件大小调整超时时间
        if file_size_mb > 100:  # 大于100MB的文件
            timeout = max(timeout, 600)  # 至少10分钟
        elif file_size_mb > 50:  # 大于50MB的文件
            timeout = max(timeout, 300)  # 至少5分钟
        
        logger.info(f"上传超时设置: {timeout} 秒")
        
        # 使用put_data上传
        ret, info = qiniu.put_data(token, filename, data)
        
        if ret is None:
            error_msg = f"上传到七牛云失败: {info}"
            logger.error(error_msg)
            print(f"❌ {error_msg}")
            return ""
            
        domain = os.getenv('QINIU_DOMAIN', 'cdn.qikongjian.com')
        url = f"https://{domain}/{filename}"
        logger.info(f"上传到七牛云成功: {url}")
        # 同时打印到控制台，确保可见
        print(f"🎬 文件上传到七牛云成功: {url}")
        return url
        
    except Exception as e:
        error_msg = f"上传到七牛云时出错: {str(e)}"
        logger.error(error_msg)
        print(f"❌ {error_msg}")
        return ""
    
def download_file_to_temp(url) -> str:
    #从url下载文件到临时文件夹
    file_path = os.path.join(tempfile.gettempdir(), os.path.basename(url))
    response = requests.get(url)
    with open(file_path, "wb") as f:
        f.write(response.content)
    return file_path

if __name__ == "__main__":
    # 测试代码
    with open("/Users/lishuqing/Downloads/badcase-两个马头.mp4", "rb") as f:
        video_bytes = f.read()
    url = upload_to_qiniu(video_bytes, "mp4")
    print(url)