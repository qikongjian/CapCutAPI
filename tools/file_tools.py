import os
import tempfile
import qiniu
import time
import logging
import random

import requests

logger = logging.getLogger(__name__)

def upload_to_qiniu(data: bytes, file_extension: str = "mp4") -> str:
    """
    上传文件到七牛云
    Args:
        data: 文件二进制数据
        file_extension: 文件扩展名 (如 mp4, png, jpg 等)
    Returns:
        文件URL
    """
    try:
        q = qiniu.Auth(os.getenv('QINIU_ACCESS_KEY', 'Ef8cxF6Hg01m6wuLpMpUgICXcztrdsXKTJzjeoro'), os.getenv('QINIU_SECRET_KEY', '-VcHBrdszBch8hBKXw4itiF-dpCIcAc91LCb_pn3'))
        token = q.upload_token(os.getenv('QINIU_BUCKET_NAME', 'risingfalling'))
        
        # 生成唯一文件名，包含时间戳和随机数
        filename = f"{int(time.time())}_{random.randint(1000, 9999)}.{file_extension}"
        
        ret, info = qiniu.put_data(token, filename, data)
        if ret is None:
            logger.error(f"上传到七牛云失败: {info}")
            return ""
        domain = os.getenv('QINIU_DOMAIN', 'cdn.qikongjian.com')
        url = f"https://{domain}/{filename}"
        logger.info(f"上传到七牛云成功: {url}")
        # 同时打印到控制台，确保可见
        print(f"🎬 文件上传到七牛云成功: {url}")
        return url
    except Exception as e:
        logger.error(f"上传到七牛云时出错: {str(e)}")
        print(f"❌ 上传到七牛云失败: {str(e)}")
        return ""
    
def download_file_to_temp(url) -> str:
    #从url下载文件到临时文件夹
    file_path = os.path.join(tempfile.gettempdir(), os.path.basename(url))
    response = requests.get(url)
    with open(file_path, "wb") as f:
        f.write(response.content)
    return file_path