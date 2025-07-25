"""进程控制器，用于管理剪映进程"""

import os
import subprocess
import psutil
import logging

logger = logging.getLogger('flask_video_generator')

class ProcessController:
    """剪映进程控制器"""
    
    @staticmethod
    def kill_jianying():
        """杀死剪映进程"""
        try:
            # 在Windows上查找剪映进程
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if 'jianying' in proc.info['name'].lower() or 'capcut' in proc.info['name'].lower():
                        proc.terminate()
                        logger.info(f"已终止剪映进程: {proc.info['name']} (PID: {proc.info['pid']})")
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception as e:
            logger.error(f"终止剪映进程时出错: {e}")
    
    @staticmethod
    def restart_jianying():
        """重启剪映程序"""
        try:
            # 这里需要根据实际的剪映安装路径进行调整
            # Windows 默认路径
            possible_paths = [
                r"C:\Program Files\CapCut\CapCut.exe",
                r"C:\Program Files (x86)\CapCut\CapCut.exe",
                # 可以添加更多可能的路径
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    subprocess.Popen([path])
                    logger.info(f"已启动剪映: {path}")
                    return True
            
            logger.error("未找到剪映程序，请检查安装路径")
            return False
        except Exception as e:
            logger.error(f"启动剪映时出错: {e}")
            return False
    
    @staticmethod
    def kill_jianying_detector():
        """杀死剪映检测器进程（如果有的话）"""
        try:
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if 'detector' in proc.info['name'].lower():
                        proc.terminate()
                        logger.info(f"已终止检测器进程: {proc.info['name']} (PID: {proc.info['pid']})")
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception as e:
            logger.error(f"终止检测器进程时出错: {e}") 