import json
import time
import logging
import threading
import os
import platform
from typing import Dict, Any, Optional
from tools.redis_cache import redis_cache, DRAFT_CACHE
from export_progress_cache import export_progress_cache
# 导入save_draft_impl
from save_draft_impl import save_draft_impl
# 平台检测和条件性导入
IS_WINDOWS = platform.system() == 'Windows'

if IS_WINDOWS:
    try:
        from pyJianYingDraft.jianying_controller import Jianying_controller, Export_resolution, Export_framerate
    except ImportError as e:
        print(f"Warning: Could not import Jianying_controller on Windows: {e}")
        IS_WINDOWS = False

logger = logging.getLogger(__name__)

class ExportQueueManager:
    """导出任务队列管理器"""
    
    def __init__(self):
        self.queue_key = "export_task_queue"
        self.processing_key = "export_processing"
        self.result_key = "export_result"
        self.local_task_lock = threading.Lock()
        self.is_processing_local = False
        self.current_local_task = None
        
    def submit_export_task(self, task_data: Dict[str, Any]) -> str:
        """
        提交导出任务到Redis队列
        
        Args:
            task_data: 任务数据，包含draft_id, draft_name, resolution, framerate等
            
        Returns:
            str: 任务ID
        """
        try:
            draft_id = task_data.get('draft_id')
            if not draft_id:
                raise Exception("Missing required parameter: draft_id")
            
            # 检查草稿是否存在于缓存中
            if draft_id not in DRAFT_CACHE:
                raise Exception(f"Draft {draft_id} does not exist in cache. Please create or save the draft first.")
            
            logger.info(f"草稿 {draft_id} 存在于缓存中，准备提交导出任务")
                
            task_data['created_at'] = time.time()
            task_data['status'] = 'queued'
            
            # 将任务推入Redis队列
            task_json = json.dumps(task_data)
            success = redis_cache.redis_client.lpush(self.queue_key, task_json)
            
            if success:
                logger.info(f"成功提交导出任务到队列: {draft_id}")
                
                # 初始化任务状态
                export_progress_cache.set_progress(draft_id, {
                    "status": "queued",
                    "percent": 0.0,
                    "message": "任务已加入队列等待处理",
                    "draft_id": draft_id,
                    "start_time": time.time(),
                    "elapsed": 0
                })
                
                return draft_id
            else:
                raise Exception("Failed to push task to Redis queue")
                
        except Exception as e:
            logger.error(f"提交导出任务失败: {str(e)}")
            raise
    
    def get_next_task(self) -> Optional[Dict[str, Any]]:
        """
        从Redis队列获取下一个任务
        
        Returns:
            Dict: 任务数据，如果没有任务则返回None
        """
        try:
            # 使用阻塞弹出，超时2秒
            result = redis_cache.redis_client.brpop([self.queue_key], timeout=2)
            
            if result:
                queue_name, task_json = result
                task_data = json.loads(task_json.decode('utf-8'))
                
                # 标记任务为处理中
                task_data['status'] = 'processing'
                task_data['processing_started_at'] = time.time()
                
                # 将任务移动到处理中列表
                redis_cache.redis_client.hset(
                    self.processing_key, 
                    task_data['draft_id'], 
                    json.dumps(task_data)
                )
                
                logger.info(f"获取到导出任务: {task_data['draft_id']}")
                return task_data
            
            return None
            
        except Exception as e:
            logger.error(f"获取导出任务失败: {str(e)}")
            return None
    
    def requeue_task(self, task_data: Dict[str, Any], reason: str = ""):
        """
        将任务重新放回队列
        
        Args:
            task_data: 任务数据
            reason: 重新入队的原因
        """
        try:
            draft_id = task_data.get('draft_id')
            
            # 从处理中列表移除
            redis_cache.redis_client.hdel(self.processing_key, draft_id)
            
            # 重置任务状态
            task_data['status'] = 'queued'
            task_data['requeued_at'] = time.time()
            task_data['requeue_reason'] = reason
            
            # 增加重试次数
            retry_count = task_data.get('retry_count', 0) + 1
            task_data['retry_count'] = retry_count
            
            # 如果重试次数过多，标记为失败
            if retry_count > 3:
                logger.error(f"任务 {draft_id} 重试次数过多 ({retry_count})，标记为失败")
                self.complete_task(draft_id, False, {
                    "error": f"重试次数过多 ({retry_count}): {reason}",
                    "retry_count": retry_count
                })
                return
            
            # 重新推入队列
            task_json = json.dumps(task_data)
            success = redis_cache.redis_client.lpush(self.queue_key, task_json)
            
            if success:
                logger.info(f"任务 {draft_id} 已重新加入队列 (重试次数: {retry_count}): {reason}")
                
                # 更新进度状态
                export_progress_cache.set_progress(draft_id, {
                    "status": "queued",
                    "percent": 0.0,
                    "message": f"任务已重新加入队列 (重试 {retry_count}/3): {reason}",
                    "draft_id": draft_id,
                    "start_time": time.time(),
                    "elapsed": 0,
                    "retry_count": retry_count
                })
            else:
                logger.error(f"重新入队失败: {draft_id}")
                
        except Exception as e:
            logger.error(f"重新入队任务失败: {str(e)}")
    
    def complete_task(self, draft_id: str, success: bool, result_data: Dict[str, Any] = None):
        """
        标记任务完成
        
        Args:
            draft_id: 草稿ID
            success: 是否成功
            result_data: 结果数据
        """
        try:
            # 从处理中列表移除
            redis_cache.redis_client.hdel(self.processing_key, draft_id)
            
            # 记录结果
            result = {
                "draft_id": draft_id,
                "success": success,
                "completed_at": time.time(),
                "result_data": result_data or {}
            }
            
            # 结果保存24小时
            redis_cache.redis_client.setex(
                f"{self.result_key}:{draft_id}", 
                86400,  # 24小时
                json.dumps(result)
            )
            
            logger.info(f"任务完成: {draft_id}, 成功: {success}")
            
        except Exception as e:
            logger.error(f"标记任务完成失败: {str(e)}")
    
    def is_local_task_running(self) -> bool:
        """检查本地是否有导出任务正在进行"""
        with self.local_task_lock:
            return self.is_processing_local
    
    def set_local_task_running(self, task_id: str = None, running: bool = True):
        """设置本地任务运行状态"""
        with self.local_task_lock:
            self.is_processing_local = running
            self.current_local_task = task_id if running else None
            
        if running:
            logger.info(f"开始本地导出任务: {task_id}")
        else:
            logger.info(f"本地导出任务完成: {task_id}")
    
    def get_queue_info(self) -> Dict[str, Any]:
        """获取队列信息"""
        try:
            queue_length = redis_cache.redis_client.llen(self.queue_key)
            processing_count = redis_cache.redis_client.hlen(self.processing_key)
            
            return {
                "queue_length": queue_length,
                "processing_count": processing_count,
                "is_local_running": self.is_local_task_running(),
                "current_local_task": self.current_local_task
            }
        except Exception as e:
            logger.error(f"获取队列信息失败: {str(e)}")
            return {"error": str(e)}

# 全局导出队列管理器实例
export_queue_manager = ExportQueueManager()


class ExportTaskProcessor:
    """导出任务处理器"""
    
    def __init__(self, queue_manager: ExportQueueManager):
        self.queue_manager = queue_manager
        self.is_running = False
        self.processor_thread = None
        
        # 平台检测
        self.is_windows = platform.system() == 'Windows'
        
        # 动态导入剪映控制器（仅Windows）
        self.jianying_controller_class = None
        self.export_resolution_class = None
        self.export_framerate_class = None
        
        if self.is_windows:
            try:
                self.jianying_controller_class = Jianying_controller
                self.export_resolution_class = Export_resolution
                self.export_framerate_class = Export_framerate
            except ImportError as e:
                logger.warning(f"Windows平台但无法导入剪映控制器: {e}")
                self.is_windows = False
    
    def start_processor(self):
        """启动任务处理器"""
        if self.is_running:
            logger.warning("任务处理器已经在运行")
            return
        
        # 检查系统是否为Windows，如果不是则不启动处理器
        if not self.is_windows:
            logger.info("非Windows系统，跳过启动导出任务处理器")
            return
            
        self.is_running = True
        self.processor_thread = threading.Thread(target=self._process_loop, daemon=True)
        self.processor_thread.start()
        logger.info("导出任务处理器已启动")
    
    def stop_processor(self):
        """停止任务处理器"""
        self.is_running = False
        if self.processor_thread:
            self.processor_thread.join(timeout=5)
        logger.info("导出任务处理器已停止")
    
    def _process_loop(self):
        """任务处理循环"""
        while self.is_running:
            try:
                # 检查本地是否有任务在进行
                if self.queue_manager.is_local_task_running():
                    time.sleep(2)  # 等待2秒再检查
                    continue
                
                # 获取下一个任务
                task_data = self.queue_manager.get_next_task()
                if not task_data:
                    continue  # 没有任务，继续循环
                
                # 处理任务
                self._process_task(task_data)
                
            except Exception as e:
                logger.error(f"任务处理循环出错: {str(e)}")
                time.sleep(5)  # 出错时等待5秒
    
    def _process_task(self, task_data: Dict[str, Any]):
        """
        处理单个导出任务
        
        Args:
            task_data: 任务数据
        """
        draft_id = task_data.get('draft_id')
        
        try:
            # 标记本地任务开始
            self.queue_manager.set_local_task_running(draft_id, True)
            
            # 更新进度状态
            export_progress_cache.set_progress(draft_id, {
                "status": "processing",
                "percent": 0.0,
                "message": "开始处理导出任务",
                "draft_id": draft_id,
                "start_time": time.time(),
                "elapsed": 0
            })
            
            # 步骤1：执行save_draft逻辑
            logger.info(f"开始执行save_draft逻辑: {draft_id}")
            save_result = save_draft_impl(draft_id)
            logger.info(f"Save draft完成: {draft_id}")

            if not save_result.get('success', False):
                raise Exception(f"Save draft失败: {save_result.get('error', 'Unknown error')}")
            
            # 更新进度
            export_progress_cache.set_progress(draft_id, {
                "status": "processing",
                "percent": 50.0,
                "message": "草稿保存完成，开始导出视频",
                "draft_id": draft_id,
                "start_time": time.time(),
                "elapsed": 0
            })
            
            # 步骤2：执行export_draft逻辑
            logger.info(f"开始执行export_draft逻辑: {draft_id}")
            export_result = self._execute_export_draft(task_data)
            
            if not export_result.get('success', False):
                raise Exception(f"Export draft失败: {export_result.get('error', 'Unknown error')}")
            
            # 任务成功完成
            self.queue_manager.complete_task(draft_id, True, {
                "save_result": save_result,
                "export_result": export_result
            })
            
        except Exception as e:
            logger.error(f"处理导出任务失败 {draft_id}: {str(e)}")
            
            # 更新失败状态
            export_progress_cache.set_progress(draft_id, {
                "status": "failed",
                "percent": 0.0,
                "message": f"导出任务失败: {str(e)}",
                "draft_id": draft_id,
                "start_time": time.time(),
                "elapsed": 0,
                "error": str(e)
            })
            
            # 重新入队
            self.queue_manager.requeue_task(task_data, f"任务执行失败，尝试重新处理: {str(e)}")
        finally:
            # 清除本地任务状态
            self.queue_manager.set_local_task_running(draft_id, False)
    
    def _execute_export_draft(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """执行export_draft逻辑"""
        try:
            if not self.jianying_controller_class:
                return {"success": False, "error": "剪映控制器未能正确导入"}
            
            # 获取参数
            draft_id = task_data.get('draft_id')
            resolution = task_data.get('resolution')
            framerate = task_data.get('framerate')
            timeout = task_data.get('timeout', 12000)
            
            if not draft_id:
                return {"success": False, "error": "缺少必需参数 draft_id"}
            
            # 获取剪映控制器实例
            controller = self._get_jianying_controller()
            
            # 转换分辨率和帧率参数
            resolution_enum = None
            if resolution:
                try:
                    resolution_enum = self.export_resolution_class(resolution)
                except ValueError:
                    return {"success": False, "error": f"不支持的分辨率: {resolution}"}
            
            framerate_enum = None
            if framerate:
                try:
                    framerate_enum = self.export_framerate_class(framerate)
                except ValueError:
                    return {"success": False, "error": f"不支持的帧率: {framerate}"}
            
            # 开始导出
            controller.export_draft(
                draft_name=draft_id,
                resolution=resolution_enum,
                framerate=framerate_enum,
                timeout=timeout
            )
            
            logger.info(f"Export draft完成: {draft_id}")
            return {
                "success": True, 
                "result": {
                    "draft_name": draft_id,
                    "message": "导出任务已启动"
                }
            }
            
        except Exception as e:
            logger.error(f"执行export_draft失败: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def _get_jianying_controller(self):
        """获取剪映控制器实例（线程安全）"""
        try:
            # 初始化 uiautomation（创建对象而不是调用函数）
            import uiautomation as uia
            ui_initializer = uia.UIAutomationInitializerInThread()
            
            # 创建新的控制器实例
            controller = self.jianying_controller_class()
            return controller
            
        except Exception as e:
            raise Exception(f"初始化剪映控制器失败: {str(e)}")

# 全局任务处理器实例
export_task_processor = ExportTaskProcessor(export_queue_manager) 