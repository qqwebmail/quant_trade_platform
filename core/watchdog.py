import pytz
import time
import threading
from datetime import datetime, timedelta
import itertools
from ..utils.logger import sys_logger

logger = sys_logger.getChild('WatchDog')

class WatchDog:
    """
    看门狗类，用于监控系统组件的健康状态
    """
    def __init__(self):
        """
        初始化看门狗
        """
        self._timezone = pytz.timezone('Asia/Shanghai')
        self.monitor_flag = True
        self.lock = threading.Lock()
        self.spinner = itertools.cycle(['.' * i for i in range(1, 11)])  # 最大点数限制为 10
        # 初始化监控线程
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True  # 设置为守护线程

    def bind(self, engine):
        self._engine = engine

    def start(self):
        logger.info("看门狗已启动")
        self.monitor_thread.start()  # 启动线程

    def _monitor_loop(self):
        """监控主循环"""
        time.sleep(5)
        while self.monitor_flag:
            try:
                with self.lock:
                    # trader断线重连
                    if not self._engine.trader.status:
                        self._engine.trader.start()
                        time.sleep(60)
                        continue
                    # 获取当前时间
                    now = datetime.now(self._timezone).time()
                    # 更新动画
                    print("\r" + " " * 30, end="", flush=True)
                    print(f"\r{now.strftime('%H:%M:%S')} {next(self.spinner)}", end="", flush=True)
                time.sleep(60)
            except Exception as e:
                logger.error("监控循环发生错误: %s", e, exc_info=True)

    def shutdown(self):
        """关闭看门狗"""
        self.monitor_flag = False
        self.monitor_thread.join()
        logger.info("看门狗已关闭")