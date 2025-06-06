"""交易系统"""
import os
import pytz
import time
import subprocess
from datetime import datetime, timedelta, time as dt_time
from typing import Dict
from .liveengine import LiveEngine
from ..config.config import config
from ..core.exceptions import TradingSystemError
from ..utils.logger import sys_logger

logger = sys_logger.getChild('LiveSystem')

class LiveSystem:
    """交易系统主控类，负责协调各组件工作流程"""
    MODE = 'live'

    def __init__(self):
        """初始化交易系统"""
        self._config = config
        self._init_flag = False  # 组件初始化标志
        self.engine = None
        # 记录当前日期
        self._timezone = pytz.timezone('Asia/Shanghai')
        self._current_date = datetime.now(self._timezone).date()
        # 交易日状态管理
        self._trading_status: Dict[str, bool] = {
            'pre_market': False,  # 开盘前准备完成
            'on_open': False,     # 开盘交易
            'on_trade': False,    # 盘中交易
            'on_close': False,    # 收盘交易
            'post_market': False, # 收盘后处理完成
            'is_trading_day': False,  # 是否为交易日
            'is_trading_time': False  # 是否在交易时段
        }
        logger.debug("交易系统实例已创建")

    def __enter__(self) -> None:
        """上下文管理器入口"""
        try:
            if not self._init_flag:
                self._init()
            return self
        except Exception:
            logger.critical("交易系统进入上下文失败", exc_info=True)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        try:
            self.shutdown()
        except Exception as e:
            logger.error("系统关闭时发生异常: %s", str(e), exc_info=True)
        finally:
            self._init_flag = False
        return False  # 传播异常

    def set_terminal_title(self, title: str = ""):
        """设置终端窗口标题（仅限Windows）"""
        try:
            full_title = f"{title} mode: {self.MODE} {os.getcwd()}"
            command = f'$Host.UI.RawUI.WindowTitle = "{full_title}"'
            subprocess.run(["powershell", "-Command", command], check=True)
            logger.debug("终端标题已更新: %s", full_title)
        except subprocess.CalledProcessError as e:
            logger.warning("终端标题更新失败: %s", str(e))

    def _init(self):
        """初始化系统核心组件"""
        # 实时引擎
        try:
            logger.info("正在初始化实盘交易引擎...")
            self.engine = LiveEngine()
            logger.info("实盘交易引擎初始化完成")
            self._init_flag = True
        except Exception as e:
            error_msg = f"系统组件初始化失败: {str(e)}"
            logger.critical(error_msg, exc_info=True)
            raise TradingSystemError(error_msg) from e

    def start(self):
        """启动交易系统准备工作
        步骤：
        1. 启动交易引擎
        2. 加载本地快照
        3. 同步服务器状态
        4. 显示本地数据
        异常：
        RuntimeError: 引擎启动失败时抛出
        """
        try:
            logger.info("启动交易引擎...")
            self.engine.start()
        except Exception as e:
            error_msg = f"系统启动流程中断: {str(e)}"
            logger.critical(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e

    def shutdown(self) -> None:
        """安全关闭系统"""
        logger.info("启动系统关闭流程...")
        try:
            if self.engine:
                self.engine.close()
                logger.info("交易引擎已关闭")
            logger.info("系统关闭完成")
        except Exception as e:
            logger.error("系统关闭过程中发生错误: %s", str(e), exc_info=True)
        finally:
            self._init_flag = False

    def run(self):
        """启动交易系统主循环"""
        try:
            self._loop()
        except Exception as e:
            logger.critical("系统运行异常终止: %s", str(e), exc_info=True)
            raise

    def _reset_daily_status(self) -> None:
        """重置每日交易状态"""
        self._trading_status.update({
            'pre_market': False,
            'on_open': False,
            'on_trade': False,
            'on_close': False,
            'post_market': False,
            'is_trading_time': False,
            'is_trading_day': False
        })
        logger.debug("每日交易状态已重置")

    def _check_date_change(self) -> None:
        # 检测日期变化（跨日重置状态）
        now = datetime.now(self._timezone)

        # 判断当前时间是否在当日8点55前
        if now.time() < dt_time(8, 55):
            # 若在8点55前，则仍视为前一交易日
            adjusted_date = (now - timedelta(days=1)).date()
        else:
            adjusted_date = now.date()

        if adjusted_date != self._current_date:
            print("", flush=True)
            logger.info("检测到交易日变更: %s -> %s", self._current_date.strftime("%Y-%m-%d"), adjusted_date.strftime("%Y-%m-%d"))
            self._current_date = adjusted_date

            # 执行实时引擎跨日操作
            self.engine.on_date_change(adjusted_date)

            self._reset_daily_status()
            self._trading_status['is_trading_day'] = self.engine.is_trading_day(now)
            logger.info("交易日状态: %s", "交易日" if self._trading_status['is_trading_day'] else "非交易日")

    def _pre_market(self) -> None:
        """执行开盘前准备"""
        print("", flush=True)
        logger.info("-" * 20 + "进入开盘前准备阶段" + "-" * 20)

        try:
            self.engine.pre_market()
            self._trading_status['pre_market'] = True
            logger.info("开盘前准备阶段执行完成")
        except Exception as e:
            error_msg = f"开盘前准备阶段执行失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise TradingSystemError(error_msg) from e

    def _on_open(self) -> None:
        """启动开盘交易"""
        print("", flush=True)
        logger.info("-" * 20 + "进入开盘交易阶段" + "-" * 20)

        try:
            self.engine.on_open()
            self._trading_status['on_open'] = True
            logger.info("开盘交易阶段执行完成")
        except Exception as e:
            error_msg = f"开盘交易阶段执行失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise TradingSystemError(error_msg) from e

    def _on_trade(self) -> None:
        """执行交易时段"""
        if not self._trading_status['on_trade']:
            print("", flush=True)
            logger.info("-" * 20 + "进入盘中交易阶段" + "-" * 20)

        try:
            self.engine.on_trade()
            if not self._trading_status['on_trade']:
                logger.info("盘中交易时段阶段执行完成")
            self._trading_status['on_trade'] = True
        except Exception as e:
            error_msg = f"盘中交易时段阶段执行失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise TradingSystemError(error_msg) from e

    def _on_close(self) -> None:
        """启动收盘交易"""
        print("", flush=True)
        logger.info("-" * 20 + "进入收盘交易阶段" + "-" * 20)

        try:
            self.engine.on_close()
            self._trading_status['on_close'] = True
            logger.info("收盘交易阶段执行完成")
        except Exception as e:
            error_msg = f"收盘交易阶段执行失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise TradingSystemError(error_msg) from e

    def _post_market(self) -> None:
        """执行收盘后记录阶段"""
        print("", flush=True)
        logger.info("-" * 20 + "进入收盘后记录阶段" + "-" * 20)

        try:
            self.engine.post_market()
            self._trading_status['post_market'] = True
            logger.info("收盘后记录阶段执行完成")
        except Exception as e:
            error_msg = f"收盘后记录阶段执行失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise TradingSystemError(error_msg) from e

    def _handle_trading_phases(self) -> None:
        """处理不同交易时段"""
        if not self._trading_status['is_trading_day']:
            return

        now = datetime.now(self._timezone)

        # 开盘前准备阶段（09:00-09:30）
        if not self._trading_status['pre_market'] and now.time() >= dt_time(9, 0):
            self._pre_market()

        # 开盘交易时间（09:30）
        if not self._trading_status['on_open'] and now.time() >= dt_time(9, 30):
            self._on_open()

        # 交易时段（09:30-15:00）
        if now.time() >= dt_time(9, 30) and now.time() < dt_time(15, 00):
            self._on_trade()

        # 收盘交易时间（14:55）
        if not self._trading_status['on_close'] and now.time() >= dt_time(14, 55):
            self._on_close()

        # 收盘后处理（15:00之后）
        if not self._trading_status['post_market'] and now.time() >= dt_time(15, 0):
            self._post_market()

    def _loop(self):
        """实盘交易模式主循环"""
        if not self._init_flag:
            raise TradingSystemError("交易引擎未初始化")

        try:
            self._handle_trading_phases()
            self._check_date_change()

            # 等待
            time.sleep(1)  # 降低主循环频率
        except Exception as e:
            error_msg = f"主循环运行异常: {str(e)}"
            logger.critical(error_msg, exc_info=True)
            raise TradingSystemError(error_msg) from e