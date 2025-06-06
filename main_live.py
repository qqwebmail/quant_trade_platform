"""主程序入口"""
import sys
import ctypes
import os
import signal
from typing import NoReturn
from datetime import datetime
from quant_trade_platform.config.config import config
from quant_trade_platform.live.livesystem import LiveSystem
from quant_trade_platform.utils.logger import sys_logger, close_logger

logger = sys_logger.getChild('Main')

# 全局退出标志
_exit_flag = False

# 定义Windows API常量
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_DISPLAY_REQUIRED = 0x00000002

def prevent_sleep():
    """启用防休眠模式"""
    logger.info("启用防休眠模式")
    ctypes.windll.kernel32.SetThreadExecutionState(
        ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
    )

def allow_sleep():
    """恢复系统默认休眠策略"""
    logger.info("恢复系统默认休眠策略")
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)

def handle_signal(signum, frame) -> None:
    """处理系统信号实现优雅退出
    Args:
        signum: 信号编号 (e.g. SIGINT=2, SIGTERM=15)
        frame: 当前堆栈帧对象
    """
    global _exit_flag
    logger.warning("接收到系统信号 %d，启动优雅退出流程...", signum)
    _exit_flag = True

def main() -> NoReturn:
    """主程序入口函数
    实现功能：
    1. 系统信号注册
    2. 交易系统初始化
    3. 主循环运行
    4. 异常安全处理
    """
    try:
        start_time = datetime.now()
        # 注册信号处理
        signal.signal(signal.SIGINT, handle_signal)  # Ctrl+C
        signal.signal(signal.SIGTERM, handle_signal)  # kill命令
        # 系统启动信息记录
        logger.info("=" * 60)
        logger.info("量化交易平台启动")
        logger.info("启动时间: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        logger.info("Python 版本: %s", sys.version.replace('\n', ''))
        logger.info("当前工作目录: %s", os.getcwd())
        logger.info("QMT工作目录: %s", config.get('xt.plugin_path'))
        logger.info("=" * 60)
        # 初始化交易系统
        with LiveSystem() as system:
            logger.info("交易系统初始化完成")
            try:
                # 启用防休眠模式
                prevent_sleep()
                # 设置终端标题
                system.set_terminal_title(f"QMT LiveSystem - {config.get('account.account_id')}")
                # 系统预热（连接交易所、加载策略等）
                system.start()
                # 主循环
                logger.info("进入主运行循环")
                while not _exit_flag:
                    try:
                        system.run()
                    except KeyboardInterrupt:
                        logger.warning("检测到键盘中断信号")
                        break
                    except Exception as e:
                        logger.critical("主循环运行异常: %s", str(e), exc_info=True)
                        raise
            except Exception as e:
                logger.critical("交易系统运行时异常: %s", str(e), exc_info=True)
                raise  # 传递异常到外层处理
        logger.info("交易系统已安全关闭")
    except Exception as e:
        logger.critical("系统级异常导致终止: %s", str(e), exc_info=True)
        sys.exit(255)
    finally:
        # 恢复系统默认休眠策略
        allow_sleep()
        logger.info("=" * 60)
        logger.info("系统退出时间: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        logger.info("运行时长: %.2f秒", (datetime.now() - start_time).total_seconds())
        logger.info("=" * 60)
        close_logger()

if __name__ == "__main__":
    main()
    sys.exit(0)