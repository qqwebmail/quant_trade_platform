"""主程序入口"""
from quant_trade_platform.config.config import config
from quant_trade_platform.utils.logger import sys_logger

mode = config.get('mode')

if __name__ == "__main__":
    if mode == 'livetest':
        from main_livetest import main
        sys_logger.info("量化交易平台启动, 当前模式为 livetest")
        main()
    elif mode == 'functest':
        from main_functest import main
        sys_logger.info("量化交易平台启动, 当前模式为 functest")
        main()
    elif mode == 'backtest':
        from main_backtest import main
        sys_logger.info("量化交易平台启动, 当前模式为 backtest")
        main()
    elif mode == 'optimize':
        from main_optimize import main
        sys_logger.info("量化交易平台启动, 当前模式为 optimize")
        main()
    else:
        from main_live import main
        sys_logger.info("量化交易平台启动, 当前模式为 live")
        main()