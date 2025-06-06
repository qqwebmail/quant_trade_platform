quant_trade_platform/
├── backtest/
│   ├── __init__.py
│   ├── analyzer.py
│   ├── backtestengine.py
│   ├── backtestsystem.py
│   ├── benchmark.py
│   └── visualizer.py
├── config/
│   ├── __init__.py
│   ├── config.py
│   └── settings.yaml
├── core/
│   ├── __init__.py
│   ├── exceptions.py
│   ├── market.py           # 行情管理
│   ├── models.py           # 数据模型
│   ├── portfolio.py        # 持仓管理
│   ├── risk.py             # 风控管理
│   ├── strategy.py         # 策略基类
│   ├── trader.py           # 交易执行
│   └── watchdog.py         # 看门狗
├── live/
│   ├── __init__.py
│   ├── liveengine.py
│   └── livesystem.py
├── logs/
├── plot/
├── records/
├── snapshots/
├── strategy/
│   ├── __init__.py
│   └── FactorCaptitalStrategy.py
├── utils/
│   ├── logger.py           # 日志系统
│   └── database.py         # 数据库接口
├── __init__.py
├── main_backtest.py
├── main_functest.py
├── main_live.py
├── main_livetest.py
├── main_optimize.py
├── main.py                 # 主程序入口
└── README.txt