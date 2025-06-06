@echo off
cd /d c:\Users\Administrator\Desktop\本地
mkdir quant_trade_platform

rem 创建 backtest 目录及其文件
mkdir quant_trade_platform\backtest
echo. > quant_trade_platform\backtest\__init__.py
echo. > quant_trade_platform\backtest\analyzer.py
echo. > quant_trade_platform\backtest\backtestengine.py
echo. > quant_trade_platform\backtest\backtestsystem.py
echo. > quant_trade_platform\backtest\benchmark.py
echo. > quant_trade_platform\backtest\visualizer.py

rem 创建 config 目录及其文件
mkdir quant_trade_platform\config
echo. > quant_trade_platform\config\__init__.py
echo. > quant_trade_platform\config\config.py
echo. > quant_trade_platform\config\settings.yaml

rem 创建 core 目录及其文件
mkdir quant_trade_platform\core
echo. > quant_trade_platform\core\__init__.py
echo. > quant_trade_platform\core\exceptions.py
echo. > quant_trade_platform\core\market.py
echo. > quant_trade_platform\core\models.py
echo. > quant_trade_platform\core\portfolio.py
echo. > quant_trade_platform\core\risk.py
echo. > quant_trade_platform\core\strategy.py
echo. > quant_trade_platform\core\trader.py
echo. > quant_trade_platform\core\watchdog.py

rem 创建 live 目录及其文件
mkdir quant_trade_platform\live
echo. > quant_trade_platform\live\__init__.py
echo. > quant_trade_platform\live\liveengine.py
echo. > quant_trade_platform\live\livesystem.py

rem 创建其他目录
mkdir quant_trade_platform\logs
mkdir quant_trade_platform\plot
mkdir quant_trade_platform\records
mkdir quant_trade_platform\snapshots

rem 创建 strategy 目录及其文件
mkdir quant_trade_platform\strategy
echo. > quant_trade_platform\strategy\__init__.py
echo. > quant_trade_platform\strategy\FactorCaptitalStrategy.py

rem 创建 utils 目录及其文件
mkdir quant_trade_platform\utils
echo. > quant_trade_platform\utils\logger.py
echo. > quant_trade_platform\utils\database.py

rem 创建根目录文件
echo. > quant_trade_platform\__init__.py
echo. > quant_trade_platform\main_backtest.py
echo. > quant_trade_platform\main_functest.py
echo. > quant_trade_platform\main_live.py
echo. > quant_trade_platform\main_livetest.py
echo. > quant_trade_platform\main_optimize.py
echo. > quant_trade_platform\main.py
echo. > quant_trade_platform\README.txt