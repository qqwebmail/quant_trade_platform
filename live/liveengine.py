"""实时引擎"""
import os
import pytz
import time
import threading
import pandas as pd
from datetime import datetime, date, timedelta, time as dt_time
from chinese_calendar import is_workday, is_holiday
from typing import Dict
from ..config.config import config
from ..core.models import Order, OrderStatus, OrderDirection
from ..core.market import MarketDataManager
from ..core.trader import TradeExecutor
from ..strategy.FactorCaptitalStrategy import FactorCaptitalStrategy
from ..core.portfolio import PortfolioManager
from ..core.risk import RiskManager
from ..core.watchdog import WatchDog
from ..utils.logger import sys_logger, LogTheme

logger = sys_logger.getChild('LiveEngine')

# 设置pandas打印选项，强制显示所有行和列
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)
pd.set_option('display.max_colwidth', None)  # 显示完整列内容
pd.set_option('display.unicode.east_asian_width', True)

class Engine:
    def __init__(self):
        # 组件
        self.market = None
        self.trader = None
        self.portfolio = None
        self.strategy  = None

class LiveEngine(Engine):
    """实时引擎核心类，负责交易环境执行策略"""
    EQUITY_RECORD_PREFIX = "live_equity_record"
    ORDERS_RECORD_PREFIX = "live_orders_record"

    def __init__(self):
        """
        初始化实时引擎
        :param strategy: 策略实例
        """
        super().__init__()
        self._config = config
        self._init_flag = False
        self._bind_flag = False
        self._record_file_path = self._config.get('record.file_path')
        self._benchmark = self._config.get('benchmark')
        # 组件初始化
        self._init()
        # 组件绑定引擎
        self._bind()
        self._timezone = pytz.timezone('Asia/Shanghai')
        self._current_date = datetime.now(self._timezone).date()
        self.parameters = self.strategy.parameters
        self._lock = threading.Lock()  # 线程锁
        self._orders: Dict[str, Order] = {}  # 策略订单记录
        self._trader_orders: Dict[str, Order] = {}
        self._history_data: Dict[str, pd.DataFrame] = {}  # 各标的的历史数据
        self.df_equity = pd.DataFrame()  # 资金记录
        self.df_orders = pd.DataFrame()  # 成交订单记录
        # 每日交易时间
        self._trading_time: Dict[str, dt_time] = {
            'morning_open': dt_time(9,  30),
            'morning_close': dt_time(11, 30),
            'afternoon_open': dt_time(13, 0),
            'afternoon_close': dt_time(15, 0)
        }
        logger.debug("实时引擎实例已创建 | 策略: %s", type(self.strategy).__name__)

    @property
    def current_date(self):
        return self._current_date

    @current_date.setter
    def current_date(self, date: date):
        self._current_date = date
        self.strategy.current_date = date

    def _init(self):
        """初始化系统组件"""
        logger.debug("开始初始化系统组件...")
        # 行情组件
        self.market = MarketDataManager(config)
        logger.debug("行情组件初始化完成")
        # 持仓组件
        self.portfolio = PortfolioManager(config)
        logger.debug("持仓组件初始化完成")
        # 风险组件
        self.risk = RiskManager(config)
        logger.debug("风险组件初始化完成")
        # 策略组件
        self.strategy = FactorCaptitalStrategy(config)
        logger.debug("策略组件初始化完成")
        # 交易组件
        self.trader = TradeExecutor()
        logger.debug("交易组件初始化完成")
        # 看门狗组件
        self.watchdog = WatchDog()
        logger.debug("看门狗组件初始化完成")
        self._init_flag = True
        logger.info("系统组件初始化完成")

    def _bind(self):
        """系统组件绑定"""
        logger.debug("开始系统组件绑定...")
        # 行情组件
        self.market.bind(self)
        logger.debug("行情组件绑定完成")
        # 持仓组件
        self.portfolio.bind(self)
        logger.debug("持仓组件绑定完成")
        # 风险组件
        self.risk.bind(self)
        logger.debug("风险组件绑定完成")
        # 策略组件
        self.strategy.bind(self) 
        logger.debug("策略组件绑定完成")
        # 交易组件
        self.trader.bind(self)
        logger.debug("交易组件绑定完成")
        # 看门狗组件
        self.watchdog.bind(self)
        logger.debug("看门狗组件绑定完成")
        self._bind_flag = True
        logger.info("系统组件绑定完成")

    def start(self):
        if not self.trader.start():
            raise ValueError("实时引擎执行启动异常") 
        #加载本地快照
        logger.info("加载本地快照数据...")
        self.load_snapshot()
        #查询服务器状态
        logger.info("查询服务器状态...")
        self.query_server()
        #显示本地持仓
        logger.info("显示本地持仓信息...")
        self.display_local() 
        # 开启看门狗
        self.watchdog.start()

    def close(self):
        #存储本地快照
        logger.info("存储本地快照数据...")
        self.portfolio.save_snapshot()
        #显示本地持仓
        logger.info("显示本地持仓信息...")
        self.display_local() 
        # 关闭看门狗
        self.watchdog.shutdown()
        # 断开交易连接
        self.trader.disconnect()

    def on_date_change(self, date: date) -> None:
        """执行跨日操作"""
        #设置实时引擎当日时间
        self.current_date = date
        # 重置订单数据
        self._reset_daily_state()
        #T+1解除
        self.portfolio.unfreeze_all()
        self.display_local()

    def prepare(self) -> None:
        """加载数据集"""
        # 获取最新股票列表
        index_symbols = self.market.get_index_symbols(index=self.strategy.index)
        # 合并股票列表
        symbols = list(set(index_symbols + self.portfolio.symbols + self._benchmark)) 
        # 下载历史数据
        _end_date   = self._current_date - timedelta(days=1)
        _start_date = _end_date - timedelta(days=10)
        logger.info("下载历史股价数据范围: %s 至 %s", _start_date.strftime("%Y-%m-%d"), _end_date.strftime("%Y-%m-%d"))
        self.market.download_history_data(symbols, _start_date, _end_date)
        # 下载最新财务数据
        _end_date   = self._current_date
        _start_date = _end_date - timedelta(days=2*365)
        logger.info("下载历史财务数据范围: %s 至 %s", _start_date.strftime("%Y-%m-%d"), _end_date.strftime("%Y-%m-%d"))
        self.market.download_financial_data(symbols, _start_date, _end_date)

    def pre_market(self) -> None:
        """执行开盘前准备"""
        # 加载数据集
        self.prepare()
        # 合并持仓股票列表, 基准股票代码
        symbols = list(set(self.portfolio.symbols + self._benchmark)) 
        # 加载历史数据
        end_date   = self._current_date - timedelta(days=1)
        start_date = end_date - timedelta(days=10)
        logger.info("加载历史数据范围: %s 至 %s",
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"))
        for symbol in symbols:
            df = self.market.get_local_history_data(symbol, start_date, end_date)
            if not df.empty:
                #数据清洗
                data = df[df['volume'] != 0]
                self.load_history_k_day_data(symbol, data)
                logger.info("已加载 %s 历史数据，数据量: %d 条", symbol, len(data))
        # 更新持仓价格，输出资金曲线数据
        self.update_equity()
        # 执行策略开盘前准备
        if self.strategy.is_execution_day():
            self.strategy.pre_market()

    def on_open(self) -> None:
        """执行开盘交易"""
        if self.strategy.is_execution_day():
            self.portfolio.rebalance_portfolio_exposure()
            self.strategy.on_open()

    def on_trade(self) -> None:
        """执行交易时段"""
        if self.strategy.is_execution_day():
            self.strategy.on_trade()

    def on_close(self) -> None:
        """执行收盘交易"""
        if self.strategy.is_execution_day():
            self.strategy.on_close()

    def post_market(self) -> None:
        """执行收盘后记录"""
        #撤销所有提交但未执行订单
        self.cancel_orders()
        #保存本地快照
        logger.info("存储本地快照数据...")
        self.save_snapshot()
        #显示本地持仓
        logger.info("显示本地持仓信息...")
        self.display_local() 

    def sync_server(self):
        #更新资金
        available_cash = self.trader.query_asset()
        self.portfolio.update_available_cash(available_cash)
        #更新持仓
        dict_positions = self.trader.query_positions()
        self.portfolio.overwrite_positions(dict_positions)

    def query_server(self):
        #查询服务器资金
        self.trader.query_asset()
        #查询服务器持仓
        self.trader.query_positions() 

    def display_local(self):
        #查询本地资金
        self.portfolio.display_equity()
        #查询本地持仓
        self.portfolio.display_positions() 

    def load_snapshot(self):
        #加载本地快照
        self.portfolio.load_snapshot()

    def save_snapshot(self):
        #存储本地快照
        self.portfolio.save_snapshot() 

    def is_trading_day(self, date: date = None):
        """
        判断是否为交易日
        :param dt: datetime对象（北京时间）
        :return: bool
        """
        date = date or datetime.now(self._timezone).date()
        # 排除周末
        if date.weekday() >= 5:
            return False
        # 排除法定节假日
        if is_holiday(date):
            return False
        # 检查调休工作日
        if is_workday(date):
            return True
        return False

    def load_history_k_day_data(self, symbol: str, data: pd.DataFrame) -> None:
        """加载标的的历史数据
        :param symbol: 标的代码
        :param data: 包含DatetimeIndex的DataFrame（需有close字段）
        """
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError("历史数据索引必须是DatetimeIndex")
        self._history_data[symbol] = data.sort_index(ascending=True)
        logger.debug("加载%s标的历史数据 | 时间范围: %s ~ %s | 数据量: %d",
                     symbol, data.index[0].date(), data.index[-1].date(), len(data)) 

    def get_latest_data(self, symbols: list):
        """
        获取最新股票数据
        :param symbol: 标的代码
        :return: 包含最新数据的dict
        """
        data = self.market.get_latest_data(symbols)
        return data

    def _reset_daily_state(self) -> None:
        """重置回测引擎状态"""
        self._orders.clear()
        self._trader_orders.clear()
        logger.info("已重置每日订单记录")

    def add_order(self, order: Order) -> bool:
        logger.debug("✚✚✚实时引擎执行添加待发送订单操作...")
        try:
            if self.portfolio.check_trade(order): 
                # 冻结订单锁定资产
                self.portfolio.freeze_order_locked_asset(order)
                # 加入待发送队列
                self._orders[order.id] = order.copy()
                logger.info("添加待发送订单 | 任务号 %s | %s %s %d股 @ %.3f",
                            order.id, order.symbol, order.direction.value,
                            order.volume, order.price)
                return True
            else:
                return False
        except Exception:
            logger.error("添加待发送订单异常", exc_info=True)
            raise ValueError("添加待发送订单异常")

    def place_orders(self) -> None:
        """提交订单并与持仓管理器交互"""
        logger.info("✪✪✪实时引擎执行提交订单操作...")
        i = 0
        for _id, _order in self._orders.items():
            #只提交状态为PENDING的订单
            if _order.status == OrderStatus.PENDING:
                res = self.trader.place_order(_order)
                if res:
                    _order.update_status(OrderStatus.SUBMITTED)
                else:
                    _order.update_status(OrderStatus.REJECTED)
                i += 1
        logger.info("实时引擎共提交的订单数 %d", i)

    def wait_orders_completion(self) -> None:
        """等待订单执行完成"""
        logger.info("实时引擎等待订单执行完成...")
        start_time = datetime.now()
        # 超时计数器
        timeout_cnt = 0
        # 已发送订单列表
        pending_ids = []
        for _id, _order in self._orders.items():
            if _order.status == OrderStatus.SUBMITTED:
                pending_ids.append(_id)
        while True:
            done = True
            for _id in pending_ids:
                # 检查交易订单记录
                if _id in self._trader_orders:
                    status = self._trader_orders[_id].status
                    if status not in (
                        OrderStatus.FILLED,
                        OrderStatus.CANCELLED,
                        OrderStatus.REJECTED
                    ):
                        done = False
                        break
                else:
                    done = False
                    break
            if not done:
                time.sleep(0.5)
                timeout_cnt += 1
                if timeout_cnt > 60:
                    logger.warning("实时引擎执行订单超时，耗时 %.2f秒", (datetime.now() - start_time).total_seconds())
                    return
            else:
                logger.info("实时引擎执行订单完成，耗时 %.2f秒", (datetime.now() - start_time).total_seconds())
                return
    def cancel_orders(self) -> None:
        """撤销订单并与持仓管理器交互"""
        logger.info("实时引擎执行撤销订单操作...")
        i = 0
        for _id, _order in self._trader_orders.items():
            if _order.status == OrderStatus.SUBMITTED:
                res = self.trader.cancel_order(_order)
                if res:
                    _order.update_status(OrderStatus.CANCELLED, 0, 0)
                    _order = self._orders[_order.id]
                    self.portfolio.unfreeze_order_locked_asset(_order)
                i += 1
        logger.info("实时引擎共撤销的订单数 %d", i)
    def query_positions(self) -> None:
        self.trader.query_positions()

    def _is_valid_order_status_transition(self, old_status: OrderStatus, new_status: OrderStatus) -> bool:
        """状态转移有效性校验"""
        valid_transitions = {
            OrderStatus.PENDING: [OrderStatus.SUBMITTED, OrderStatus.REJECTED],
            OrderStatus.PENDING: [OrderStatus.SUBMITTED, OrderStatus.REJECTED],
            OrderStatus.SUBMITTED: [OrderStatus.PARTIAL_FILLED, OrderStatus.FILLED,
                                    OrderStatus.CANCELLED, OrderStatus.REJECTED],
            OrderStatus.PARTIAL_FILLED: [OrderStatus.FILLED, OrderStatus.CANCELLED]
        }
        return new_status in valid_transitions.get(old_status, [])

    def process_trader_order_callback(self, order: Order) -> None:
        """
        处理订单状态更新（线程安全）
        :param order: 更新后的订单对象
        """
        logger.debug("实时引擎执行回调操作 | 订单号 %s | %s %s %s",
                     order.order_id, order.status.value, order.symbol, order.direction.value)
        try:
            existing = self._trader_orders.get(order.id)
            if not (existing or self._orders.get(order.id)):
                logger.debug("接收到未发送订单 %s %d股 @ %.3f", order.id, order.volume, order.price)
                logger.debug("接收到未发送订单 %s %d股 @ %.3f", order.id, order.volume, order.price)
                return
            # 存在订单：执行更新逻辑
            if existing:
                update_existing = self._update_existing_trader_order(existing, order)
            # 新订单：执行添加逻辑
            else:
                self._add_new_trader_order(order)
            # 统一处理成交订单
            if not existing or update_existing:
                if order.status == OrderStatus.FILLED:
                    self._process_trader_filled_order(order)
                elif order.status == OrderStatus.REJECTED:
                    # 解冻订单锁定资产
                    _order = self._orders[order.id]
                    self.portfolio.unfreeze_order_locked_asset(_order)
        except Exception:
            logger.error("实时引擎执行回调函数异常", exc_info=True)
            raise ValueError("实时引擎执行回调函数异常")

    def _update_existing_trader_order(self, existing: Order, order: Order) -> bool:
        """
        处理委托状态更新
        """
        if not self._is_valid_order_status_transition(existing.status, order.status):
            logger.warning("非法状态转移: %s -> %s | 订单ID: %s",
                           existing.status, order.status, order.order_id)
            return False
        old_status = existing.status
        old_status = existing.status
        existing.update_status(order.status, order.filled_volume, order.filled_price)
        logger.info("已提交订单更新 | 订单号 %s | %s -> %s %s | 成交量:%d/%d 均价 %.3f",
                    order.order_id, old_status.value, order.status.value, order.symbol,
                    order.filled_volume, order.volume, order.filled_price)
        return True

    def _add_new_trader_order(self, order: Order):
        """
        处理委托状态更新
        """
        logger.info("新提交订单更新 | 订单号 %s | %s %s | 成交量:%d/%d 均价 %.3f",
                    order.order_id, order.status.value, order.symbol,
                    order.filled_volume, order.volume, order.filled_price)
        self._trader_orders[order.id] = order.copy()

    def _process_trader_filled_order(self, order: Order) -> None:
        """执行订单并与持仓管理器交互"""
        logger.info("执行订单处理 | 订单号 %s | %s %s %d股 @ %s",
                    order.order_id, order.symbol, order.direction.value, order.filled_volume, order.filled_price)
        order.order_id, order.symbol, order.direction.value, order.filled_volume, order.filled_price
        # 应用交易
        try:
            # 解冻订单锁定资产
            _order = self._orders[order.id]
            self.portfolio.unfreeze_order_locked_asset(_order)
            # 获取订单持仓平均成交价
            if _order.direction == OrderDirection.SELL:
                avg_price = self.portfolio.get_position(order.symbol).avg_price
                entry_date = self.portfolio.get_position(order.symbol).entry_date
                pnl = (order.filled_price - avg_price) * order.filled_volume
                if entry_date:
                    holding_days = (self._current_date - entry_date).days + 1
                else:
                    holding_days = 0
            else:
                pnl = 0.0
                holding_days = 0
            # 更新持仓数据
            self.portfolio.apply_trade(order)
            logger.info(f"✔✔✔ 成功执行订单 | 订单号 {order.order_id} | {self._current_date.strftime('%Y-%m-%d')} {order.symbol} {order.direction.value} {LogTheme.SYMBOL} {order.symbol} {LogTheme.RESET} {order.filled_volume}股 @ {order.filled_price}")
            # 当日成交订单
            dict_order = {
                '日期'    : self._current_date,
                '股票'    : order.symbol,
                '方向'    : order.direction.value,
                '方向'    : order.direction.value,
                '成交量'  : order.filled_volume,
                '成交价'  : order.filled_price,
                '成交总额': round(order.filled_price * order.filled_volume, 2),
                '盈利'    : round(pnl, 2),
                '持仓天数': holding_days
            }
            df_order = pd.DataFrame(dict_order, index=[0](@ref)  # 将持仓信息转换为 DataFrame 格式
            # 创建记录目录
            os.makedirs(self._record_file_path, exist_ok=True)
            filename = f"{self.ORDERS_RECORD_PREFIX}"
            filepath = os.path.join(self._record_file_path, f"{filename}.csv")
            header = not os.path.exists(filepath)
            df_order.to_csv(filepath, mode='a', header=header, index=False, encoding='utf-8-sig')
            logger.debug(f"成功保存成交订单记录至 {filepath}")
            # 合并成交订单
            self.df_orders = pd.concat([self.df_orders, df_order])
        except ValueError as e:
            logger.error("订单执行失败: %s", str(e))
        except Exception as e:
            logger.error("订单处理异常: %s", str(e), exc_info=True)

    def update_equity(self) -> None:
        """更新当日资金曲线数据"""
        try:
            # 价格获取闭包函数（最近价格查找）
            def get_price(symbol: str) -> float:
                """获取价格优先级：当日收盘价 > 最近收盘价 > 持仓成本 > 0"""
                try:
                    # 检查标的是否存在历史数据
                    if symbol not in self._history_data:
                        logger.warning(f"标的 {symbol} 无历史数据")
                        return 0.0
                    # 尝试获取最近一日收盘价
                    return round(self._history_data[symbol].iloc[-1]['close'],2)
                except Exception as e:
                    logger.error("价格获取闭包函数: %s", str(e), exc_info=True)
                    return 0.0
            # 更新所有持仓的市值（使用前一交易日日收盘价）
            self.portfolio.update_market_value(self._current_date, price_getter=get_price)
            # 当日总权益
            dict_equity = {
                '日期'    : self._current_date,
                '总资产'  : round(self.portfolio.total_equity, 2),
                '总现金'  : round(self.portfolio.total_cash, 2),
                '可用现金': round(self.portfolio.available_cash, 2),
                '持仓市值': round(self.portfolio.total_market_value, 2),
            }
            # 添加每个持仓标的的市值
            for symbol in self.portfolio.symbols:
                dict_equity[f'{symbol}'] = position_value
            # 添加每个基准股票的独立数据
            for symbol in self._benchmark:
                try:
                    # 检查基准股票数据是否存在
                    if symbol not in self._history_data:
                        price = 0.0
                        logger.warning(f"基准股票 {symbol} 无历史数据")
                    else:
                        price = round(self._history_data[symbol].iloc[-1]['close'], 2)
                except Exception as e:
                    logger.error(f"基准股票 {symbol} 数据处理异常: {str(e)}", exc_info=True)
                    price = 0.0
                price = 0.0
                # 使用"基准_股票代码"的格式作为列名
                dict_equity[f'基准_{symbol}'] = price
            df_equity = pd.DataFrame(dict_equity, index=[0](@ref)  # 将持仓信息转换为 DataFrame 格式
            logger.debug(f"更新资金信息\n {df_equity}")
            # 创建记录目录
            os.makedirs(self._record_file_path, exist_ok=True)
            filename = f"{self.EQUITY_RECORD_PREFIX}"
            filepath = os.path.join(self._record_file_path, f"{filename}.csv")
            header = True
            df_equity.to_csv(filepath, mode='a', header=header, index=False, encoding='utf-8-sig')
            logger.info(f"成功保存资金记录至 {filepath}")
            # 合并资金记录
            self.df_equity = pd.concat([self.df_equity, df_equity])
        except Exception as e:
            logger.error("资金曲线更新异常 %s: %s", self._current_date, str(e), exc_info=True)
