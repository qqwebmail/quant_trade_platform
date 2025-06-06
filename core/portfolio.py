import os
import json
import gzip
import pandas as pd
from datetime import datetime, date
from typing import Dict, Callable
from ..config.config import ConfigManager
from ..core.models import Order, OrderType, OrderDirection, Position
from ..utils.logger import sys_logger
logger = sys_logger.getChild('Portfolio')
# 设置pandas打印选项，强制显示所有行和列
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)
pd.set_option('display.max_colwidth', None)  # 显示完整列内容
pd.set_option('display.unicode.east_asian_width', True)
class PortfolioManager:
    """持仓管理器"""
    # 新增类常量
    SNAPSHOT_VERSION = "0.3"  # 快照版本号
    SNAPSHOT_PREFIX = "portfolio_snapshot"
    def __init__(self, config: ConfigManager):
        """
        初始化持仓管理器
        """
        self.config = config
        self.logger = logger
        self._snapshot_file_path = self.config.get('snapshot.file_path')
        self.available_cash = self.config.get('account.available_cash')  # 可用现金
        self.total_cash = self.available_cash # 总资金
        self._positions: Dict[str, Position] = {}  # 各标的持仓
        self._orders: Dict[str, Order] = {}  # 交易订单
        # 交易参数
        self.commission_rate = self.config.get('account.commission_rate')
        self.stamp_duty_rate = self.config.get('account.stamp_duty_rate')
        self.slippage_rate = self.config.get('account.slippage_rate')
        self.minimum_commission_fee = self.config.get('account.minimum_commission_fee')
    def bind(self, engine):
        self.engine = engine
        self.risk = self.engine.risk
    @property
    def symbols(self):
        return sorted(list(self._positions.keys()))
    @property
    def positions(self):
        return self._positions
    def get_position(self, symbol):
        if self._positions.get(symbol):
            return self._positions[symbol]
        else:
            return None
    def update_available_cash(self, available_cash: float) -> None:
        self.logger.info(f'可用现金更新为: {self.available_cash:,.3f} -> {available_cash:,.3f}') 
        self.logger.info(f'总现金更新为: {self.total_cash:,.3f} -> {available_cash:,.3f}') 
        self.available_cash = available_cash
        self.total_cash = available_cash
    def overwrite_positions(self, positions:Dict[str, Position] = None) -> None:
        if not positions:
            self.logger.info("无持仓数据输入, 清空本地持仓")
            self._positions.clear()
            return
        if not self._positions:
            self.logger.info("本地无持仓数据输入，复制输入数据")
        else:
            self.logger.info("本地有持仓数据输入，覆盖本地数据")
        self._positions = positions.copy()            
        self.display_positions()    
    def add_positions(self, positions:Dict[str, Position]) -> None:
        if not positions:
            self.logger.info("无持仓数据输入")
            return
        self.logger.info("手动添加持仓数据输入")
        for _id in positions:
            self._positions[_id] = positions[_id]
        self.display_positions()    
    def display_equity(self) -> None:
        dict_cash = {
            '总资产':   self.total_equity,
            '总资金':   self.total_cash,
            '持仓市值': self.total_market_value,
            '可用资金': self.available_cash,
            '冻结资金': self.total_cash - self.available_cash
        }
        df_cash = pd.DataFrame(dict_cash, index=[0])  # 将资金信息转换为 DataFrame 格式
        self.logger.info(f'当前本地资金为:\n{df_cash}') 
    def display_positions(self) -> None:
        list_positions = []
        for symbol in self._positions:
            position = self._positions[symbol]    
            dict_position = {
                '证券代码': position.symbol,
                '持仓数量': position.total_volume,
                '可用数量': position.available_volume,
                '成本价'  : position.avg_price, 
                '当前价格': position.cur_price,
                '当前市值': position.market_value,
                '浮动盈亏': position.float_pnl,
                '买入时间': position.entry_date,
            }
            list_positions.append(dict_position)
        df_positions = pd.DataFrame(list_positions)  # 将持仓信息转换为 DataFrame 格式
        self.logger.info(f"当前本地持仓为\n{df_positions}")                    
    def freeze_order_locked_asset(self, order: Order) -> None:
        self.logger.debug("执行冻结订单锁定资产操作")
        try:
            if order.direction == OrderDirection.BUY:
                # 买方向冻结资金
                # 计算交易费用
                trade_amount = order.price * order.volume
                commission = trade_amount * self.commission_rate
                stamp_duty = 0.0
                total_fee = round(max(commission, self.minimum_commission_fee) + stamp_duty, 2)     
                total_cost = trade_amount + total_fee                
                old_available_cash = self.available_cash
                self.available_cash -= total_cost
                self.logger.info("冻结订单锁定资产 | 任务号 %s | %s %s %d @ %.3f | 可用资金 %.3f -> %.3f", 
                            order.id, order.symbol, order.direction.value, 
                            order.volume, order.price,
                            old_available_cash, self.available_cash)
            else:
                #卖方向冻结持仓
                position = self._positions[order.symbol]
                old_available_volume = position.available_volume
                position.available_volume -= order.volume
                self.logger.info("冻结订单锁定资产 | 任务号 %s | %s %s %d @ %.3f | 可用持仓 %d -> %d", 
                            order.id, order.symbol, order.direction.value, 
                            order.volume, order.price,
                            old_available_volume, position.available_volume)
            # 添加入订单列表中
            self._orders[order.id] = order.copy()
        except Exception:
            self.logger.error("冻结订单资产操作异常", exc_info=True)
            raise ValueError("冻结订单资产操作异常")        
    def unfreeze_order_locked_asset(self, order: Order) -> None:
        self.logger.debug("执行解冻订单锁定资产操作")
        try:
            if order.direction == OrderDirection.BUY:
                # 买方向冻结资金
                # 计算交易费用
                trade_amount = order.price * order.volume
                commission = trade_amount * self.commission_rate
                stamp_duty = 0.0
                total_fee = round(max(commission, self.minimum_commission_fee) + stamp_duty, 2)     
                total_cost = trade_amount + total_fee                
                old_available_cash = self.available_cash
                self.available_cash += total_cost
                self.logger.info("解冻订单锁定资产 | 任务号 %s | %s %s %d @ %.3f | 可用资金 %.3f -> %.3f", 
                            order.id, order.symbol, order.direction.value, 
                            order.volume, order.price,
                            old_available_cash, self.available_cash)
            else:
                #卖方向冻结持仓
                if self._positions.get(order.symbol):
                    position = self._positions[order.symbol]
                    old_available_volume = position.available_volume
                    position.available_volume += order.volume
                    self.logger.info("解冻订单锁定资产 | 任务号 %s | %s %s %d @ %.3f | 可用持仓 %d -> %d", 
                                order.id, order.symbol, order.direction.value, 
                                order.volume, order.price,
                                old_available_volume, position.available_volume)
                else:
                    self.logger.error("解冻订单锁定资产失败, 无卖方向冻结持仓 | 任务号 %s | %s %s %d @ %.3f", 
                                order.id, order.symbol, order.direction.value, 
                                order.volume, order.price)
            # 从订单列表中删除
            if self._orders.get(order.id):
                self.logger.debug("删除订单锁定订单 %s %d股 @ %.3f", order.id, order.volume, order.price)
                del self._orders[order.id]
            else:
                self.logger.error("删除订单锁定订单失败, 无锁定订单 %s %d股 @ %.3f", order.id, order.volume, order.price)
        except Exception:
            self.logger.error("解冻订单锁定资产操作异常", exc_info=True)
            raise ValueError("解冻订单锁定资产操作异常")                
    def unfreeze_all(self) -> None:
        self.logger.info("解冻所有冻结资金, 本地持仓, 删除所有订单锁定订单")
        self._orders.clear()
        self.available_cash = self.total_cash
        for symbol in self._positions:
            position = self._positions[symbol]    
            position.available_volume = position.total_volume
    def check_trade(self, order: Order) -> bool:
        # 计算交易费用
        trade_amount = order.price * order.volume
        if order.direction == OrderDirection.BUY:
            commission = trade_amount * self.commission_rate
            stamp_duty = 0.0
        else:
            commission = trade_amount * self.commission_rate
            stamp_duty = trade_amount * self.stamp_duty_rate
        total_fee = round(max(commission, self.minimum_commission_fee) + stamp_duty, 2)     
        total_cost = trade_amount + total_fee
        if order.direction == OrderDirection.BUY:
            # 检查资金是否充足
            if self.available_cash < total_cost:
                self.logger.warning(f"资金不足，无法买入 {order.symbol} {order.volume}股@{order.price} 需 {total_cost:,.3f}元, 可用资金为{self.available_cash:,.3f}")      
                return False
            else:
                self.logger.info(f"资金充足，可以买入 {order.symbol} {order.volume}股@{order.price} 需 {total_cost:,.3f}元, 可用资金为{self.available_cash:,.3f}")
                return True
        else:
            # 检查持仓是否足够
            if order.symbol not in self._positions:
                self.logger.warning(f"尝试卖出未持仓标的 {order.symbol}")
                return False
            position = self._positions[order.symbol]
            if position.available_volume < order.volume:
                self.logger.warning(f"可用持仓不足 {order.symbol} 可用持仓:{position.available_volume} 尝试卖出:{order.volume}")
                return False
            return True
    def apply_trade(self, order: Order) -> None:
        """
        应用交易结果（含最低手续费）
        :param order: 已执行订单
        """
        # 计算交易费用
        trade_amount = order.filled_price * order.filled_volume
        if order.direction == OrderDirection.BUY:
            commission = trade_amount * self.commission_rate
            stamp_duty = 0.0
        else:
            commission = trade_amount * self.commission_rate
            stamp_duty = trade_amount * self.stamp_duty_rate
        total_fee = round(max(commission, self.minimum_commission_fee) + stamp_duty, 2)
        # 更新持仓
        if order.direction == OrderDirection.BUY:
            self._handle_buy(order.symbol, order.filled_volume, order.filled_price, total_fee)
        else:
            self._handle_sell(order.symbol, order.filled_volume, order.filled_price, total_fee)
        self.logger.info(
            f"持仓更新 | {order.direction.value} {order.symbol} {order.filled_volume}股 "
            f"@ {order.filled_price:.3f} 手续费:{total_fee:.3f} "
            f"可用现金:{self.available_cash:,.3f}"
        )
        self.display_equity()
        self.display_positions()
    def _handle_buy(self, symbol: str, volume: int, price: float, fee: float) -> None:
        """处理买入交易"""
        # 计算总成本
        total_cost = price * volume + fee
        # 检查资金是否充足
        if self.available_cash < total_cost:
            self.logger.warning(f"资金不足，无法买入 {symbol} {volume}股 需{total_cost:,.3f}元")
        # 更新现金
        self.available_cash -= total_cost
        self.total_cash -= total_cost
        # 更新持仓
        if symbol not in self._positions:
            self._positions[symbol] = Position(symbol)
        position = self._positions[symbol]
        position.update_positon(volume, 0, price)
        position.entry_date = self.engine.current_date
    def _handle_sell(self, symbol: str, volume: int, price: float, fee: float) -> None:
        """处理卖出交易"""          
        position = self._positions[symbol]
        # 计算净收入
        net_proceeds = price * volume - fee
        # 更新现金
        self.available_cash += net_proceeds
        self.total_cash += net_proceeds
        # 更新持仓
        position.update_positon(-volume, -volume, price)
        # 清理空头寸
        if position.total_volume <= 0:
            del self._positions[symbol]
    def update_market_value(self, date: date, price_getter: Callable[[str], float]) -> None:
        """更新所有持仓市值（需提供最新价格获取函数）"""
        total_value = self.available_cash
        for symbol, pos in self._positions.items():
            latest_price = price_getter(symbol)
            pos.update_price(date, latest_price)
            total_value += pos.market_value
        self.total_value = total_value
    @property
    def total_equity(self) -> float:
        """当前总权益（现金 + 持仓市值）"""
        return self.total_cash + self.total_market_value
    @property
    def total_market_value(self) -> float:
        """当前持仓市值"""
        return sum(
            pos.market_value for pos in self._positions.values()
        )
    def reset(self) -> None:
        """重置持仓到初始状态"""
        self.available_cash = 0
        self._positions.clear()
        self.logger.info("持仓管理器已重置")
    def rebalance_portfolio_exposure(self) -> None:
        """持仓组合风险敞口再平衡"""
        try:
            # 获取当前总市值和总权益
            current_mv = self.total_market_value
            equity = self.total_equity
            target = self.risk.params.max_portfolio_exposure
            if equity <= 0:
                self.logger.warning("总权益为0或负，无法调整风险敞口")
                return
            # 计算当前风险敞口比例
            current_ratio = current_mv / equity
            if current_ratio <= target:
                self.logger.info(f"当前风险敞口 {current_ratio:.2%} ≤ 目标 {target:.2%}，无需调整")
                return
            # 计算需要减少的市值
            target_mv = target * equity
            excess_mv = current_mv - target_mv
            self.logger.info(
                f"风险敞口再平衡 | 当前 {current_ratio:.2%} → 目标 {target:.2%} | "
                f"需减少市值 ¥{excess_mv:,.2f}"
            )
            # 按市值降序排序持仓
            positions = sorted(
                self._positions.values(),
                key=lambda x: x.market_value,
                reverse=True
            )
            # 按比例分配卖出金额
            total_mv = current_mv
            remaining_excess = excess_mv
            orders_created = 0
            for pos in positions:
                if remaining_excess <= 0:
                    break
                # 计算该持仓应承担的卖出市值（按市值比例分配）
                allocation_ratio = pos.market_value / total_mv
                target_sell_mv = excess_mv * allocation_ratio
                # 计算理论卖出量（向下取整到整百股）
                target_volume = int(round(target_sell_mv / (pos.cur_price * 100) + 0.5)) * 100
                available_volume = pos.available_volume
                # 确定实际可卖出量
                actual_volume = min(target_volume, available_volume)
                actual_volume = max(actual_volume, 0)  # 确保非负
                # 检查是否满足最小交易单位
                if actual_volume < 100:
                    self.logger.debug(f"{pos.symbol} 理论需卖出{target_volume}股，但可用不足100股")
                    continue
                # 生成市价单
                order = Order(
                    symbol=pos.symbol,
                    direction=OrderDirection.SELL,
                    price=pos.cur_price,
                    volume=actual_volume,
                    type=OrderType.MARKET
                )
                # 添加订单
                if self.engine.add_order(order):
                    orders_created += 1
                    # 更新剩余需减少市值
                    remaining_excess -= actual_volume * pos.cur_price
                    self.logger.info(
                        f"生成调仓卖单 | {pos.symbol} "
                        f"目标减持¥{target_sell_mv:,.2f} → 实际减持{actual_volume}股 "
                        f"(@{pos.cur_price:.2f} 合计¥{actual_volume*pos.cur_price:,.2f})"
                    )
            # 处理剩余敞口
            if remaining_excess > 0:
                self.logger.warning(
                    f"未能完全平衡风险敞口 | 剩余需减持 ¥{remaining_excess:,.2f} "
                    f"（完成度 {(excess_mv - remaining_excess)/excess_mv:.1%}）"
                )
            else:
                self.logger.info(
                    f"风险敞口已平衡 | 共生成 {orders_created} 个卖单 | "
                    f"实际减持 ¥{excess_mv - remaining_excess:,.2f}"
                )
            # 提交当日卖单
            self.engine.place_orders()
            # 等待订单全部执行完成
            self.engine.wait_orders_completion()
        except ZeroDivisionError:
            self.logger.error("计算过程中出现除零错误，可能持仓总市值为零")
        except KeyError as e:
            self.logger.error(f"持仓数据异常，缺失关键字段: {str(e)}")
        except Exception as e:
            self.logger.error(f"风险敞口再平衡失败: {str(e)}", exc_info=True)
            raise RuntimeError(f"风险敞口再平衡异常: {str(e)}")
    def save_snapshot(self, tag: str = None) -> str:
        """
        保存持仓及资金快照（自动压缩存储）
        :param tag: 快照标签（用于区分不同策略/时间点）
        :return: 生成的快照文件路径
        """
        try:
            # 创建快照目录
            os.makedirs(self._snapshot_file_path, exist_ok=True)
            # 构建快照数据
            snapshot_data = {
                "version": self.SNAPSHOT_VERSION,
                "timestamp": datetime.now().isoformat(),
                "available_cash": self.available_cash,
                "total_cash": self.total_cash,
                "positions": {
                    symbol: {
                        "total_volume": pos.total_volume,
                        "available_volume": pos.available_volume,
                        "avg_price": pos.avg_price,
                        "cur_price": pos.cur_price,
                        "market_value": round(pos.market_value, 2),
                        "float_pnl": pos.float_pnl,
                        "entry_date": pos.entry_date.strftime("%Y-%m-%d")
                    }
                    for symbol, pos in self._positions.items()
                }
            }
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.SNAPSHOT_PREFIX}_{timestamp}"
            if tag:
                filename += f"_{tag}"
            filepath = os.path.join(self._snapshot_file_path, f"{filename}.json.gz")
            # 将数据序列化为 JSON 并压缩
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                json.dump(snapshot_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"成功保存快照至 {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"快照保存失败: {str(e)}", exc_info=True)
            raise RuntimeError(f"无法保存快照: {str(e)}")
    def load_snapshot(self, filepath: str = None, latest: bool = True) -> None:
        """
        加载持仓快照
        :param filepath: 指定快照文件路径
        :param latest: 当未指定filepath时，加载最新快照
        """
        try:
            # 自动查找最新快照
            if not filepath and latest:
                filepath = self._find_latest_snapshot()
            if not os.path.exists(filepath):
                self.logger.warning(f"快照文件不存在: {filepath}")
                return
            # 解压并读取
            with gzip.open(filepath, 'r') as f:
                snapshot_data = json.load(f)
            # 版本兼容性检查
            if snapshot_data["version"] != self.SNAPSHOT_VERSION:
                self.logger.error(f"快照版本不匹配: {snapshot_data['version']} vs {self.SNAPSHOT_VERSION}")
            # 恢复核心数据
            self.available_cash = snapshot_data["available_cash"]
            self.total_cash = snapshot_data["total_cash"]
            self._positions.clear()
            # 恢复持仓
            for symbol, pos_data in snapshot_data["positions"].items():
                position = Position(symbol)
                position.total_volume = pos_data["total_volume"]
                position.available_volume = pos_data["available_volume"]
                position.avg_price = pos_data["avg_price"]
                position.cur_price = pos_data["cur_price"]
                position.market_value = pos_data["market_value"]
                position.float_pnl = pos_data["float_pnl"]
                position.entry_date = datetime.strptime(pos_data["entry_date"], "%Y-%m-%d").date()
                self._positions[symbol] = position
            self.logger.info(f"成功加载快照 {filepath}")
            self.display_equity()
            self.display_positions()
        except Exception as e:
            self.logger.error(f"快照加载失败: {str(e)}", exc_info=True)
            raise RuntimeError(f"无法加载快照: {str(e)}")
    def _find_latest_snapshot(self) -> str:
        """查找最新的快照文件"""
        try:
            files = [
                os.path.join(self._snapshot_file_path, f)
                for f in os.listdir(self._snapshot_file_path)
                if f.startswith(self.SNAPSHOT_PREFIX) and f.endswith(".json.gz")
            ]
            if not files:
                return ""
            latest_file = max(files, key=os.path.getctime)
            return latest_file
        except Exception as e:
            self.logger.error(f"查找最新快照失败: {str(e)}")
            raise
