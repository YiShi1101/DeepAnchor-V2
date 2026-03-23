"""
deep_anchor/core/exit_manager.py
================================
深海锚 - 模块三: 状态维持与平仓管家 (Exit Manager)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
核心职责 (管账本，发指令，不动手):

  1. 挂单池 (Order Pool)
     - 跟踪所有活跃挂单的生命周期
     - 超过 TTL 仍未成交 → 输出 CANCEL_ORDER 动作

  2. 持仓台 (Position Desk)
     - 跟踪已成交仓位的入场价和浮盈
     - 最新价触及止盈线 → 输出 CLOSE_POSITION 动作
     - 最新价击穿止损线 → 输出 CLOSE_POSITION 动作
     - 持仓超时未动     → 输出 CLOSE_POSITION 动作

  3. 防滑点
     - 所有平仓指令强制 IOC 类型
     - 限价 = mark_price ± slippage_tolerance

信号输出:
  - asyncio.Queue: 外部消费者从 self.action_queue 取动作
  - 可选回调: on_action(ExitAction) 即时通知

设计原则:
  - 绝对解耦: 不含任何网络请求/交易所代码
  - 外部喂数据: register_order / update_price / notify_fill
  - 外部取指令: action_queue.get() / on_action 回调
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Dict, List, Optional

from ..config import ExitManagerConfig


logger = logging.getLogger("DeepAnchor.ExitManager")


# ============================================================
#  一、数据结构
# ============================================================

class ActionType(Enum):
    """管家输出的动作类型"""
    CANCEL_ORDER   = auto()     # 撤销超时挂单
    CLOSE_POSITION = auto()     # 平仓 (止盈/止损/超时)


class CloseReason(Enum):
    """平仓原因"""
    TAKE_PROFIT    = "TAKE_PROFIT"
    STOP_LOSS      = "STOP_LOSS"
    TIMEOUT        = "TIMEOUT"
    MANUAL         = "MANUAL"
    CIRCUIT_BREAK  = "CIRCUIT_BREAK"    # 由模块二熔断触发


@dataclass
class TrackedOrder:
    """
    被追踪的活跃挂单

    外部通过 register_order() 注册，管家追踪其 TTL。
    """
    order_id: str
    symbol: str
    side: str                   # "BUY" / "SELL"
    price: float
    size: float
    created_at: float           # time.time() 注册时间
    ttl_sec: float              # 存活时间上限 (秒)
    zone_name: str = ""         # 来自哪个防线层级 (可选标签)

    @property
    def age_sec(self) -> float:
        """已存活时长 (秒)"""
        return time.time() - self.created_at

    @property
    def is_expired(self) -> bool:
        """是否超过 TTL"""
        return self.age_sec > self.ttl_sec


@dataclass
class TrackedPosition:
    """
    被追踪的持仓

    外部通过 register_position() 注册，管家追踪其止盈止损。
    """
    symbol: str
    side: str                   # "LONG" / "SHORT"
    size: float                 # 仓位大小 (绝对值)
    entry_price: float          # 入场均价
    opened_at: float            # 开仓时间 (time.time())

    # 止盈止损价格 (由管家根据配置自动计算)
    take_profit_price: float = 0.0
    stop_loss_price: float = 0.0

    # 关联的订单 ID (如有)
    source_order_id: str = ""

    @property
    def hold_time_sec(self) -> float:
        """持仓时长 (秒)"""
        return time.time() - self.opened_at


@dataclass
class ExitAction:
    """
    管家输出的标准化动作指令

    外部执行器 (Executor) 消费此对象并翻译为实际交易所操作。
    """
    action_type: ActionType
    symbol: str
    timestamp: float = 0.0

    # --- CANCEL_ORDER 专用 ---
    order_id: str = ""
    cancel_reason: str = ""

    # --- CLOSE_POSITION 专用 ---
    close_side: str = ""        # 平仓方向: 多头持仓 → "SELL"，空头持仓 → "BUY"
    close_size: float = 0.0
    close_price: float = 0.0    # IOC 限价 (含滑点容忍)
    close_reason: CloseReason = CloseReason.MANUAL
    order_type: str = "IOC"     # 强制 IOC

    # 触发时的市场价 (用于日志/审计)
    trigger_price: float = 0.0
    entry_price: float = 0.0
    unrealized_pnl_pct: float = 0.0


# ============================================================
#  二、平仓管家核心类
# ============================================================

class ExitManager:
    """
    模块三: 状态维持与平仓管家

    使用方式:
      1. 初始化:  mgr = ExitManager(config, on_action=callback)
      2. 喂数据:  mgr.register_order(...)     # 新挂单入池
                  mgr.notify_fill(order_id)    # 挂单成交，从池中移除
                  mgr.register_position(...)   # 新持仓入台
                  mgr.update_price(symbol, p)  # 喂最新价
      3. 取指令:  action = await mgr.action_queue.get()
                  或通过 on_action 回调即时接收
      4. 主循环:  await mgr.run_forever()      # 并入 asyncio.gather
    """

    def __init__(
        self,
        config: ExitManagerConfig = None,
        on_action: Optional[Callable[[ExitAction], None]] = None,
        exchange=None,    # 保留接口兼容，内部不使用
    ):
        self._config = config or ExitManagerConfig()
        self._on_action = on_action

        # --- 挂单池: order_id → TrackedOrder ---
        self._orders: Dict[str, TrackedOrder] = {}

        # --- 持仓台: symbol → TrackedPosition ---
        # (简化设计: 同一交易对同时只有一个方向的仓位)
        self._positions: Dict[str, TrackedPosition] = {}

        # --- 最新价缓存: symbol → price ---
        self._last_prices: Dict[str, float] = {}

        # --- 输出队列 ---
        self.action_queue: asyncio.Queue[ExitAction] = asyncio.Queue()

        # --- 统计 ---
        self._cancel_count: int = 0
        self._close_count: int = 0
        self._running: bool = False

    # ────────────────────────────────────────────
    #  外部输入接口 (喂数据)
    # ────────────────────────────────────────────

    def register_order(
        self,
        order_id: str,
        symbol: str,
        side: str,
        price: float,
        size: float,
        ttl_sec: float = None,
        zone_name: str = "",
    ):
        """
        注册一个新的活跃挂单到追踪池

        Args:
            order_id: 交易所返回的订单 ID
            ttl_sec: 自定义 TTL (默认使用配置值)
        """
        ttl = ttl_sec if ttl_sec is not None else self._config.order_ttl_sec
        order = TrackedOrder(
            order_id=order_id,
            symbol=symbol,
            side=side,
            price=price,
            size=size,
            created_at=time.time(),
            ttl_sec=ttl,
            zone_name=zone_name,
        )
        self._orders[order_id] = order
        logger.debug(f"📋 订单入池: {order_id} | {side} {symbol} @ {price} | TTL={ttl}s")

    def remove_order(self, order_id: str):
        """从追踪池中移除订单 (不触发撤单动作)"""
        removed = self._orders.pop(order_id, None)
        if removed:
            logger.debug(f"📋 订单移除: {order_id}")

    def notify_fill(self, order_id: str, fill_price: float = 0.0, fill_size: float = 0.0):
        """
        通知挂单已成交

        成交后自动从挂单池移除。
        如果提供了 fill_price/fill_size，可自动创建持仓追踪。
        """
        order = self._orders.pop(order_id, None)
        if order:
            logger.info(
                f"✅ 挂单成交: {order_id} | {order.side} {order.symbol} "
                f"@ {fill_price or order.price} x {fill_size or order.size}"
            )

    def register_position(
        self,
        symbol: str,
        side: str,
        size: float,
        entry_price: float,
        source_order_id: str = "",
    ):
        """
        注册一个新的持仓到追踪台

        自动计算止盈止损价格。
        """
        tp_pct = self._config.take_profit_pct
        sl_pct = self._config.stop_loss_pct

        if side.upper() == "LONG":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else:  # SHORT
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        pos = TrackedPosition(
            symbol=symbol,
            side=side.upper(),
            size=size,
            entry_price=entry_price,
            opened_at=time.time(),
            take_profit_price=round(tp_price, 2),
            stop_loss_price=round(sl_price, 2),
            source_order_id=source_order_id,
        )
        self._positions[symbol] = pos

        logger.info(
            f"📊 持仓登记: {side} {symbol} | "
            f"入场={entry_price:,.2f} | "
            f"止盈={pos.take_profit_price:,.2f} ({tp_pct*100:.1f}%) | "
            f"止损={pos.stop_loss_price:,.2f} ({sl_pct*100:.1f}%) | "
            f"超时={self._config.max_hold_time_sec}s"
        )

    def remove_position(self, symbol: str):
        """从持仓台移除 (平仓完成后调用)"""
        removed = self._positions.pop(symbol, None)
        if removed:
            logger.debug(f"📊 持仓移除: {symbol}")

    def update_price(self, symbol: str, price: float):
        """
        喂入最新市场价格

        每次价格更新后会立即检查该 symbol 的持仓止盈止损。
        """
        self._last_prices[symbol] = price

    def force_close_all(self, reason: CloseReason = CloseReason.CIRCUIT_BREAK):
        """
        强制平仓所有持仓 (供模块二熔断器调用)
        """
        for symbol, pos in list(self._positions.items()):
            price = self._last_prices.get(symbol, pos.entry_price)
            self._emit_close(pos, price, reason)

    # ────────────────────────────────────────────
    #  主循环
    # ────────────────────────────────────────────

    async def run_forever(self):
        """
        主循环: 定期扫描挂单池和持仓台
        """
        if not self._config.enabled:
            logger.info("🔇 模块三 (ExitManager) 已禁用")
            while True:
                await asyncio.sleep(3600)

        self._running = True
        logger.info(
            f"🛡️ 平仓管家启动 | "
            f"挂单TTL={self._config.order_ttl_sec}s | "
            f"止盈={self._config.take_profit_pct*100:.1f}% | "
            f"止损={self._config.stop_loss_pct*100:.1f}% | "
            f"超时={self._config.max_hold_time_sec}s | "
            f"平仓类型={self._config.close_order_type}"
        )

        while self._running:
            try:
                self.tick()
            except Exception as e:
                logger.error(f"❌ 管家 tick 异常: {e}", exc_info=True)
            await asyncio.sleep(self._config.check_interval_sec)

    def stop(self):
        self._running = False

    def tick(self) -> List[ExitAction]:
        """
        单次扫描周期 (同步方法，可独立调用用于测试)

        扫描顺序:
          1. 挂单池 → 找出超时订单 → 输出 CANCEL_ORDER
          2. 持仓台 → 检查止盈止损和超时 → 输出 CLOSE_POSITION

        Returns:
            本轮产生的动作列表 (也会同时放入 action_queue)
        """
        actions: List[ExitAction] = []

        # --- 扫描挂单池 ---
        expired_ids = [
            oid for oid, order in self._orders.items()
            if order.is_expired
        ]
        for oid in expired_ids:
            order = self._orders.pop(oid)
            action = ExitAction(
                action_type=ActionType.CANCEL_ORDER,
                symbol=order.symbol,
                timestamp=time.time(),
                order_id=order.order_id,
                cancel_reason=(
                    f"TTL 超时: {order.age_sec:.1f}s > {order.ttl_sec}s | "
                    f"{order.side} @ {order.price}"
                ),
            )
            actions.append(action)
            self._emit(action)
            self._cancel_count += 1
            logger.info(
                f"⏰ 撤单: {order.order_id} | {order.zone_name} "
                f"{order.side} @ {order.price} | 存活 {order.age_sec:.1f}s"
            )

        # --- 扫描持仓台 ---
        for symbol in list(self._positions.keys()):
            pos = self._positions.get(symbol)
            if not pos:
                continue

            price = self._last_prices.get(symbol)
            if price is None:
                continue    # 尚无价格数据，跳过

            close_reason = self._check_position(pos, price)
            if close_reason:
                action = self._emit_close(pos, price, close_reason)
                if action:
                    actions.append(action)

        return actions

    # ────────────────────────────────────────────
    #  内部: 持仓检查逻辑
    # ────────────────────────────────────────────

    def _check_position(self, pos: TrackedPosition, price: float) -> Optional[CloseReason]:
        """
        检查单个持仓是否触及退出条件

        检查优先级: 止损 > 止盈 > 超时
        (止损优先级最高，因为风控最重要)
        """
        if pos.side == "LONG":
            # 多头: 价格跌破止损线
            if price <= pos.stop_loss_price:
                return CloseReason.STOP_LOSS
            # 多头: 价格涨过止盈线
            if price >= pos.take_profit_price:
                return CloseReason.TAKE_PROFIT

        elif pos.side == "SHORT":
            # 空头: 价格涨破止损线
            if price >= pos.stop_loss_price:
                return CloseReason.STOP_LOSS
            # 空头: 价格跌过止盈线
            if price <= pos.take_profit_price:
                return CloseReason.TAKE_PROFIT

        # 超时检查
        if pos.hold_time_sec >= self._config.max_hold_time_sec:
            return CloseReason.TIMEOUT

        return None

    def _emit_close(
        self,
        pos: TrackedPosition,
        trigger_price: float,
        reason: CloseReason,
    ) -> Optional[ExitAction]:
        """
        生成平仓动作并发射

        自动计算:
        - 平仓方向 (多头 → 卖出，空头 → 买入)
        - IOC 限价 (mark ± slippage_tolerance)
        - 浮盈百分比
        """
        # 平仓方向
        if pos.side == "LONG":
            close_side = "SELL"
            # 卖出平多: 限价略低于当前价 (愿意少亏一点成交)
            close_price = trigger_price * (1 - self._config.slippage_tolerance_pct)
        else:
            close_side = "BUY"
            # 买入平空: 限价略高于当前价
            close_price = trigger_price * (1 + self._config.slippage_tolerance_pct)

        close_price = round(close_price, 2)

        # 计算浮盈
        if pos.side == "LONG":
            pnl_pct = (trigger_price - pos.entry_price) / pos.entry_price
        else:
            pnl_pct = (pos.entry_price - trigger_price) / pos.entry_price

        action = ExitAction(
            action_type=ActionType.CLOSE_POSITION,
            symbol=pos.symbol,
            timestamp=time.time(),
            close_side=close_side,
            close_size=pos.size,
            close_price=close_price,
            close_reason=reason,
            order_type=self._config.close_order_type,
            trigger_price=trigger_price,
            entry_price=pos.entry_price,
            unrealized_pnl_pct=pnl_pct,
        )

        self._emit(action)
        self._close_count += 1

        # 从持仓台移除 (已发出平仓指令)
        self._positions.pop(pos.symbol, None)

        # 日志
        reason_icons = {
            CloseReason.TAKE_PROFIT: "🎯",
            CloseReason.STOP_LOSS: "🛑",
            CloseReason.TIMEOUT: "⏰",
            CloseReason.CIRCUIT_BREAK: "🚨",
            CloseReason.MANUAL: "🔧",
        }
        icon = reason_icons.get(reason, "❓")

        logger.warning(
            f"{icon} 平仓: {pos.side} {pos.symbol} | "
            f"原因={reason.value} | "
            f"入场={pos.entry_price:,.2f} → 触发={trigger_price:,.2f} | "
            f"盈亏={pnl_pct:+.2%} | "
            f"IOC限价={close_price:,.2f} | "
            f"持仓={pos.hold_time_sec:.1f}s"
        )

        return action

    # ────────────────────────────────────────────
    #  内部: 输出动作
    # ────────────────────────────────────────────

    def _emit(self, action: ExitAction):
        """发射动作到队列和回调"""
        # 队列 (异步消费)
        try:
            self.action_queue.put_nowait(action)
        except asyncio.QueueFull:
            logger.warning("⚠️ 动作队列已满，丢弃最旧的")
            try:
                self.action_queue.get_nowait()
                self.action_queue.put_nowait(action)
            except Exception:
                pass

        # 回调 (同步即时)
        if self._on_action:
            try:
                self._on_action(action)
            except Exception as e:
                logger.error(f"回调异常: {e}")

    # ────────────────────────────────────────────
    #  只读状态查询
    # ────────────────────────────────────────────

    @property
    def tracked_order_count(self) -> int:
        return len(self._orders)

    @property
    def tracked_position_count(self) -> int:
        return len(self._positions)

    @property
    def stats(self) -> dict:
        """当前统计摘要"""
        return {
            "active_orders": len(self._orders),
            "active_positions": len(self._positions),
            "total_cancels": self._cancel_count,
            "total_closes": self._close_count,
        }

    def get_position(self, symbol: str) -> Optional[TrackedPosition]:
        return self._positions.get(symbol)

    def get_all_orders(self) -> List[TrackedOrder]:
        return list(self._orders.values())

    def get_all_positions(self) -> List[TrackedPosition]:
        return list(self._positions.values())
