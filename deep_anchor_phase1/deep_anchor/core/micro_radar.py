"""
deep_anchor/core/micro_radar.py
===============================
深海锚 - 模块二: 微观雷达与物理熔断器 (Micro Radar & Circuit Breaker)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
核心职责 (只吹哨，不动手):
  1. 接入币安永续合约 WebSocket，监听:
     - !forceOrder@arr   → 全市场强平订单流 (爆仓)
     - <symbol>@aggTrade  → 指定币种聚合成交流
  2. 滑动窗口聚合 (Rolling Window):
     - 过去 N 秒内累计爆仓名义价值 (USD)
     - 瞬时成交笔数 (TPS) 与基线 TPS 的比值
  3. 熔断判定 (Circuit Breaker):
     - 爆仓累计 > 阈值  → 触发
     - TPS 飙升 > 倍数   → 触发
     - 触发后进入冷却期，避免反复吹哨
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

设计原则:
  - 绝对解耦: 不含任何 01 DEX / 撤单逻辑
  - 信号输出: 通过 asyncio.Event + 可选回调通知外部
  - 健壮连接: 自动断线重连 + ping/pong 心跳保活

依赖:
  pip install websockets
"""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Deque, Optional, Tuple

try:
    import websockets
    import websockets.exceptions
    _HAS_WEBSOCKETS = True
except ImportError:
    _HAS_WEBSOCKETS = False

# 从配置导入 (运行时)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deep_anchor.config import MicroRadarConfig


logger = logging.getLogger("DeepAnchor.MicroRadar")


# ============================================================
#  一、数据结构
# ============================================================

class BreakerState(Enum):
    """熔断器状态机"""
    NORMAL   = auto()   # 正常监控中
    TRIPPED  = auto()   # 已触发，等待外部处理
    COOLDOWN = auto()   # 冷却期，不重复触发


@dataclass
class RadarSnapshot:
    """
    雷达实时快照 —— 每次评估时生成，可供外部读取或日志打印
    """
    timestamp: float = 0.0

    # 爆仓窗口
    liq_total_usd: float = 0.0          # 窗口内累计爆仓 (USD)
    liq_count: int = 0                  # 窗口内爆仓笔数
    liq_sell_usd: float = 0.0           # 其中卖方爆仓 (多头被爆)
    liq_buy_usd: float = 0.0            # 其中买方爆仓 (空头被爆)

    # 成交窗口
    tps_short: float = 0.0              # 短窗口 TPS (笔/秒)
    tps_baseline: float = 0.0           # 基线 TPS
    tps_ratio: float = 0.0              # 倍率 = short / baseline

    # 熔断器
    breaker_state: str = "NORMAL"
    trigger_reason: str = ""


class RollingWindow:
    """
    轻量级滑动时间窗口

    内部用 deque 存储 (timestamp, value) 对。
    每次读取时自动清理过期数据 (懒清理, O(k) k=过期数量)。
    """

    def __init__(self, max_age_sec: float):
        self._max_age = max_age_sec
        self._data: Deque[Tuple[float, float]] = deque()

    def add(self, value: float, ts: Optional[float] = None):
        """添加一个数据点"""
        now = ts if ts is not None else time.time()
        self._data.append((now, value))
        # 顺便做一次轻量清理 (避免无限增长)
        self._prune(now)

    def sum(self) -> float:
        """窗口内数值总和"""
        self._prune(time.time())
        return sum(v for _, v in self._data)

    def count(self) -> int:
        """窗口内数据点个数"""
        self._prune(time.time())
        return len(self._data)

    def rate_per_sec(self) -> float:
        """窗口内平均每秒数据点个数 (TPS)"""
        self._prune(time.time())
        n = len(self._data)
        if n == 0:
            return 0.0
        return n / self._max_age

    def clear(self):
        self._data.clear()

    def _prune(self, now: float):
        cutoff = now - self._max_age
        while self._data and self._data[0][0] < cutoff:
            self._data.popleft()


# ============================================================
#  二、微观雷达核心类
# ============================================================

class MicroRadar:
    """
    模块二: 微观雷达与物理熔断器

    生命周期:
      __init__  → 注入配置和回调
      run_forever → 连接 WebSocket, 持续监听, 自动重连
      (外部读取 self.snapshot 获取实时状态)
      (外部检查 self.circuit_break_event 判断是否触发)

    信号输出方式 (三选一或组合使用):
      1. asyncio.Event  → self.circuit_break_event.is_set()
      2. 回调函数       → on_circuit_break(snapshot)
      3. 轮询快照       → self.snapshot.breaker_state
    """

    def __init__(
        self,
        config: MicroRadarConfig = None,
        on_circuit_break: Optional[Callable[['RadarSnapshot'], None]] = None,
        exchange=None,    # 保留接口兼容，内部不使用
    ):
        self._config = config or MicroRadarConfig()
        self._on_circuit_break = on_circuit_break

        # --- 公开状态 ---
        self.snapshot = RadarSnapshot()
        self.circuit_break_event = asyncio.Event()

        # --- 熔断器状态机 ---
        self._breaker_state = BreakerState.NORMAL
        self._cooldown_until: float = 0.0       # 冷却期截止时间戳
        self._trip_count: int = 0               # 历史触发次数

        # --- 滑动窗口 ---
        self._liq_window = RollingWindow(self._config.window_seconds)
        self._liq_sell_window = RollingWindow(self._config.window_seconds)  # 多头被爆
        self._liq_buy_window = RollingWindow(self._config.window_seconds)   # 空头被爆
        self._trade_window_short = RollingWindow(self._config.window_seconds)
        self._trade_window_baseline = RollingWindow(self._config.tps_baseline_window_sec)

        # --- 连接状态 ---
        self._ws = None
        self._running = False
        self._reconnect_count = 0
        self._start_time: float = 0.0           # run_forever 启动时间 (用于预热期判定)

    # ────────────────────────────────────────────
    #  公开方法
    # ────────────────────────────────────────────

    async def run_forever(self):
        """
        主循环: 连接 → 监听 → 断线重连

        此方法永远不会主动返回 (除非 stop() 被调用)。
        """
        if not _HAS_WEBSOCKETS:
            logger.error(
                "❌ websockets 库未安装! 请执行: pip install websockets\n"
                "   模块二 (MicroRadar) 将处于禁用状态。"
            )
            # 不退出，静默等待 (不拖垮其他模块)
            while True:
                await asyncio.sleep(3600)

        if not self._config.enabled:
            logger.info("🔇 模块二 (MicroRadar) 已禁用")
            while True:
                await asyncio.sleep(3600)

        self._running = True
        self._start_time = time.time()
        warmup = self._config.tps_baseline_window_sec // 2
        logger.info(
            f"🔴 微观雷达启动 | "
            f"监听={self._config.watch_symbol.upper()} | "
            f"窗口={self._config.window_seconds}s | "
            f"爆仓阈值=${self._config.liquidation_threshold_usd:,.0f} | "
            f"TPS倍率={self._config.tps_spike_ratio}x | "
            f"预热期={warmup}s"
        )

        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                logger.info("⏹️ 微观雷达收到取消信号")
                break
            except Exception as e:
                self._reconnect_count += 1
                if self._reconnect_count > self._config.max_reconnect_attempts:
                    logger.error(f"❌ 超过最大重连次数 ({self._config.max_reconnect_attempts})，雷达停止")
                    break
                delay = min(self._config.reconnect_delay_sec * self._reconnect_count, 30)
                logger.warning(
                    f"⚠️ WebSocket 断开: {e} | "
                    f"第 {self._reconnect_count} 次重连，{delay:.1f}s 后..."
                )
                await asyncio.sleep(delay)

        self._running = False
        logger.info("🔇 微观雷达已停止")

    def stop(self):
        """优雅停止"""
        self._running = False

    def reset_breaker(self):
        """手动重置熔断器 (从 TRIPPED/COOLDOWN → NORMAL)"""
        self._breaker_state = BreakerState.NORMAL
        self._cooldown_until = 0.0
        self.circuit_break_event.clear()
        logger.info("🔄 熔断器已手动重置 → NORMAL")

    def inject_liquidation(self, notional_usd: float, side: str = "SELL"):
        """
        手动注入爆仓事件 (用于测试)

        Args:
            notional_usd: 爆仓名义价值 (USD)
            side: "SELL" = 多头被爆, "BUY" = 空头被爆
        """
        now = time.time()
        self._liq_window.add(notional_usd, now)
        if side.upper() == "SELL":
            self._liq_sell_window.add(notional_usd, now)
        else:
            self._liq_buy_window.add(notional_usd, now)
        self._evaluate()

    def inject_trades(self, count: int = 1):
        """手动注入成交事件 (用于测试)"""
        now = time.time()
        for _ in range(count):
            self._trade_window_short.add(1.0, now)
            self._trade_window_baseline.add(1.0, now)
        self._evaluate()

    # ────────────────────────────────────────────
    #  WebSocket 连接与消息处理
    # ────────────────────────────────────────────

    async def _connect_and_listen(self):
        """建立 WebSocket 连接并进入消息循环"""
        symbol = self._config.watch_symbol.lower()

        # 使用组合流 (Combined Streams) 同时监听爆仓 + 聚合成交
        streams = f"!forceOrder@arr/{symbol}@aggTrade"
        url = f"{self._config.ws_base_url}/stream?streams={streams}"

        logger.info(f"📡 连接 WebSocket: {url}")

        async with websockets.connect(
            url,
            ping_interval=self._config.ping_interval_sec,
            ping_timeout=self._config.ping_interval_sec * 2,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            self._reconnect_count = 0   # 连接成功，重置计数
            logger.info("✅ WebSocket 连接成功，开始监听...")

            async for raw_msg in ws:
                if not self._running:
                    break
                try:
                    self._handle_message(raw_msg)
                except Exception as e:
                    logger.debug(f"消息处理异常: {e}")

    def _handle_message(self, raw_msg: str):
        """
        解析币安 Combined Stream 消息并路由到对应处理器

        消息格式:
        {
          "stream": "!forceOrder@arr" | "btcusdt@aggTrade",
          "data": { ... }
        }
        """
        msg = json.loads(raw_msg)
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if "forceOrder" in stream:
            self._on_liquidation(data)
        elif "aggTrade" in stream:
            self._on_agg_trade(data)

        # 每条消息后评估一次熔断条件
        self._evaluate()

    def _on_liquidation(self, data: dict):
        """
        处理强平订单消息

        币安 forceOrder 消息体 (data.o):
        {
          "s": "BTCUSDT",       # 交易对
          "S": "SELL",           # 方向: SELL=多头被爆, BUY=空头被爆
          "o": "LIMIT",          # 订单类型
          "f": "IOC",            # 有效方式
          "q": "0.014",          # 数量
          "p": "67000.00",       # 价格
          "ap": "66950.20",      # 平均价
          "X": "FILLED",         # 状态
          "l": "0.014",          # 最新成交量
          "z": "0.014",          # 累计成交量
          "T": 1234567890123     # 时间戳
        }
        """
        order = data.get("o", data)  # 兼容两种格式
        try:
            qty = float(order.get("q", 0))
            price = float(order.get("p", 0)) or float(order.get("ap", 0))
            side = order.get("S", "SELL")
            notional = qty * price
        except (ValueError, TypeError):
            return

        if notional <= 0:
            return

        now = time.time()
        self._liq_window.add(notional, now)
        if side == "SELL":
            self._liq_sell_window.add(notional, now)
        else:
            self._liq_buy_window.add(notional, now)

    def _on_agg_trade(self, data: dict):
        """
        处理聚合成交消息

        币安 aggTrade 消息体:
        {
          "e": "aggTrade",
          "s": "BTCUSDT",
          "p": "67000.50",   # 价格
          "q": "0.100",      # 数量
          "T": 1234567890123 # 时间戳
        }
        """
        now = time.time()
        self._trade_window_short.add(1.0, now)
        self._trade_window_baseline.add(1.0, now)

    # ────────────────────────────────────────────
    #  熔断评估引擎
    # ────────────────────────────────────────────

    def _evaluate(self):
        """
        核心熔断判定逻辑 —— 每条消息后调用一次

        状态机转移:
          NORMAL  → (阈值突破) → TRIPPED
          TRIPPED → (冷却到期) → NORMAL
          COOLDOWN → (冷却到期) → NORMAL
        """
        now = time.time()

        # --- 采集快照 ---
        liq_total = self._liq_window.sum()
        liq_count = self._liq_window.count()
        liq_sell = self._liq_sell_window.sum()
        liq_buy = self._liq_buy_window.sum()

        tps_short = self._trade_window_short.rate_per_sec()
        tps_baseline = self._trade_window_baseline.rate_per_sec()

        # ═══ 预热期保护 ═══
        # 基线窗口 (默认60s) 至少积累一半时间的数据才有统计意义。
        # 否则分母太小，正常的几笔成交就会算出超高倍率导致误触发。
        # 同时要求基线 TPS >= 2.0 (即至少每秒 2 笔的市场活跃度)。
        warmup_elapsed = now - self._start_time if self._start_time > 0 else 0
        warmup_needed = self._config.tps_baseline_window_sec / 2
        tps_warmed_up = (warmup_elapsed >= warmup_needed) and (tps_baseline >= 2.0)
        tps_ratio = (tps_short / tps_baseline) if tps_warmed_up else 0.0

        self.snapshot = RadarSnapshot(
            timestamp=now,
            liq_total_usd=liq_total,
            liq_count=liq_count,
            liq_sell_usd=liq_sell,
            liq_buy_usd=liq_buy,
            tps_short=tps_short,
            tps_baseline=tps_baseline,
            tps_ratio=tps_ratio,
            breaker_state=self._breaker_state.name,
        )

        # --- 冷却期检查 ---
        if self._breaker_state in (BreakerState.TRIPPED, BreakerState.COOLDOWN):
            if now >= self._cooldown_until:
                self._breaker_state = BreakerState.NORMAL
                self.circuit_break_event.clear()
                self.snapshot.breaker_state = "NORMAL"  # 同步快照
                logger.info("✅ 熔断器冷却结束 → NORMAL")
            else:
                return  # 冷却中，跳过评估

        # --- NORMAL 状态下的阈值检测 ---
        trigger_reason = ""

        # 检测一: 爆仓累计突破
        if liq_total >= self._config.liquidation_threshold_usd:
            trigger_reason = (
                f"💥 爆仓熔断: 窗口内累计 ${liq_total:,.0f} "
                f"≥ 阈值 ${self._config.liquidation_threshold_usd:,.0f} "
                f"(卖方/多头爆: ${liq_sell:,.0f} | 买方/空头爆: ${liq_buy:,.0f})"
            )

        # 检测二: TPS 飙升突破
        if not trigger_reason and tps_ratio >= self._config.tps_spike_ratio:
            trigger_reason = (
                f"📈 TPS熔断: 瞬时TPS {tps_short:.1f} / 基线TPS {tps_baseline:.1f} "
                f"= {tps_ratio:.1f}x ≥ 阈值 {self._config.tps_spike_ratio}x"
            )

        # --- 触发熔断 ---
        if trigger_reason:
            self._trip_count += 1
            self._breaker_state = BreakerState.TRIPPED
            self._cooldown_until = now + self._config.cooldown_seconds
            self.snapshot.breaker_state = "TRIPPED"
            self.snapshot.trigger_reason = trigger_reason

            # 信号输出 1: asyncio.Event
            self.circuit_break_event.set()

            # 信号输出 2: 回调
            if self._on_circuit_break:
                try:
                    self._on_circuit_break(self.snapshot)
                except Exception as cb_err:
                    logger.error(f"熔断回调异常: {cb_err}")

            logger.warning(
                f"🚨 ══ 熔断器触发 #{self._trip_count} ══\n"
                f"   原因: {trigger_reason}\n"
                f"   冷却: {self._config.cooldown_seconds}s"
            )

            # 进入冷却
            self._breaker_state = BreakerState.COOLDOWN
