"""
deep_anchor/core/performance.py
================================
深海锚 - 模块五: 效能监控与参数调试 (Performance & Tuning)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
三大功能:

  1. API 延迟监控 (LatencyTracker)
     - 上下文管理器包裹 API 调用，记录耗时
     - 超阈值自动 WARNING
     - 计算 avg / p50 / p95 / p99 / max

  2. 影子撮合引擎 (ShadowEngine)
     - 接收模拟挂单 + 最新市场价
     - 本地判断价格是否击穿挂单价 → 模拟成交
     - 模拟止盈止损 → 计算理论盈亏

  3. 调参报告 (TuningReport)
     - 延迟统计 (avg/p95/max)
     - 影子战绩 (胜率、盈亏比、各层级触发次数)
     - 文本格式一键输出

设计原则: 纯工具箱，零业务依赖，外部按需调用。
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional

from ..config import PerformanceConfig


logger = logging.getLogger("DeepAnchor.Performance")


# ============================================================
#  一、API 延迟追踪器
# ============================================================

class LatencyTracker:
    """
    轻量级 API 延迟追踪器

    两种使用方式:
        # 上下文管理器
        async with tracker.measure("place_order"):
            await exchange.place_order(...)

        # 手动记录
        tracker.record("get_ticker", elapsed_ms=123.4)
    """

    def __init__(self, warn_threshold_ms: float = 500.0, max_history: int = 1000):
        self._warn_ms = warn_threshold_ms
        self._max_history = max_history

        # 全局延迟记录 (所有 API 合计)
        self._all_latencies: Deque[float] = deque(maxlen=max_history)

        # 按 API 名称分组
        self._by_name: Dict[str, Deque[float]] = {}

        # 计数器
        self._total_calls: int = 0
        self._slow_calls: int = 0

    def measure(self, api_name: str = "unknown") -> '_LatencyContext':
        """返回上下文管理器，自动计时"""
        return _LatencyContext(self, api_name)

    def record(self, api_name: str, elapsed_ms: float):
        """手动记录一次 API 延迟"""
        self._total_calls += 1
        self._all_latencies.append(elapsed_ms)

        if api_name not in self._by_name:
            self._by_name[api_name] = deque(maxlen=self._max_history)
        self._by_name[api_name].append(elapsed_ms)

        if elapsed_ms > self._warn_ms:
            self._slow_calls += 1
            logger.warning(f"🐌 慢调用: {api_name} = {elapsed_ms:.1f}ms (阈值 {self._warn_ms}ms)")

    def stats(self, api_name: str = None) -> dict:
        """
        计算延迟统计

        Args:
            api_name: 指定 API 名称，None = 全局统计

        Returns:
            {"count", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms", "slow_count"}
        """
        data = list(self._by_name.get(api_name, [])) if api_name else list(self._all_latencies)
        if not data:
            return {
                "count": 0, "avg_ms": 0, "p50_ms": 0,
                "p95_ms": 0, "p99_ms": 0, "max_ms": 0,
                "slow_count": self._slow_calls,
            }

        data_sorted = sorted(data)
        n = len(data_sorted)

        return {
            "count": n,
            "avg_ms": round(sum(data_sorted) / n, 2),
            "p50_ms": round(data_sorted[int(n * 0.50)], 2),
            "p95_ms": round(data_sorted[min(int(n * 0.95), n - 1)], 2),
            "p99_ms": round(data_sorted[min(int(n * 0.99), n - 1)], 2),
            "max_ms": round(data_sorted[-1], 2),
            "slow_count": self._slow_calls,
        }

    @property
    def api_names(self) -> List[str]:
        return list(self._by_name.keys())

    def reset(self):
        self._all_latencies.clear()
        self._by_name.clear()
        self._total_calls = 0
        self._slow_calls = 0


class _LatencyContext:
    """上下文管理器: 自动计时并记录"""

    def __init__(self, tracker: LatencyTracker, api_name: str):
        self._tracker = tracker
        self._name = api_name
        self._start = 0.0

    async def __aenter__(self):
        self._start = time.monotonic()
        return self

    async def __aexit__(self, *exc):
        elapsed = (time.monotonic() - self._start) * 1000  # → ms
        self._tracker.record(self._name, elapsed)
        return False


# ============================================================
#  二、影子撮合引擎
# ============================================================

@dataclass
class ShadowOrder:
    """影子挂单"""
    order_id: str
    symbol: str
    side: str               # "BUY" / "SELL"
    price: float
    size: float
    zone_name: str = ""     # 层级标签
    created_at: float = 0.0

    def __post_init__(self):
        if self.created_at == 0:
            self.created_at = time.time()


@dataclass
class ShadowFill:
    """影子成交记录"""
    order_id: str
    symbol: str
    side: str
    fill_price: float       # 成交价 (= 触发时的 mark price)
    size: float
    zone_name: str
    filled_at: float
    # 平仓结果 (由引擎在模拟止盈止损后填入)
    exit_price: float = 0.0
    pnl_pct: float = 0.0
    close_reason: str = ""  # "TAKE_PROFIT" / "STOP_LOSS" / "OPEN"


class ShadowEngine:
    """
    影子撮合引擎 —— 本地模拟挂单成交

    使用方式:
        engine = ShadowEngine(tp_pct=0.05, sl_pct=0.02)
        engine.place_order(ShadowOrder(...))
        engine.update_price("BTC-PERP", 67500.0)   # 喂价，自动撮合
        engine.update_price("BTC-PERP", 68000.0)   # 继续喂，检查止盈止损
        report = engine.summary()
    """

    def __init__(self, tp_pct: float = 0.05, sl_pct: float = 0.02):
        self._tp_pct = tp_pct
        self._sl_pct = sl_pct

        # 待成交挂单池: order_id → ShadowOrder
        self._pending: Dict[str, ShadowOrder] = {}

        # 已成交但未平仓的持仓: order_id → ShadowFill
        self._open_positions: Dict[str, ShadowFill] = {}

        # 已完结的交易 (含盈亏)
        self._closed: List[ShadowFill] = []

    def place_order(self, order: ShadowOrder):
        """放入一个影子挂单"""
        self._pending[order.order_id] = order

    def cancel_order(self, order_id: str):
        """取消影子挂单"""
        self._pending.pop(order_id, None)

    def update_price(self, symbol: str, price: float):
        """
        喂入最新市场价，触发两阶段撮合:

        阶段 1: 检查挂单是否被击穿 → 模拟成交
        阶段 2: 检查持仓是否触及止盈止损 → 模拟平仓
        """
        now = time.time()

        # --- 阶段 1: 挂单撮合 ---
        filled_ids = []
        for oid, order in self._pending.items():
            if order.symbol != symbol:
                continue
            # 买单: 市场价 <= 挂单价 → 成交
            if order.side == "BUY" and price <= order.price:
                filled_ids.append(oid)
            # 卖单: 市场价 >= 挂单价 → 成交
            elif order.side == "SELL" and price >= order.price:
                filled_ids.append(oid)

        for oid in filled_ids:
            order = self._pending.pop(oid)
            fill = ShadowFill(
                order_id=oid,
                symbol=order.symbol,
                side=order.side,
                fill_price=price,
                size=order.size,
                zone_name=order.zone_name,
                filled_at=now,
                close_reason="OPEN",
            )
            self._open_positions[oid] = fill

        # --- 阶段 2: 持仓止盈止损 ---
        closed_ids = []
        for oid, pos in self._open_positions.items():
            if pos.symbol != symbol:
                continue

            if pos.side == "BUY":
                # 买单成交 → 多头持仓
                pnl = (price - pos.fill_price) / pos.fill_price
                if pnl >= self._tp_pct:
                    pos.exit_price = price
                    pos.pnl_pct = pnl
                    pos.close_reason = "TAKE_PROFIT"
                    closed_ids.append(oid)
                elif pnl <= -self._sl_pct:
                    pos.exit_price = price
                    pos.pnl_pct = pnl
                    pos.close_reason = "STOP_LOSS"
                    closed_ids.append(oid)
            else:
                # 卖单成交 → 空头持仓
                pnl = (pos.fill_price - price) / pos.fill_price
                if pnl >= self._tp_pct:
                    pos.exit_price = price
                    pos.pnl_pct = pnl
                    pos.close_reason = "TAKE_PROFIT"
                    closed_ids.append(oid)
                elif pnl <= -self._sl_pct:
                    pos.exit_price = price
                    pos.pnl_pct = pnl
                    pos.close_reason = "STOP_LOSS"
                    closed_ids.append(oid)

        for oid in closed_ids:
            pos = self._open_positions.pop(oid)
            self._closed.append(pos)

    def summary(self) -> dict:
        """
        影子交易汇总

        Returns:
            {
              "pending_orders": int,
              "open_positions": int,
              "closed_trades": int,
              "wins": int, "losses": int, "win_rate": float,
              "total_pnl_pct": float, "avg_pnl_pct": float,
              "best_pnl_pct": float, "worst_pnl_pct": float,
              "by_zone": { "浅水区": {"fills": N, "wins": N, ...}, ... },
              "by_reason": { "TAKE_PROFIT": N, "STOP_LOSS": N },
            }
        """
        closed = self._closed
        wins = [t for t in closed if t.pnl_pct > 0]
        losses = [t for t in closed if t.pnl_pct <= 0]
        pnls = [t.pnl_pct for t in closed]

        # 按层级分组
        by_zone: Dict[str, dict] = {}
        for t in closed:
            z = t.zone_name or "未知"
            if z not in by_zone:
                by_zone[z] = {"fills": 0, "wins": 0, "losses": 0, "total_pnl": 0.0}
            by_zone[z]["fills"] += 1
            by_zone[z]["total_pnl"] += t.pnl_pct
            if t.pnl_pct > 0:
                by_zone[z]["wins"] += 1
            else:
                by_zone[z]["losses"] += 1

        # 按原因分组
        by_reason: Dict[str, int] = {}
        for t in closed:
            r = t.close_reason
            by_reason[r] = by_reason.get(r, 0) + 1

        return {
            "pending_orders": len(self._pending),
            "open_positions": len(self._open_positions),
            "closed_trades": len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(closed) if closed else 0.0,
            "total_pnl_pct": sum(pnls) if pnls else 0.0,
            "avg_pnl_pct": sum(pnls) / len(pnls) if pnls else 0.0,
            "best_pnl_pct": max(pnls) if pnls else 0.0,
            "worst_pnl_pct": min(pnls) if pnls else 0.0,
            "by_zone": by_zone,
            "by_reason": by_reason,
        }

    def reset(self):
        self._pending.clear()
        self._open_positions.clear()
        self._closed.clear()


# ============================================================
#  三、调参报告生成器
# ============================================================

def generate_report(
    latency: LatencyTracker,
    shadow: ShadowEngine,
    title: str = "深海锚 调参报告",
) -> str:
    """
    生成文本格式的调参报告

    Returns:
        排版好的多行字符串，可直接 print 或写入日志
    """
    lines = []
    w = 62  # 报告宽度

    # ---- 标题 ----
    lines.append("╔" + "═" * w + "╗")
    lines.append(f"║{title:^{w}}║")
    lines.append(f"║{'生成时间: ' + time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()):^{w}}║")
    lines.append("╠" + "═" * w + "╣")

    # ---- 延迟统计 ----
    lines.append(f"║{'📡 API 延迟统计':^{w}}║")
    lines.append("║" + "─" * w + "║")

    gs = latency.stats()
    lines.append(
        f"║  总调用: {gs['count']:>6d}   │ 慢调用: {gs['slow_count']:>4d}   │"
        f" 慢调用率: {gs['slow_count']/max(gs['count'],1)*100:>5.1f}%        ║"
    )
    lines.append(
        f"║  平均:  {gs['avg_ms']:>7.1f}ms │ P50: {gs['p50_ms']:>7.1f}ms │"
        f" P95: {gs['p95_ms']:>7.1f}ms │ Max: {gs['max_ms']:>7.1f}ms ║"
    )

    # 按 API 细分
    for name in latency.api_names:
        s = latency.stats(name)
        lines.append(
            f"║    {name:<18s} │ n={s['count']:<4d} avg={s['avg_ms']:>6.1f}ms"
            f"  p95={s['p95_ms']:>6.1f}ms  max={s['max_ms']:>6.1f}ms ║"
        )

    lines.append("╠" + "═" * w + "╣")

    # ---- 影子交易 ----
    lines.append(f"║{'🎭 影子交易战绩':^{w}}║")
    lines.append("║" + "─" * w + "║")

    ss = shadow.summary()
    lines.append(
        f"║  挂单池: {ss['pending_orders']:>3d}  │ 持仓中: {ss['open_positions']:>3d}  │"
        f" 已完结: {ss['closed_trades']:>4d}                 ║"
    )
    lines.append(
        f"║  胜: {ss['wins']:>4d}  负: {ss['losses']:>4d}  │"
        f" 胜率: {ss['win_rate']*100:>5.1f}%  │"
        f" 累计: {ss['total_pnl_pct']:>+7.2%}              ║"
    )
    lines.append(
        f"║  平均盈亏: {ss['avg_pnl_pct']:>+7.2%}  │"
        f" 最佳: {ss['best_pnl_pct']:>+7.2%}  │"
        f" 最差: {ss['worst_pnl_pct']:>+7.2%}         ║"
    )

    # 按层级
    if ss["by_zone"]:
        lines.append("║" + "─" * w + "║")
        lines.append(f"║  {'层级':<20s} │ {'成交':>4s} │ {'胜':>3s} │ {'负':>3s} │ {'胜率':>6s} │ {'累计PnL':>8s}    ║")
        for zone, zs in ss["by_zone"].items():
            wr = zs["wins"] / max(zs["fills"], 1) * 100
            lines.append(
                f"║  {zone:<20s} │ {zs['fills']:>4d} │ {zs['wins']:>3d} │ {zs['losses']:>3d} │"
                f" {wr:>5.1f}% │ {zs['total_pnl']:>+7.2%}    ║"
            )

    # 按平仓原因
    if ss["by_reason"]:
        lines.append("║" + "─" * w + "║")
        reasons_str = "  ".join(f"{k}: {v}" for k, v in ss["by_reason"].items())
        lines.append(f"║  平仓分布: {reasons_str:<{w-14}}║")

    lines.append("╚" + "═" * w + "╝")

    return "\n".join(lines)


# ============================================================
#  四、PerformanceMonitor 主类 (集成入口)
# ============================================================

class PerformanceMonitor:
    """
    模块五: 效能监控与参数调试

    集成 LatencyTracker + ShadowEngine，
    提供 run_forever 主循环定期输出报告。

    使用:
        pm = PerformanceMonitor(config)
        # 记录延迟
        async with pm.latency.measure("place_order"):
            ...
        # 影子挂单
        pm.shadow.place_order(ShadowOrder(...))
        pm.shadow.update_price("BTC-PERP", 67500)
        # 出报告
        print(pm.report())
    """

    def __init__(self, config: PerformanceConfig = None, exchange=None):
        self._config = config or PerformanceConfig()

        self.latency = LatencyTracker(
            warn_threshold_ms=self._config.latency_warn_ms,
            max_history=self._config.latency_history_size,
        )

        self.shadow = ShadowEngine(
            tp_pct=self._config.shadow_take_profit_pct,
            sl_pct=self._config.shadow_stop_loss_pct,
        )

        self._running = False

    def report(self, title: str = "深海锚 调参报告") -> str:
        """生成并返回调参报告文本"""
        return generate_report(self.latency, self.shadow, title)

    async def run_forever(self):
        """主循环: 定期输出报告"""
        if not self._config.enabled:
            logger.info("🔇 模块五 (PerformanceMonitor) 已禁用")
            while True:
                await asyncio.sleep(3600)

        self._running = True
        logger.info(
            f"📊 效能监控启动 | "
            f"延迟阈值={self._config.latency_warn_ms}ms | "
            f"影子模式={'✅' if self._config.shadow_mode else '❌'} | "
            f"报告周期={self._config.report_interval_sec}s"
        )

        while self._running:
            await asyncio.sleep(self._config.report_interval_sec)
            try:
                text = self.report(title="深海锚 定期调参报告")
                logger.info(f"\n{text}")
            except Exception as e:
                logger.warning(f"⚠️ 报告生成异常: {e}")

    def stop(self):
        self._running = False
