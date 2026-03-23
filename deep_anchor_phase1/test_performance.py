#!/usr/bin/env python3
"""
test_performance.py
===================
深海锚 - 模块五 (效能监控) 独立测试脚本

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
测试覆盖:

  Test 1: 延迟记录 + 百分位统计
  Test 2: 慢调用 WARNING 触发
  Test 3: 上下文管理器自动计时
  Test 4: 按 API 名称分组统计
  Test 5: 影子挂单 + 价格击穿成交
  Test 6: 影子多头止盈
  Test 7: 影子空头止损
  Test 8: 按层级汇总
  Test 9: 完整调参报告输出
  Test 10: PerformanceMonitor 集成

运行:
  python3 test_performance.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import sys
import os
import time
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from deep_anchor.config import PerformanceConfig
from deep_anchor.core.performance import (
    LatencyTracker, ShadowEngine, ShadowOrder, ShadowFill,
    PerformanceMonitor, generate_report,
)

# ============================================================
#  日志
# ============================================================

def setup_logging():
    fmt = logging.Formatter(
        "%(asctime)s │ %(levelname)-7s │ %(name)-28s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)

logger = logging.getLogger("TestPerf")

passed = 0
failed = 0

def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        print(f"  ❌ {name} {detail}")


# ============================================================
#  测试主体
# ============================================================

async def run_all_tests():
    global passed, failed
    passed = 0
    failed = 0

    print("\n" + "═" * 60)
    print("  🧪 Mock 测试: 效能监控 (Performance Monitor)")
    print("═" * 60 + "\n")

    # ──────────────────────────────────────────
    #  Test 1: 延迟记录 + 百分位统计
    # ──────────────────────────────────────────
    print("─── Test 1: 延迟记录 + 百分位统计 ───")

    tracker = LatencyTracker(warn_threshold_ms=500.0, max_history=1000)

    # 注入 100 个有梯度的延迟值
    for i in range(100):
        tracker.record("test_api", float(i + 1))  # 1ms ~ 100ms

    s = tracker.stats()
    check(f"count=100", s["count"] == 100)
    check(f"avg≈50ms", 49 < s["avg_ms"] < 52, f"got {s['avg_ms']}")
    check(f"p50≈50", 49 <= s["p50_ms"] <= 52, f"got {s['p50_ms']}")
    check(f"p95≈95", 94 <= s["p95_ms"] <= 97, f"got {s['p95_ms']}")
    check(f"max=100", s["max_ms"] == 100.0)
    check("slow_count=0 (都 <500ms)", s["slow_count"] == 0)
    print()

    # ──────────────────────────────────────────
    #  Test 2: 慢调用 WARNING
    # ──────────────────────────────────────────
    print("─── Test 2: 慢调用 WARNING ───")

    tracker2 = LatencyTracker(warn_threshold_ms=100.0)
    tracker2.record("fast_api", 50.0)    # 不触发
    tracker2.record("slow_api", 150.0)   # 触发
    tracker2.record("slow_api", 200.0)   # 触发

    s2 = tracker2.stats()
    check("slow_count=2", s2["slow_count"] == 2)
    check("总调用=3", s2["count"] == 3)
    print()

    # ──────────────────────────────────────────
    #  Test 3: 上下文管理器自动计时
    # ──────────────────────────────────────────
    print("─── Test 3: 上下文管理器自动计时 ───")

    tracker3 = LatencyTracker(warn_threshold_ms=500.0)

    async with tracker3.measure("sleep_test"):
        await asyncio.sleep(0.05)  # 50ms

    s3 = tracker3.stats("sleep_test")
    check(f"自动记录1次", s3["count"] == 1)
    check(f"耗时≈50ms (实际 {s3['avg_ms']:.0f}ms)", 30 < s3["avg_ms"] < 150)
    print()

    # ──────────────────────────────────────────
    #  Test 4: 按 API 分组
    # ──────────────────────────────────────────
    print("─── Test 4: 按 API 名称分组 ───")

    tracker4 = LatencyTracker()
    for _ in range(10):
        tracker4.record("get_ticker", random.uniform(5, 20))
    for _ in range(5):
        tracker4.record("place_order", random.uniform(50, 200))

    check("api_names 包含 2 项", len(tracker4.api_names) == 2)
    gt = tracker4.stats("get_ticker")
    po = tracker4.stats("place_order")
    check(f"get_ticker: n=10", gt["count"] == 10)
    check(f"place_order: n=5", po["count"] == 5)
    check("place_order 比 get_ticker 慢", po["avg_ms"] > gt["avg_ms"])
    print()

    # ──────────────────────────────────────────
    #  Test 5: 影子挂单 + 价格击穿成交
    # ──────────────────────────────────────────
    print("─── Test 5: 影子挂单 + 击穿成交 ───")

    engine = ShadowEngine(tp_pct=0.05, sl_pct=0.02)

    # 挂一个买单 @ 65000
    engine.place_order(ShadowOrder(
        order_id="S-001", symbol="BTC-PERP", side="BUY",
        price=65000.0, size=0.1, zone_name="浅水区",
    ))

    # 价格 67000 — 不击穿
    engine.update_price("BTC-PERP", 67000.0)
    ss = engine.summary()
    check("67000: 未成交", ss["pending_orders"] == 1 and ss["open_positions"] == 0)

    # 价格跌到 64800 — 击穿 65000 → 成交
    engine.update_price("BTC-PERP", 64800.0)
    ss = engine.summary()
    check("64800: 已成交", ss["pending_orders"] == 0 and ss["open_positions"] == 1)

    # 同理挂卖单测试
    engine.place_order(ShadowOrder(
        order_id="S-002", symbol="BTC-PERP", side="SELL",
        price=70000.0, size=0.1, zone_name="甜点区",
    ))
    engine.update_price("BTC-PERP", 70500.0)
    ss = engine.summary()
    # 70500 同时触发: S-002 卖单成交 + S-001 多头止盈 (64800→70500 = +8.8%)
    # 所以: pending=0 (S-002被消耗), open=1 (S-002), closed=1 (S-001止盈)
    check("卖单击穿成交 (pending=0)", ss["pending_orders"] == 0)
    check("S-001 同步止盈 (closed≥1)", ss["closed_trades"] >= 1)
    print()

    # ──────────────────────────────────────────
    #  Test 6: 影子多头止盈
    # ──────────────────────────────────────────
    print("─── Test 6: 影子多头止盈 ───")

    # S-001 买入 @ 64800, 止盈 5% = 68040
    engine.update_price("BTC-PERP", 68100.0)  # > 68040
    ss = engine.summary()
    # S-001 应该止盈平仓了; S-002 卖单按 70500 成交, 价格跌到 68100 → 空头浮盈
    tp_trades = [t for t in engine._closed if t.close_reason == "TAKE_PROFIT"]
    check(f"S-001 止盈: closed={len(tp_trades)}", len(tp_trades) >= 1)
    if tp_trades:
        check(f"盈亏>0", tp_trades[0].pnl_pct > 0)
    print()

    # ──────────────────────────────────────────
    #  Test 7: 影子空头止损
    # ──────────────────────────────────────────
    print("─── Test 7: 影子空头止损 ───")

    # S-002 卖出 @ 70500, 止损 2% = 71910
    engine.update_price("BTC-PERP", 72000.0)  # > 71910
    ss = engine.summary()
    sl_trades = [t for t in engine._closed if t.close_reason == "STOP_LOSS"]
    check(f"S-002 止损: 有 STOP_LOSS 记录", len(sl_trades) >= 1)
    if sl_trades:
        check("止损亏损<0", sl_trades[-1].pnl_pct < 0)
    check(f"全部已平: open={ss['open_positions']}", ss["open_positions"] == 0)
    print()

    # ──────────────────────────────────────────
    #  Test 8: 按层级汇总
    # ──────────────────────────────────────────
    print("─── Test 8: 按层级汇总 ───")

    ss = engine.summary()
    check("有 by_zone 数据", len(ss["by_zone"]) > 0)
    check("有 by_reason 数据", len(ss["by_reason"]) > 0)

    if "浅水区" in ss["by_zone"]:
        check("浅水区 fills≥1", ss["by_zone"]["浅水区"]["fills"] >= 1)
    if "甜点区" in ss["by_zone"]:
        check("甜点区 fills≥1", ss["by_zone"]["甜点区"]["fills"] >= 1)
    print()

    # ──────────────────────────────────────────
    #  Test 9: 完整调参报告
    # ──────────────────────────────────────────
    print("─── Test 9: 完整调参报告 ───")

    # 用 tracker4 (有分组数据) + engine (有影子交易) 生成报告
    report_text = generate_report(tracker4, engine, title="🧪 测试调参报告")
    check("报告非空", len(report_text) > 100)
    check("包含'延迟'", "延迟" in report_text)
    check("包含'影子'", "影子" in report_text)
    check("包含'浅水区'", "浅水区" in report_text)

    # 打印完整报告
    print()
    print(report_text)
    print()

    # ──────────────────────────────────────────
    #  Test 10: PerformanceMonitor 集成
    # ──────────────────────────────────────────
    print("─── Test 10: PerformanceMonitor 集成 ───")

    config = PerformanceConfig(
        enabled=True,
        latency_warn_ms=100.0,
        shadow_take_profit_pct=0.03,
        shadow_stop_loss_pct=0.01,
    )
    pm = PerformanceMonitor(config=config)

    # 延迟
    pm.latency.record("connect", 45.0)
    pm.latency.record("connect", 250.0)  # > 100ms → 慢

    # 影子
    pm.shadow.place_order(ShadowOrder(
        order_id="PM-001", symbol="ETH-PERP", side="BUY",
        price=3400.0, size=1.0, zone_name="深海区",
    ))
    pm.shadow.update_price("ETH-PERP", 3350.0)  # 击穿 → 成交
    pm.shadow.update_price("ETH-PERP", 3460.0)  # +3.3% > 3% → 止盈

    full_report = pm.report(title="🔧 集成测试报告")
    check("集成报告非空", len(full_report) > 50)

    pm_stats = pm.shadow.summary()
    check("集成: 已有 closed", pm_stats["closed_trades"] >= 1)

    lat_stats = pm.latency.stats()
    check("集成: 延迟记录=2", lat_stats["count"] == 2)
    check("集成: slow=1", lat_stats["slow_count"] == 1)
    print()

    # ──────────────────────────────────────────
    #  汇总
    # ──────────────────────────────────────────
    total = passed + failed
    print("═" * 60)
    print(f"  📊 测试结果: {passed}/{total} 通过", end="")
    if failed > 0:
        print(f"  ⚠️ {failed} 项失败!")
    else:
        print(f"  🎉 全部通过!")
    print("═" * 60 + "\n")

    return failed == 0


if __name__ == "__main__":
    setup_logging()
    try:
        ok = asyncio.run(run_all_tests())
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\n⏹️ 用户终止")
