#!/usr/bin/env python3
"""
test_exit_manager.py
====================
深海锚 - 模块三 (平仓管家) 独立测试脚本

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
测试覆盖:

  Test 1: 挂单 TTL 超时撤单
  Test 2: 挂单成交通知 (不触发撤单)
  Test 3: 多头止盈触发
  Test 4: 多头止损触发
  Test 5: 空头止盈触发
  Test 6: 空头止损触发
  Test 7: 持仓超时平仓
  Test 8: 平仓使用 IOC + 滑点保护
  Test 9: 熔断强制平仓 (force_close_all)
  Test 10: 无价格数据时不误判

运行:
  python3 test_exit_manager.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from deep_anchor.config import ExitManagerConfig
from deep_anchor.core.exit_manager import (
    ExitManager, ExitAction, ActionType, CloseReason,
    TrackedOrder, TrackedPosition,
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

logger = logging.getLogger("TestExitMgr")


# ============================================================
#  测试框架
# ============================================================

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
    print("  🧪 Mock 测试: 平仓管家 (Exit Manager) 逻辑验证")
    print("═" * 60 + "\n")

    # ──────────────────────────────────────────
    #  Test 1: 挂单 TTL 超时撤单
    # ──────────────────────────────────────────
    print("─── Test 1: 挂单 TTL 超时撤单 ───")

    actions_log: list = []
    config = ExitManagerConfig(
        enabled=True,
        order_ttl_sec=2.0,          # 2 秒超时 (加速测试)
        take_profit_pct=0.05,
        stop_loss_pct=0.02,
        max_hold_time_sec=300,
    )
    mgr = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    # 注册一个挂单
    mgr.register_order(
        order_id="ORD-001",
        symbol="BTC-PERP",
        side="BUY",
        price=65000.0,
        size=0.1,
        zone_name="浅水区",
    )
    check("注册后: 挂单池=1", mgr.tracked_order_count == 1)

    # 立即 tick — 不应触发
    result = mgr.tick()
    check("0s: 无撤单动作", len(result) == 0)

    # 等待超时
    print("    (等待 2.5s TTL 过期...)")
    await asyncio.sleep(2.5)

    result = mgr.tick()
    check("2.5s: 有撤单动作", len(result) == 1)
    if result:
        check("动作类型=CANCEL_ORDER", result[0].action_type == ActionType.CANCEL_ORDER)
        check("订单ID=ORD-001", result[0].order_id == "ORD-001")
    check("撤单后: 挂单池=0", mgr.tracked_order_count == 0)
    check("回调被调用", len(actions_log) == 1)
    print()

    # ──────────────────────────────────────────
    #  Test 2: 挂单成交通知 (不触发撤单)
    # ──────────────────────────────────────────
    print("─── Test 2: 挂单成交通知 ───")

    actions_log.clear()
    mgr2 = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    mgr2.register_order("ORD-002", "BTC-PERP", "BUY", 64000.0, 0.2)
    check("注册后: 池=1", mgr2.tracked_order_count == 1)

    # 通知成交 → 应从池中移除，不触发撤单
    mgr2.notify_fill("ORD-002", fill_price=64000.0, fill_size=0.2)
    check("成交后: 池=0", mgr2.tracked_order_count == 0)

    # 等超时后 tick — 不应有任何撤单
    await asyncio.sleep(2.5)
    result = mgr2.tick()
    check("超时tick: 无动作 (已移除)", len(result) == 0)
    check("无回调", len(actions_log) == 0)
    print()

    # ──────────────────────────────────────────
    #  Test 3: 多头止盈触发
    # ──────────────────────────────────────────
    print("─── Test 3: 多头止盈 ───")

    actions_log.clear()
    mgr3 = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    mgr3.register_position("BTC-PERP", "LONG", 0.1, entry_price=67000.0)
    pos = mgr3.get_position("BTC-PERP")
    check(f"止盈价={pos.take_profit_price}", pos.take_profit_price == 70350.0)
    # 67000 * 1.05 = 70350.0

    # 价格未到止盈 → 无动作
    mgr3.update_price("BTC-PERP", 69000.0)
    result = mgr3.tick()
    check("69000: 无平仓", len(result) == 0)

    # 价格触及止盈
    mgr3.update_price("BTC-PERP", 70500.0)
    result = mgr3.tick()
    check("70500: 有平仓", len(result) == 1)
    if result:
        a = result[0]
        check("类型=CLOSE_POSITION", a.action_type == ActionType.CLOSE_POSITION)
        check("原因=TAKE_PROFIT", a.close_reason == CloseReason.TAKE_PROFIT)
        check("方向=SELL (平多)", a.close_side == "SELL")
        check("大小=0.1", a.close_size == 0.1)
        check(f"盈亏=+{a.unrealized_pnl_pct:.2%}", a.unrealized_pnl_pct > 0.04)
    check("平仓后: 持仓台=0", mgr3.tracked_position_count == 0)
    print()

    # ──────────────────────────────────────────
    #  Test 4: 多头止损触发
    # ──────────────────────────────────────────
    print("─── Test 4: 多头止损 ───")

    actions_log.clear()
    mgr4 = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    mgr4.register_position("BTC-PERP", "LONG", 0.2, entry_price=67000.0)
    pos = mgr4.get_position("BTC-PERP")
    check(f"止损价={pos.stop_loss_price}", pos.stop_loss_price == 65660.0)
    # 67000 * (1 - 0.02) = 65660.0

    # 价格击穿止损
    mgr4.update_price("BTC-PERP", 65500.0)
    result = mgr4.tick()
    check("65500: 有平仓", len(result) == 1)
    if result:
        a = result[0]
        check("原因=STOP_LOSS", a.close_reason == CloseReason.STOP_LOSS)
        check("方向=SELL (平多)", a.close_side == "SELL")
        check(f"盈亏={a.unrealized_pnl_pct:.2%} (亏损)", a.unrealized_pnl_pct < 0)
    print()

    # ──────────────────────────────────────────
    #  Test 5: 空头止盈触发
    # ──────────────────────────────────────────
    print("─── Test 5: 空头止盈 ───")

    actions_log.clear()
    mgr5 = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    mgr5.register_position("ETH-PERP", "SHORT", 1.0, entry_price=3500.0)
    pos = mgr5.get_position("ETH-PERP")
    check(f"止盈价={pos.take_profit_price}", pos.take_profit_price == 3325.0)
    # 3500 * (1 - 0.05) = 3325.0

    # 价格跌过止盈
    mgr5.update_price("ETH-PERP", 3300.0)
    result = mgr5.tick()
    check("3300: 有平仓", len(result) == 1)
    if result:
        a = result[0]
        check("原因=TAKE_PROFIT", a.close_reason == CloseReason.TAKE_PROFIT)
        check("方向=BUY (平空)", a.close_side == "BUY")
        check(f"盈亏=+{a.unrealized_pnl_pct:.2%}", a.unrealized_pnl_pct > 0.04)
    print()

    # ──────────────────────────────────────────
    #  Test 6: 空头止损触发
    # ──────────────────────────────────────────
    print("─── Test 6: 空头止损 ───")

    actions_log.clear()
    mgr6 = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    mgr6.register_position("ETH-PERP", "SHORT", 1.0, entry_price=3500.0)
    pos = mgr6.get_position("ETH-PERP")
    check(f"止损价={pos.stop_loss_price}", pos.stop_loss_price == 3570.0)
    # 3500 * (1 + 0.02) = 3570.0

    # 价格涨破止损
    mgr6.update_price("ETH-PERP", 3580.0)
    result = mgr6.tick()
    check("3580: 有平仓", len(result) == 1)
    if result:
        a = result[0]
        check("原因=STOP_LOSS", a.close_reason == CloseReason.STOP_LOSS)
        check("方向=BUY (平空)", a.close_side == "BUY")
        check(f"盈亏={a.unrealized_pnl_pct:.2%} (亏损)", a.unrealized_pnl_pct < 0)
    print()

    # ──────────────────────────────────────────
    #  Test 7: 持仓超时平仓
    # ──────────────────────────────────────────
    print("─── Test 7: 持仓超时 ───")

    config_fast = ExitManagerConfig(
        enabled=True,
        order_ttl_sec=60,
        take_profit_pct=0.05,
        stop_loss_pct=0.02,
        max_hold_time_sec=2.0,      # 2 秒超时 (加速测试)
    )
    actions_log.clear()
    mgr7 = ExitManager(config=config_fast, on_action=lambda a: actions_log.append(a))

    mgr7.register_position("SOL-PERP", "LONG", 10.0, entry_price=150.0)
    mgr7.update_price("SOL-PERP", 151.0)  # 价格正常，不触发 TP/SL

    result = mgr7.tick()
    check("0s: 无平仓 (未超时)", len(result) == 0)

    print("    (等待 2.5s 超时...)")
    await asyncio.sleep(2.5)

    result = mgr7.tick()
    check("2.5s: 有平仓", len(result) == 1)
    if result:
        check("原因=TIMEOUT", result[0].close_reason == CloseReason.TIMEOUT)
    print()

    # ──────────────────────────────────────────
    #  Test 8: IOC + 滑点保护
    # ──────────────────────────────────────────
    print("─── Test 8: IOC + 滑点保护 ───")

    config_slip = ExitManagerConfig(
        enabled=True,
        take_profit_pct=0.05,
        stop_loss_pct=0.02,
        slippage_tolerance_pct=0.005,   # 0.5%
        close_order_type="IOC",
    )
    actions_log.clear()
    mgr8 = ExitManager(config=config_slip, on_action=lambda a: actions_log.append(a))

    # 多头止盈
    mgr8.register_position("BTC-PERP", "LONG", 0.1, entry_price=67000.0)
    mgr8.update_price("BTC-PERP", 71000.0)
    result = mgr8.tick()

    if result:
        a = result[0]
        check("订单类型=IOC", a.order_type == "IOC")
        # 平多 → 卖出: 限价 = 71000 * (1 - 0.005) = 70645
        expected = round(71000.0 * (1 - 0.005), 2)
        check(f"限价={a.close_price} (期望≈{expected})", abs(a.close_price - expected) < 1)
        check("限价 < 触发价 (卖出滑点保护)", a.close_price < a.trigger_price)
    else:
        check("应有平仓动作", False)

    # 空头止损
    mgr8.register_position("ETH-PERP", "SHORT", 1.0, entry_price=3500.0)
    mgr8.update_price("ETH-PERP", 3580.0)
    result = mgr8.tick()

    if result:
        a = result[0]
        # 平空 → 买入: 限价 = 3580 * (1 + 0.005) = 3597.9
        expected = round(3580.0 * (1 + 0.005), 2)
        check(f"空头限价={a.close_price} (期望≈{expected})", abs(a.close_price - expected) < 1)
        check("限价 > 触发价 (买入滑点保护)", a.close_price > a.trigger_price)
    else:
        check("应有空头平仓动作", False)
    print()

    # ──────────────────────────────────────────
    #  Test 9: 熔断强制平仓
    # ──────────────────────────────────────────
    print("─── Test 9: 熔断强制平仓 (force_close_all) ───")

    actions_log.clear()
    mgr9 = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    mgr9.register_position("BTC-PERP", "LONG", 0.5, entry_price=67000.0)
    mgr9.register_position("ETH-PERP", "SHORT", 2.0, entry_price=3500.0)
    mgr9.update_price("BTC-PERP", 67100.0)
    mgr9.update_price("ETH-PERP", 3490.0)
    check("两个持仓", mgr9.tracked_position_count == 2)

    mgr9.force_close_all(CloseReason.CIRCUIT_BREAK)
    check("全部平仓: 回调=2", len(actions_log) == 2)
    check("持仓台清空", mgr9.tracked_position_count == 0)
    if len(actions_log) >= 2:
        reasons = {a.close_reason for a in actions_log}
        check("原因=CIRCUIT_BREAK", CloseReason.CIRCUIT_BREAK in reasons)
    print()

    # ──────────────────────────────────────────
    #  Test 10: 无价格数据时不误判
    # ──────────────────────────────────────────
    print("─── Test 10: 无价格数据时安全跳过 ───")

    actions_log.clear()
    mgr10 = ExitManager(config=config, on_action=lambda a: actions_log.append(a))

    mgr10.register_position("DOGE-PERP", "LONG", 100.0, entry_price=0.3)
    # 不喂价格数据
    result = mgr10.tick()
    check("无价格: 无动作", len(result) == 0)
    check("持仓仍在", mgr10.tracked_position_count == 1)
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


# ============================================================
#  主入口
# ============================================================

if __name__ == "__main__":
    setup_logging()
    try:
        ok = asyncio.run(run_all_tests())
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\n⏹️ 用户终止")
