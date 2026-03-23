#!/usr/bin/env python3
"""
test_radar.py
=============
深海锚 - 模块二 (微观雷达) 独立测试脚本

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
三种测试模式:

  python3 test_radar.py live
    → 连接真实币安 WebSocket，实时打印爆仓/成交聚合状态
    → Ctrl+C 退出

  python3 test_radar.py mock
    → 不连接网络，手动注入极端数据验证熔断器触发逻辑
    → 全自动运行，无需网络

  python3 test_radar.py both
    → 先跑 Mock 验证，通过后自动切换到 Live 监听
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

依赖:
  pip install websockets    (仅 live 模式需要)
"""

import asyncio
import logging
import sys
import os
import time

# 确保项目根目录在路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from deep_anchor.config import MicroRadarConfig
from deep_anchor.core.micro_radar import (
    MicroRadar, RadarSnapshot, RollingWindow, BreakerState
)

# ============================================================
#  日志配置
# ============================================================

def setup_logging():
    formatter = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-7s │ %(name)-28s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)

logger = logging.getLogger("TestRadar")


# ============================================================
#  测试一: Mock 模式 (离线, 验证熔断逻辑)
# ============================================================

async def run_mock_tests():
    """
    离线 Mock 测试:
    不连接任何网络，手动注入数据验证:
      1. RollingWindow 基础功能
      2. 爆仓阈值熔断
      3. TPS 飙升熔断
      4. 冷却期机制
      5. 手动重置
    """
    print("\n" + "═" * 60)
    print("  🧪 Mock 测试: 微观雷达熔断逻辑验证")
    print("═" * 60 + "\n")

    passed = 0
    failed = 0

    def check(name: str, condition: bool, detail: str = ""):
        nonlocal passed, failed
        if condition:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name} {detail}")

    # ────── Test 1: RollingWindow 基础 ──────
    print("─── Test 1: RollingWindow 基础功能 ───")
    win = RollingWindow(max_age_sec=2.0)
    win.add(100.0)
    win.add(200.0)
    check("添加后 sum=300", win.sum() == 300.0, f"got {win.sum()}")
    check("添加后 count=2", win.count() == 2, f"got {win.count()}")

    await asyncio.sleep(2.1)  # 等窗口过期
    check("过期后 sum=0", win.sum() == 0.0, f"got {win.sum()}")
    check("过期后 count=0", win.count() == 0, f"got {win.count()}")
    print()

    # ────── Test 2: 爆仓阈值熔断 ──────
    print("─── Test 2: 爆仓阈值熔断 ───")

    breaker_fired = []

    def on_break(snapshot: RadarSnapshot):
        breaker_fired.append(snapshot)

    config = MicroRadarConfig(
        enabled=True,
        window_seconds=5,
        liquidation_threshold_usd=1_000_000.0,   # 100 万 USD
        tps_spike_ratio=10.0,                      # 故意设高，不让 TPS 触发
        cooldown_seconds=3,
    )

    radar = MicroRadar(config=config, on_circuit_break=on_break)

    # 注入 50 万 — 不该触发
    radar.inject_liquidation(500_000.0, side="SELL")
    check("50万: 未触发", not radar.circuit_break_event.is_set())
    check("50万: 状态=NORMAL", radar.snapshot.breaker_state == "NORMAL")

    # 再注入 60 万 — 累计 110 万，应触发
    radar.inject_liquidation(600_000.0, side="SELL")
    check("110万: 已触发", radar.circuit_break_event.is_set())
    check("110万: 回调被调用", len(breaker_fired) == 1, f"got {len(breaker_fired)}")
    check("110万: 原因含'爆仓'", "爆仓" in breaker_fired[-1].trigger_reason if breaker_fired else False)
    check("110万: 状态=COOLDOWN", radar.snapshot.breaker_state in ("TRIPPED", "COOLDOWN"))

    # 冷却期内再注入 — 不该重复触发
    radar.inject_liquidation(2_000_000.0, side="SELL")
    check("冷却期内: 回调仍只1次", len(breaker_fired) == 1, f"got {len(breaker_fired)}")
    print()

    # ────── Test 3: 冷却到期自动恢复 ──────
    print("─── Test 3: 冷却到期自动恢复 ───")
    # 需等待: 冷却期(3s) + 窗口过期(5s) 取较大值，确保旧数据全部清除
    wait_time = max(config.cooldown_seconds, config.window_seconds) + 1.0
    print(f"    (等待 {wait_time:.0f}s 让冷却+窗口全部过期...)")
    await asyncio.sleep(wait_time)

    # 现在: 冷却已到期 + 旧爆仓数据已滑出窗口
    # 注入一笔小额，仅用于触发一次 _evaluate
    radar.inject_liquidation(100.0, side="BUY")
    check("冷却后: 状态回 NORMAL", radar.snapshot.breaker_state == "NORMAL")
    check("冷却后: event 已清除", not radar.circuit_break_event.is_set())
    print()

    # ────── Test 4: TPS 飙升熔断 ──────
    print("─── Test 4: TPS 飙升熔断 ───")

    breaker_fired_tps = []

    config2 = MicroRadarConfig(
        enabled=True,
        window_seconds=3,                         # 短窗口 3 秒
        liquidation_threshold_usd=999_999_999.0,  # 故意设超高，不让爆仓触发
        tps_spike_ratio=3.0,                       # TPS 倍率阈值
        tps_baseline_window_sec=60,                # 基线窗口 60 秒
        cooldown_seconds=2,
    )
    radar2 = MicroRadar(config=config2, on_circuit_break=lambda s: breaker_fired_tps.append(s))

    # ⚠️ 模拟已运行 120 秒 (跳过预热期，预热期 = 60/2 = 30s)
    radar2._start_time = time.time() - 120

    # 建立基线: 30 笔分散在过去 55 秒 (基线不能太稀疏，需 >= 2.0 TPS)
    base_time = time.time()
    for i in range(30):
        ts = base_time - 55.0 + i * 1.8
        radar2._trade_window_baseline.add(1.0, ts)

    # 在短窗口内集中注入 100 笔 (短窗口 TPS ≈ 33.3)
    # 基线 TPS ≈ (30+100)/60 ≈ 2.17 → 满足 >= 2.0 门槛
    # 比率 ≈ 33.3 / 2.17 ≈ 15.3x >> 阈值 3x → 必触发
    now = time.time()
    for _ in range(100):
        radar2._trade_window_short.add(1.0, now)
        radar2._trade_window_baseline.add(1.0, now)

    radar2._evaluate()

    check("TPS飙升: 已触发", radar2.circuit_break_event.is_set())
    check("TPS飙升: 原因含'TPS'",
          "TPS" in breaker_fired_tps[-1].trigger_reason if breaker_fired_tps else False)
    check(f"TPS飙升: 比率={radar2.snapshot.tps_ratio:.1f}x ≥ 3x",
          radar2.snapshot.tps_ratio >= 3.0,
          f"got {radar2.snapshot.tps_ratio:.2f}")
    print()

    # ────── Test 5: 手动重置 ──────
    print("─── Test 5: 手动重置 ───")
    radar2.reset_breaker()
    check("重置后: 状态=NORMAL", radar2.snapshot.breaker_state == "NORMAL" or
          radar2._breaker_state == BreakerState.NORMAL)
    check("重置后: event 已清除", not radar2.circuit_break_event.is_set())
    print()

    # ────── 汇总 ──────
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
#  测试二: Live 模式 (连接真实币安 WebSocket)
# ============================================================

async def run_live_monitor():
    """
    连接真实币安 WebSocket，实时打印:
    - 每收到爆仓消息时打印详情
    - 每 2 秒打印一次聚合状态仪表盘
    - 如果触发熔断，高亮打印警报
    """
    print("\n" + "═" * 60)
    print("  📡 Live 监听: 连接币安永续合约 WebSocket")
    print("     (Ctrl+C 退出)")
    print("═" * 60 + "\n")

    try:
        import websockets
    except ImportError:
        print("❌ 需要安装 websockets: pip install websockets")
        return

    config = MicroRadarConfig(
        enabled=True,
        window_seconds=10,
        liquidation_threshold_usd=5_000_000.0,
        tps_spike_ratio=5.0,
        cooldown_seconds=30,
    )

    trigger_log = []

    def on_break(snapshot: RadarSnapshot):
        trigger_log.append(snapshot)
        print(f"\n{'🚨' * 20}")
        print(f"  熔断触发! 原因: {snapshot.trigger_reason}")
        print(f"{'🚨' * 20}\n")

    radar = MicroRadar(config=config, on_circuit_break=on_break)

    # 仪表盘定时打印任务
    async def dashboard_printer():
        """每 2 秒打印一次雷达状态"""
        await asyncio.sleep(3)  # 等连接建立
        while True:
            s = radar.snapshot
            if s.timestamp == 0:
                await asyncio.sleep(2)
                continue

            # 状态颜色
            state_icon = {
                "NORMAL": "🟢",
                "TRIPPED": "🔴",
                "COOLDOWN": "🟡",
            }.get(s.breaker_state, "⚪")

            # 预热期标识
            warmup_elapsed = time.time() - radar._start_time if radar._start_time > 0 else 0
            warmup_needed = config.tps_baseline_window_sec / 2
            warmup_tag = "" if warmup_elapsed >= warmup_needed else f" ⏳预热 {warmup_needed - warmup_elapsed:.0f}s"

            line = (
                f"\r{state_icon} {s.breaker_state:8s}{warmup_tag} │ "
                f"💥 爆仓: ${s.liq_total_usd:>12,.0f} "
                f"({s.liq_count:>3d}笔, 卖${s.liq_sell_usd:>10,.0f} 买${s.liq_buy_usd:>10,.0f}) │ "
                f"📊 TPS: {s.tps_short:>5.1f} / base {s.tps_baseline:>5.1f} = {s.tps_ratio:>4.1f}x │ "
                f"🚨 累计触发: {radar._trip_count}"
            )
            print(line, end="", flush=True)
            await asyncio.sleep(2)

    # 并发: 雷达主循环 + 仪表盘打印
    try:
        await asyncio.gather(
            radar.run_forever(),
            dashboard_printer(),
        )
    except asyncio.CancelledError:
        pass

    radar.stop()
    print(f"\n\n📊 会话统计: 共触发 {radar._trip_count} 次熔断")


# ============================================================
#  主入口
# ============================================================

async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "both"

    if mode == "mock":
        await run_mock_tests()

    elif mode == "live":
        await run_live_monitor()

    elif mode == "both":
        print("━" * 60)
        print("  阶段一: Mock 逻辑验证")
        print("━" * 60)
        ok = await run_mock_tests()
        if not ok:
            print("❌ Mock 测试未通过，跳过 Live 监听")
            return

        print("━" * 60)
        print("  阶段二: Live 实时监听 (10 秒预览后自动退出)")
        print("━" * 60)
        # both 模式下只跑 10 秒 Live 预览
        try:
            await asyncio.wait_for(run_live_monitor(), timeout=15)
        except asyncio.TimeoutError:
            print("\n\n⏱️ Live 预览结束 (10s)")
        except asyncio.CancelledError:
            pass

    else:
        print(f"未知模式: {mode}")
        print("用法: python3 test_radar.py [mock|live|both]")


if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户手动终止")
