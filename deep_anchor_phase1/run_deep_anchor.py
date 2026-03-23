#!/usr/bin/env python3
"""
run_deep_anchor.py
==================
深海锚 (Deep Anchor) - 统一运行入口

运行模式:
    python3 run_deep_anchor.py test       # 单次测试 (拉K线 → 计算 → 打印)
    python3 run_deep_anchor.py dry        # 模拟循环 (全模块运行, 不下单)
    python3 run_deep_anchor.py live       # 🔴 实盘 (连接 01 DEX)
    python3 run_deep_anchor.py validate   # 独立指标验证

默认: test
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from deep_anchor.config import (
    DeepAnchorConfig, MacroEngineConfig, DataFeedConfig,
    ExchangeConnectionConfig, MicroRadarConfig,
    ExitManagerConfig, StorageConfig, PerformanceConfig,
    OrderZone,
)
from deep_anchor.main import DeepAnchorSystem
from deep_anchor.core.data_feed import BinanceFuturesDataFeed
from deep_anchor.core.indicators import calculate_indicators


# ============================================================
#  日志配置
# ============================================================

def setup_logging(level=logging.INFO):
    formatter = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-7s │ %(name)-28s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)


# ============================================================
#  通用配置构建
# ============================================================

def build_config(
    dry_run: bool = True,
    symbol: str = "BTC-PERP",
    macro_refresh: int = 300,
    micro_enabled: bool = True,
) -> DeepAnchorConfig:
    """构建标准化配置，所有模式共用"""
    return DeepAnchorConfig(
        symbol=symbol,
        dry_run=dry_run,
        exchange=ExchangeConnectionConfig(
            base_url="https://zo-mainnet.n1.xyz",
            account_id=int(os.getenv("ZERO1_ACCOUNT_ID", "0")),
        ),
        data_feed=DataFeedConfig(),
        macro=MacroEngineConfig(
            ema_period=20,
            atr_period=14,
            kline_timeframe="1h",
            kline_limit=100,
            total_capital_per_side=10_000.0,
            refresh_interval_sec=macro_refresh,
            post_only=True,
            zones=[
                OrderZone(name="🌊 浅水区 (Shallow)", atr_multiplier=2.5, capital_ratio=0.20),
                OrderZone(name="🎯 甜点区 (Sweet Spot)", atr_multiplier=4.0, capital_ratio=0.30),
                OrderZone(name="🐋 深海区 (Deep Sea)", atr_multiplier=7.0, capital_ratio=0.50),
            ],
        ),
        micro=MicroRadarConfig(
            enabled=micro_enabled,
            window_seconds=10,
            liquidation_threshold_usd=5_000_000.0,
            tps_spike_ratio=5.0,
        ),
        exit_mgr=ExitManagerConfig(
            enabled=True,
            order_ttl_sec=330.0,
            take_profit_pct=0.015,
            stop_loss_pct=0.02,
            max_hold_time_sec=300.0,
        ),
        storage=StorageConfig(
            enabled=True,
            db_path="data/deep_anchor.db",
            log_dir="logs",
        ),
        performance=PerformanceConfig(
            enabled=True,
            latency_warn_ms=500.0,
            shadow_mode=True,
            shadow_take_profit_pct=0.015,        # 影子止盈 3% (实盘 5%)
            shadow_stop_loss_pct=0.02,           # 影子止损 1% (实盘 2%)
            shadow_capital_per_side=10_000.0,    # 影子虚拟资金
            shadow_zones=[                       # 影子独立网格 (更激进)
                OrderZone(name="🔵 影-浅水 (S-Shallow)", atr_multiplier=1.5, capital_ratio=0.20),
                OrderZone(name="🟣 影-甜点 (S-Sweet)",   atr_multiplier=3.0, capital_ratio=0.30),
                OrderZone(name="⚫ 影-深海 (S-Deep)",     atr_multiplier=5.0, capital_ratio=0.50),
            ],
            report_interval_sec=3600,
        ),
    )


# ============================================================
#  模式一: 单次测试 (test)
# ============================================================

async def run_test_mode():
    """拉取K线 → 计算指标 → 打印挂单方案，不下单"""
    print("\n" + "═" * 64)
    print("  🐋 深海锚 (Deep Anchor) - 单次测试模式")
    print("═" * 64)
    print(f"  时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  模式: 🏖️ 测试 (仅计算，不下单)")
    print("═" * 64 + "\n")

    config = build_config(dry_run=True, micro_enabled=False)
    system = DeepAnchorSystem(config)
    orders = await system.run_single_tick()

    if orders:
        print("\n" + "─" * 64)
        print("  📋 挂单方案汇总")
        print("─" * 64)
        print(f"  {'方向':<6} {'层级':<26} {'价格':>12} {'数量(BTC)':>12} {'金额(USDC)':>12}")
        print("  " + "─" * 68)
        for o in orders:
            print(
                f"  {o.side.value:<6} {o.zone_name:<26} "
                f"${o.price:>10,.1f} {o.size:>12.6f} ${o.notional_usdc:>10,.0f}"
            )
        print("─" * 64)
        print(f"  📊 共 {len(orders)} 个挂单 | post_only=✅ | dry_run=✅")
        print("─" * 64 + "\n")


# ============================================================
#  模式二: 模拟循环 (dry)
# ============================================================

async def run_dry_mode():
    """全五个模块并发运行，但不连接交易所、不下单"""
    print("\n🏖️ 深海锚 - 模拟循环模式 (Ctrl+C 退出)\n")

    config = build_config(
        dry_run=True,
        macro_refresh=60,       # 测试时 1 分钟刷新
        micro_enabled=True,     # 启用微观雷达
    )

    system = DeepAnchorSystem(config)
    await system.run()


# ============================================================
#  模式三: 实盘运行 (live)
# ============================================================

async def run_live_mode():
    """连接 01 DEX，实际下单"""
    if not os.getenv("ZERO1_PRIVATE_KEY"):
        print("❌ 未设置 ZERO1_PRIVATE_KEY!")
        print("   export ZERO1_PRIVATE_KEY='your_base58_private_key'")
        return

    print("\n🔴 深海锚 - 实盘模式 (Ctrl+C 退出)")
    print("⚠️  警告: 将发送真实订单到 01 Exchange!\n")

    for i in range(3, 0, -1):
        print(f"  {i}...")
        await asyncio.sleep(1)
    print("  🚀 启动!\n")

    config = build_config(
        dry_run=False,
        macro_refresh=300,      # 5 分钟刷新
        micro_enabled=True,
    )

    system = DeepAnchorSystem(config)
    await system.run()


# ============================================================
#  模式四: 独立指标验证 (validate)
# ============================================================

async def run_validate():
    """直接拉取 K 线并验证 EMA/ATR 计算"""
    print("\n🔬 深海锚 - 指标计算验证\n")

    data_feed = BinanceFuturesDataFeed(DataFeedConfig())
    try:
        candles = await data_feed.fetch_ohlcv("BTC-PERP", "1h", 100)
        print(f"✅ 获取 {len(candles)} 根 K 线")

        indicators = calculate_indicators(candles)
        print(f"\n📈 EMA(20): ${indicators['ema']:>12,.2f}")
        print(f"📈 ATR(14): ${indicators['atr']:>12,.2f}")
        print(f"📈 最新价:  ${indicators['last_close']:>12,.2f}")
    except Exception as e:
        print(f"❌ {e}")
    finally:
        await data_feed.close()
    print()


# ============================================================
#  主入口
# ============================================================

if __name__ == "__main__":
    setup_logging(logging.INFO)

    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    modes = {
        "test": run_test_mode,
        "dry": run_dry_mode,
        "live": run_live_mode,
        "validate": run_validate,
    }

    if mode not in modes:
        print(f"未知模式: '{mode}'")
        print(f"可用: {', '.join(modes.keys())}")
        sys.exit(1)

    try:
        asyncio.run(modes[mode]())
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户手动终止")
    except Exception as e:
        print(f"\n💥 致命错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
