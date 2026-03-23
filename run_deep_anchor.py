#!/usr/bin/env python3
"""
run_deep_anchor.py
==================
深海锚 (Deep Anchor) - 独立运行入口

本脚本提供三种运行模式:
    1. test   - 单次测试: 拉取K线 → 计算指标 → 打印挂单方案 (默认)
    2. dry    - 模拟循环: 持续运行但不下单
    3. live   - 实盘运行: 连接 01 DEX 并实际下单 (需私钥)

使用方式:
    python run_deep_anchor.py              # 默认: 单次测试
    python run_deep_anchor.py test         # 等同上面
    python run_deep_anchor.py dry          # 模拟循环
    python run_deep_anchor.py live         # 实盘 (危险!)

环境要求:
    pip install aiohttp          # 异步 HTTP (拉取币安K线)
    # 以下仅实盘需要:
    pip install protobuf cryptography base58 requests pyyaml
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone

# 确保项目根目录在 Python 路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from deep_anchor.config import (
    DeepAnchorConfig, MacroEngineConfig, DataFeedConfig,
    ExchangeConnectionConfig, OrderZone
)
from deep_anchor.main import DeepAnchorSystem
from deep_anchor.core.data_feed import BinanceFuturesDataFeed
from deep_anchor.core.indicators import calculate_ema, calculate_atr, calculate_indicators
from deep_anchor.core.macro_engine import MacroEngine


# ============================================================
#  日志配置 (彩色格式)
# ============================================================
def setup_logging(level=logging.INFO):
    """配置日志格式"""
    formatter = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-7s │ %(name)-28s │ %(message)s",
        datefmt="%H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)


# ============================================================
#  模式一: 单次测试 (test)
# ============================================================
async def run_test_mode():
    """
    单次测试模式:
    1. 从币安拉取 BTC 1H K线 (100 根)
    2. 计算 EMA(20) 和 ATR(14)
    3. 在控制台漂亮打印三道防线的挂单方案

    不连接 01 DEX，不发送任何订单。
    """
    print("\n" + "═" * 64)
    print("  🐋 深海锚 (Deep Anchor) - Phase 1 单次测试模式")
    print("═" * 64)
    print(f"  时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  标的: BTC-PERP (数据源: 币安合约)")
    print(f"  模式: 🏖️ 测试 (仅计算，不下单)")
    print("═" * 64 + "\n")

    # --- 构建配置 ---
    config = DeepAnchorConfig(
        symbol="BTC-PERP",
        dry_run=True,               # 关键: 不下单
        macro=MacroEngineConfig(
            ema_period=20,
            atr_period=14,
            kline_timeframe="1h",
            kline_limit=100,
            total_capital_per_side=5_000.0,
            refresh_interval_sec=300,
            post_only=True,
            zones=[
                OrderZone(name="🌊 浅水区 (Shallow)", atr_multiplier=2.5, capital_ratio=0.20),
                OrderZone(name="🎯 甜点区 (Sweet Spot)", atr_multiplier=4.0, capital_ratio=0.30),
                OrderZone(name="🐋 深海区 (Deep Sea)", atr_multiplier=7.0, capital_ratio=0.50),
            ],
        ),
        data_feed=DataFeedConfig(),
    )

    # --- 方法 A: 使用 DeepAnchorSystem 的 run_single_tick ---
    system = DeepAnchorSystem(config)
    orders = await system.run_single_tick()

    if orders:
        # 额外输出: 以简洁表格形式汇总
        print("\n" + "─" * 64)
        print("  📋 挂单方案汇总表 (CSV 友好格式)")
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
    else:
        print("❌ 未生成任何挂单方案 (可能是网络问题)")

    return orders


# ============================================================
#  模式二: 模拟循环 (dry)
# ============================================================
async def run_dry_mode():
    """
    模拟循环模式: 
    持续运行模块一，每隔 5 分钟刷新，但不实际下单。
    Ctrl+C 退出。
    """
    print("\n🏖️ 深海锚 - 模拟循环模式启动 (Ctrl+C 退出)\n")

    config = DeepAnchorConfig(
        symbol="BTC-PERP",
        dry_run=True,
        macro=MacroEngineConfig(
            refresh_interval_sec=60,    # 测试时 1 分钟刷新一次
        ),
    )

    system = DeepAnchorSystem(config)
    await system.run()


# ============================================================
#  模式三: 实盘运行 (live)
# ============================================================
async def run_live_mode():
    """
    实盘模式: 连接 01 DEX 并实际下单。
    
    ⚠️ 危险操作! 请确保:
    1. ZERO1_PRIVATE_KEY 环境变量已设置
    2. 已充分测试
    """
    # 安全检查
    if not os.getenv("ZERO1_PRIVATE_KEY"):
        print("❌ 错误: 未设置 ZERO1_PRIVATE_KEY 环境变量!")
        print("   请执行: export ZERO1_PRIVATE_KEY='your_base58_private_key'")
        return

    print("\n🔴 深海锚 - 实盘模式启动 (Ctrl+C 退出)")
    print("⚠️  警告: 将会发送真实订单到 01 Exchange!\n")

    # 3 秒确认倒计时
    for i in range(3, 0, -1):
        print(f"  {i}...")
        await asyncio.sleep(1)
    print("  🚀 启动!\n")

    config = DeepAnchorConfig(
        symbol="BTC-PERP",
        dry_run=False,  # 实盘!
        exchange=ExchangeConnectionConfig(
            base_url="https://zo-mainnet.n1.xyz",
            account_id=int(os.getenv("ZERO1_ACCOUNT_ID", "0")),
        ),
    )

    system = DeepAnchorSystem(config)
    await system.run()


# ============================================================
#  独立数据验证 (直接测试指标计算)
# ============================================================
async def run_indicator_validation():
    """
    独立验证: 
    单独测试数据拉取和指标计算，不涉及任何交易逻辑。
    用于确认币安 API 连通性和数学计算正确性。
    """
    print("\n🔬 深海锚 - 指标计算独立验证\n")

    data_feed = BinanceFuturesDataFeed(DataFeedConfig())

    try:
        # 拉取 K 线
        candles = await data_feed.fetch_ohlcv("BTC-PERP", "1h", 100)
        print(f"✅ 成功拉取 {len(candles)} 根 1H K 线\n")

        # 打印最近 5 根 K 线
        print("📊 最近 5 根 K 线:")
        print(f"  {'时间戳':<15} {'开盘':>10} {'最高':>10} {'最低':>10} {'收盘':>10} {'成交量':>12}")
        print("  " + "─" * 70)
        for c in candles[-5:]:
            ts = datetime.fromtimestamp(c.timestamp / 1000, tz=timezone.utc)
            print(
                f"  {ts.strftime('%m-%d %H:%M'):<15} "
                f"{c.open:>10,.1f} {c.high:>10,.1f} {c.low:>10,.1f} {c.close:>10,.1f} "
                f"{c.volume:>12,.2f}"
            )

        # 计算指标
        indicators = calculate_indicators(candles, ema_period=20, atr_period=14)

        print(f"\n📈 指标计算结果:")
        print(f"  ├── EMA(20):     ${indicators['ema']:>12,.2f}")
        print(f"  ├── ATR(14):     ${indicators['atr']:>12,.2f}")
        print(f"  ├── 最新收盘价:  ${indicators['last_close']:>12,.2f}")
        print(f"  └── 使用K线数:   {indicators['candle_count']} 根")

        # 验证: EMA 应该接近最新价
        ema = indicators['ema']
        close = indicators['last_close']
        deviation_pct = abs(close - ema) / ema * 100
        print(f"\n🔍 健全性检查:")
        print(f"  ├── EMA vs 收盘价偏离: {deviation_pct:.4f}% {'✅' if deviation_pct < 10 else '⚠️'}")
        print(f"  ├── ATR > 0: {'✅' if indicators['atr'] > 0 else '❌'}")
        print(f"  └── 数据量充足: {'✅' if len(candles) >= 50 else '⚠️'}")

    except Exception as e:
        print(f"❌ 验证失败: {e}")
    finally:
        await data_feed.close()

    print("\n✅ 独立验证完成\n")


# ============================================================
#  主入口
# ============================================================
if __name__ == "__main__":
    setup_logging(logging.INFO)

    # 解析命令行参数
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    mode_map = {
        "test": run_test_mode,
        "dry": run_dry_mode,
        "live": run_live_mode,
        "validate": run_indicator_validation,
    }

    if mode not in mode_map:
        print(f"未知模式: '{mode}'")
        print(f"可用模式: {', '.join(mode_map.keys())}")
        sys.exit(1)

    try:
        asyncio.run(mode_map[mode]())
    except KeyboardInterrupt:
        print("\n\n⏹️ 用户手动终止 (Ctrl+C)")
    except Exception as e:
        print(f"\n💥 致命错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
