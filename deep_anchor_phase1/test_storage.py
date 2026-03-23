#!/usr/bin/env python3
"""
test_storage.py
===============
深海锚 - 模块四 (极简运维) 独立测试脚本

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
测试覆盖:

  Test 1: 数据库初始化 (建目录 + 建表)
  Test 2: 单条写入 + 读回验证
  Test 3: 并发写入 100 条 (非阻塞性能)
  Test 4: 查询最近 N 条
  Test 5: 每日盈亏汇总
  Test 6: 写入失败不崩溃 (故意制造异常)
  Test 7: 日志文件轮转创建验证
  Test 8: 统计信息正确

运行:
  python3 test_storage.py

清理:
  rm -rf /tmp/da_test_storage/
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import os
import random
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from deep_anchor.config import StorageConfig
from deep_anchor.core.storage import LeanStorage, TradeRecord, setup_file_logger


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

logger = logging.getLogger("TestStorage")


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
#  测试用工作目录 (隔离)
# ============================================================

TEST_DIR = "/tmp/da_test_storage"


def clean_test_dir():
    """清理测试目录"""
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    os.makedirs(TEST_DIR, exist_ok=True)


def make_config(**overrides) -> StorageConfig:
    """创建使用测试目录的配置"""
    defaults = dict(
        enabled=True,
        db_path=os.path.join(TEST_DIR, "test.db"),
        log_dir=os.path.join(TEST_DIR, "logs"),
        log_filename="test.log",
        log_max_days=3,
        retention_days=30,
        cleanup_interval_hours=24,
    )
    defaults.update(overrides)
    return StorageConfig(**defaults)


def make_record(
    symbol="BTC-PERP",
    side="LONG",
    entry=67000.0,
    exit_p=67500.0,
    reason="TAKE_PROFIT",
    **kwargs,
) -> TradeRecord:
    """快速创建测试用 TradeRecord"""
    pnl_pct = (exit_p - entry) / entry if side == "LONG" else (entry - exit_p) / entry
    return TradeRecord(
        order_id=kwargs.get("order_id", f"ORD-{random.randint(1000, 9999)}"),
        symbol=symbol,
        side=side,
        entry_price=entry,
        exit_price=exit_p,
        size=kwargs.get("size", 0.1),
        close_reason=reason,
        pnl_pct=round(pnl_pct, 6),
        pnl_usd=round(pnl_pct * entry * 0.1, 2),
        hold_time_sec=kwargs.get("hold_time", random.uniform(5, 300)),
        zone_name=kwargs.get("zone", "浅水区"),
        timestamp=kwargs.get("timestamp", ""),
    )


# ============================================================
#  测试主体
# ============================================================

async def run_all_tests():
    global passed, failed
    passed = 0
    failed = 0

    print("\n" + "═" * 60)
    print("  🧪 Mock 测试: 极简运维 (Lean Storage) 逻辑验证")
    print("═" * 60 + "\n")

    clean_test_dir()

    # ──────────────────────────────────────────
    #  Test 1: 数据库初始化
    # ──────────────────────────────────────────
    print("─── Test 1: 数据库初始化 ───")

    config = make_config()
    storage = LeanStorage(config=config)

    await storage.initialize()
    check("初始化成功", storage._initialized)
    check("DB 文件已创建", os.path.exists(config.db_path))
    check("日志目录已创建", os.path.isdir(config.log_dir))

    log_path = os.path.join(config.log_dir, config.log_filename)
    check("日志文件已创建", os.path.exists(log_path), f"(expected: {log_path})")

    backend = storage.stats["backend"]
    print(f"    ℹ️ DB 后端: {backend}")
    print()

    # ──────────────────────────────────────────
    #  Test 2: 单条写入 + 读回
    # ──────────────────────────────────────────
    print("─── Test 2: 单条写入 + 读回 ───")

    record = make_record(
        order_id="TEST-001",
        symbol="BTC-PERP",
        side="LONG",
        entry=67000.0,
        exit_p=67500.0,
        reason="TAKE_PROFIT",
    )

    ok = await storage.log_trade(record)
    check("写入成功", ok)

    rows = await storage.query_recent(1)
    check("读回1条", len(rows) == 1)

    if rows:
        row = rows[0]
        check("order_id 正确", row["order_id"] == "TEST-001")
        check("symbol 正确", row["symbol"] == "BTC-PERP")
        check("side 正确", row["side"] == "LONG")
        check("entry_price 正确", row["entry_price"] == 67000.0)
        check("exit_price 正确", row["exit_price"] == 67500.0)
        check("close_reason 正确", row["close_reason"] == "TAKE_PROFIT")
        check("pnl_pct > 0", row["pnl_pct"] > 0)
        check("created_at 非空", len(str(row.get("created_at", ""))) > 0)
    print()

    # ──────────────────────────────────────────
    #  Test 3: 并发写入 100 条
    # ──────────────────────────────────────────
    print("─── Test 3: 并发写入 100 条 ───")

    symbols = ["BTC-PERP", "ETH-PERP", "SOL-PERP"]
    sides = ["LONG", "SHORT"]
    reasons = ["TAKE_PROFIT", "STOP_LOSS", "TIMEOUT"]

    tasks = []
    for i in range(100):
        sym = random.choice(symbols)
        side = random.choice(sides)
        entry = random.uniform(60000, 70000) if sym == "BTC-PERP" else random.uniform(100, 200)
        delta = entry * random.uniform(-0.03, 0.06)
        exit_p = entry + delta if side == "LONG" else entry - delta

        rec = make_record(
            order_id=f"BATCH-{i:03d}",
            symbol=sym,
            side=side,
            entry=round(entry, 2),
            exit_p=round(exit_p, 2),
            reason=random.choice(reasons),
        )
        tasks.append(storage.log_trade(rec))

    t0 = time.monotonic()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.monotonic() - t0

    success_count = sum(1 for r in results if r is True)
    fail_count = sum(1 for r in results if r is not True)

    check(f"100条全部成功 (成功={success_count})", success_count == 100, f"失败={fail_count}")
    check(f"耗时 {elapsed:.3f}s < 5s", elapsed < 5.0, f"实际={elapsed:.3f}s")

    total = await storage.get_trade_count()
    check(f"总记录数=101 (1+100)", total == 101, f"实际={total}")

    print(f"    ℹ️ 100 条并发写入耗时: {elapsed:.3f}s ({100/elapsed:.0f} TPS)")
    print()

    # ──────────────────────────────────────────
    #  Test 4: 查询最近 N 条
    # ──────────────────────────────────────────
    print("─── Test 4: 查询最近 N 条 ───")

    top5 = await storage.query_recent(5)
    check("返回5条", len(top5) == 5)

    if len(top5) >= 2:
        # 验证按 id DESC 排序 (最新在前)
        check("按时间倒序", top5[0]["id"] > top5[1]["id"])
    print()

    # ──────────────────────────────────────────
    #  Test 5: 每日盈亏汇总
    # ──────────────────────────────────────────
    print("─── Test 5: 每日盈亏汇总 ───")

    daily = await storage.get_daily_pnl(days=1)
    check("有今日汇总", len(daily) >= 1)

    if daily:
        today = daily[0]
        check(f"今日交易数={today['trades']}", today["trades"] == 101)
        check("有 wins 字段", "wins" in today)
        check("有 losses 字段", "losses" in today)
        check(f"wins + losses = {today.get('wins',0)} + {today.get('losses',0)} = {today['trades']}",
              (today.get("wins", 0) + today.get("losses", 0)) == today["trades"])

        print(f"    ℹ️ 今日: {today['trades']}笔 | "
              f"胜={today.get('wins',0)} 负={today.get('losses',0)} | "
              f"总PnL={today.get('total_pnl_pct',0):.4f}")
    print()

    # ──────────────────────────────────────────
    #  Test 6: 写入失败不崩溃
    # ──────────────────────────────────────────
    print("─── Test 6: 写入失败不崩溃 ───")

    # 创建一个指向不可能路径的 storage
    bad_config = make_config(db_path="/nonexistent/impossible/path/test.db")
    bad_storage = LeanStorage(config=bad_config)

    # 不调用 initialize，直接写——应该不崩溃
    try:
        ok = await bad_storage.log_trade(make_record())
        # 可能成功 (如果 initialize 内部吞了目录创建错误)
        # 也可能失败 (返回 False)
        check("未崩溃 (返回布尔值)", isinstance(ok, bool))
    except Exception as e:
        # 如果真的抛异常了，那是 bug
        check("不应崩溃", False, f"异常: {e}")

    # 即使上面有异常，后续正常 storage 仍可用
    ok2 = await storage.log_trade(make_record(order_id="AFTER-FAIL"))
    check("故障后正常 storage 仍可写", ok2)
    print()

    # ──────────────────────────────────────────
    #  Test 7: 日志文件创建验证
    # ──────────────────────────────────────────
    print("─── Test 7: 日志文件验证 ───")

    log_path = os.path.join(config.log_dir, config.log_filename)
    check("日志文件存在", os.path.exists(log_path))

    # 文件应该有内容 (初始化日志 + 写入日志)
    if os.path.exists(log_path):
        size = os.path.getsize(log_path)
        check(f"日志非空 ({size} bytes)", size > 0)

        # 读取内容验证
        with open(log_path, "r") as f:
            content = f.read()
        check("包含 'Storage' 关键词", "Storage" in content or "storage" in content.lower())
    print()

    # ──────────────────────────────────────────
    #  Test 8: 统计信息
    # ──────────────────────────────────────────
    print("─── Test 8: 统计信息 ───")

    stats = storage.stats
    check(f"writes={stats['writes']} (≥101)", stats["writes"] >= 101)
    check("errors=少量", stats["errors"] <= 1)  # 可能 Test 6 产生了 0 或 1 个
    check("initialized=True", stats["initialized"])
    check(f"backend={stats['backend']}", stats["backend"] in ("aiosqlite", "sqlite3+executor"))
    print()

    # ──────────────────────────────────────────
    #  清理
    # ──────────────────────────────────────────
    await storage.close()

    # 打印 DB 文件大小
    if os.path.exists(config.db_path):
        db_size = os.path.getsize(config.db_path) / 1024
        print(f"    ℹ️ DB 文件大小: {db_size:.1f} KB (102 条记录)")

    # ──────────────────────────────────────────
    #  汇总
    # ──────────────────────────────────────────
    total = passed + failed
    print("\n" + "═" * 60)
    print(f"  📊 测试结果: {passed}/{total} 通过", end="")
    if failed > 0:
        print(f"  ⚠️ {failed} 项失败!")
    else:
        print(f"  🎉 全部通过!")
    print("═" * 60)
    print(f"\n    📁 测试数据保留在: {TEST_DIR}")
    print(f"    🧹 清理命令: rm -rf {TEST_DIR}\n")

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
