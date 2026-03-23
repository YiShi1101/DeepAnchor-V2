"""
deep_anchor/core/storage.py
============================
深海锚 - 模块四: 极简运维与状态持久化 (Lean Storage)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
核心职责 (面向 15GB 小盘 VPS):

  1. SQLite 异步写入
     - 一张 trade_history 表记录所有交易快照
     - 使用 aiosqlite 非阻塞写入 (回退: sqlite3 + executor)
     - 写入失败绝不崩溃主程序

  2. 日志轮转
     - TimedRotatingFileHandler 按天轮转
     - 最多保留 7 天，旧日志物理删除
     - 项目级 Logger 统一配置

  3. 数据降维
     - 定期清理超过 retention_days 的旧记录
     - 周期性 VACUUM 回收空间

接口极简:
    await storage.log_trade(...)     # 记录一笔交易
    await storage.query_recent(n)    # 查最近 n 条
    await storage.get_daily_pnl()    # 日汇总
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

# aiosqlite 可选依赖 (回退到 sqlite3 + executor)
try:
    import aiosqlite
    _HAS_AIOSQLITE = True
except ImportError:
    _HAS_AIOSQLITE = False

from ..config import StorageConfig


logger = logging.getLogger("DeepAnchor.Storage")


# ============================================================
#  一、交易记录数据结构
# ============================================================

@dataclass
class TradeRecord:
    """
    交易快照 —— 一笔完整的开平仓记录

    由模块三 (ExitManager) 的 ExitAction 转换而来。
    """
    order_id: str = ""              # 平仓订单 ID
    symbol: str = ""                # 交易对
    side: str = ""                  # 持仓方向: "LONG" / "SHORT"
    entry_price: float = 0.0        # 入场均价
    exit_price: float = 0.0         # 出场价 (触发价)
    size: float = 0.0               # 仓位大小
    close_reason: str = ""          # 平仓原因: TAKE_PROFIT / STOP_LOSS / TIMEOUT / CIRCUIT_BREAK
    pnl_pct: float = 0.0           # 盈亏百分比 (如 +0.05 = +5%)
    pnl_usd: float = 0.0           # 盈亏绝对值 (USD 估算)
    hold_time_sec: float = 0.0     # 持仓时长 (秒)
    zone_name: str = ""             # 来源层级 (浅水/甜点/深海)
    timestamp: str = ""             # ISO 格式时间戳


# ============================================================
#  二、日志轮转配置
# ============================================================

def setup_file_logger(config: StorageConfig) -> Optional[logging.Handler]:
    """
    配置项目级文件日志:
    - 按天轮转 (midnight)
    - 最多保留 config.log_max_days 天
    - 旧日志自动物理删除

    Returns:
        添加到 root logger 的 handler (供测试验证)
    """
    try:
        log_dir = Path(config.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / config.log_filename

        handler = TimedRotatingFileHandler(
            filename=str(log_path),
            when="midnight",            # 每天午夜轮转
            interval=1,
            backupCount=config.log_max_days,  # 保留天数
            encoding="utf-8",
            utc=True,                   # 使用 UTC 时间
        )
        handler.suffix = "%Y-%m-%d"     # 旧文件命名: deep_anchor.log.2026-03-09

        level = getattr(logging, config.log_level.upper(), logging.INFO)
        handler.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s │ %(levelname)-7s │ %(name)-28s │ %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # 添加到 root logger
        root = logging.getLogger()
        root.addHandler(handler)

        logger.info(
            f"📝 文件日志启用: {log_path} | "
            f"轮转=每天 | 保留={config.log_max_days}天"
        )
        return handler

    except Exception as e:
        logger.error(f"❌ 文件日志配置失败: {e}")
        return None


# ============================================================
#  三、LeanStorage 核心类
# ============================================================

# trade_history 表结构
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trade_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT    NOT NULL DEFAULT '',
    symbol          TEXT    NOT NULL,
    side            TEXT    NOT NULL,
    entry_price     REAL    NOT NULL,
    exit_price      REAL    NOT NULL,
    size            REAL    NOT NULL DEFAULT 0,
    close_reason    TEXT    NOT NULL,
    pnl_pct         REAL    NOT NULL,
    pnl_usd         REAL    NOT NULL DEFAULT 0,
    hold_time_sec   REAL    NOT NULL DEFAULT 0,
    zone_name       TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_trade_created ON trade_history(created_at);
CREATE INDEX IF NOT EXISTS idx_trade_symbol ON trade_history(symbol);
"""

_INSERT_SQL = """
INSERT INTO trade_history
    (order_id, symbol, side, entry_price, exit_price, size,
     close_reason, pnl_pct, pnl_usd, hold_time_sec, zone_name, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


class LeanStorage:
    """
    模块四: 极简运维与状态持久化

    使用方式:
        storage = LeanStorage(config)
        await storage.initialize()          # 建表
        await storage.log_trade(record)     # 写入
        rows = await storage.query_recent() # 查询
        await storage.run_forever()         # 定期清理 (并入 gather)
    """

    def __init__(self, config: StorageConfig = None, exchange=None):
        self._config = config or StorageConfig()
        self._db_path = self._config.db_path
        self._initialized = False
        self._write_count = 0
        self._error_count = 0
        self._log_handler: Optional[logging.Handler] = None

        # 持久连接 + 写锁 (解决 SQLite 并发 "database is locked")
        self._db_conn = None            # aiosqlite 持久连接 或 sqlite3.Connection
        self._write_lock = asyncio.Lock()

    async def initialize(self):
        """
        初始化: 创建目录 + 打开持久连接 + 建表 + 配置日志轮转

        关键设计: 使用单一持久连接，避免并发写入时
        多连接抢 SQLite 文件锁导致 "database is locked"。
        """
        if self._initialized:
            return

        # 创建数据库目录
        db_dir = Path(self._db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        # 打开持久连接
        try:
            if _HAS_AIOSQLITE:
                self._db_conn = await aiosqlite.connect(
                    self._db_path,
                    timeout=self._config.db_busy_timeout_ms / 1000,
                )
                # WAL 模式: 允许读写并发，大幅减少锁争用
                await self._db_conn.execute("PRAGMA journal_mode=WAL")
                await self._db_conn.execute("PRAGMA busy_timeout=5000")
                await self._db_conn.executescript(_CREATE_TABLE_SQL)
                await self._db_conn.executescript(_CREATE_INDEX_SQL)
                await self._db_conn.commit()
            else:
                self._db_conn = sqlite3.connect(
                    self._db_path,
                    timeout=self._config.db_busy_timeout_ms / 1000,
                )
                self._db_conn.execute("PRAGMA journal_mode=WAL")
                self._db_conn.executescript(_CREATE_TABLE_SQL)
                self._db_conn.executescript(_CREATE_INDEX_SQL)
                self._db_conn.commit()

            logger.info(f"💾 数据库就绪: {self._db_path} (持久连接 + WAL)")
        except Exception as e:
            logger.error(f"❌ 数据库初始化失败: {e}")

        # 配置文件日志
        self._log_handler = setup_file_logger(self._config)

        self._initialized = True

    # ────────────────────────────────────────────
    #  核心写入接口
    # ────────────────────────────────────────────

    async def log_trade(self, record: TradeRecord) -> bool:
        """
        异步记录一笔交易到 SQLite

        即使写入失败也只会 log warning，绝不会抛异常崩溃主程序。

        Returns:
            True = 写入成功, False = 失败
        """
        if not self._initialized:
            await self.initialize()

        ts = record.timestamp or datetime.now(timezone.utc).isoformat()
        params = (
            record.order_id,
            record.symbol,
            record.side,
            record.entry_price,
            record.exit_price,
            record.size,
            record.close_reason,
            record.pnl_pct,
            record.pnl_usd,
            record.hold_time_sec,
            record.zone_name,
            ts,
        )

        try:
            await self._execute_write(_INSERT_SQL, params)
            self._write_count += 1
            return True
        except Exception as e:
            self._error_count += 1
            logger.warning(f"⚠️ 交易记录写入失败 (#{self._error_count}): {e}")
            return False

    async def log_trade_from_action(self, action) -> bool:
        """
        从模块三的 ExitAction 直接转换并写入

        便捷封装，避免外部手动构造 TradeRecord。
        """
        record = TradeRecord(
            order_id=getattr(action, 'order_id', ''),
            symbol=action.symbol,
            side=getattr(action, 'close_side', ''),
            entry_price=getattr(action, 'entry_price', 0),
            exit_price=getattr(action, 'trigger_price', 0),
            size=getattr(action, 'close_size', 0),
            close_reason=getattr(action, 'close_reason', ''),
            pnl_pct=getattr(action, 'unrealized_pnl_pct', 0),
            pnl_usd=0,  # 可后续补算
            hold_time_sec=0,
            zone_name='',
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        # 如果 close_reason 是 Enum, 转 str
        if hasattr(record.close_reason, 'value'):
            record.close_reason = record.close_reason.value
        return await self.log_trade(record)

    # ────────────────────────────────────────────
    #  查询接口
    # ────────────────────────────────────────────

    async def query_recent(self, limit: int = 20) -> List[Dict[str, Any]]:
        """查询最近 N 条交易记录"""
        sql = "SELECT * FROM trade_history ORDER BY id DESC LIMIT ?"
        return await self._execute_read(sql, (limit,))

    async def get_trade_count(self) -> int:
        """总记录数"""
        rows = await self._execute_read("SELECT COUNT(*) as cnt FROM trade_history")
        return rows[0]["cnt"] if rows else 0

    async def get_daily_pnl(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        查询最近 N 天的每日盈亏汇总

        Returns:
            [{"date": "2026-03-09", "trades": 15, "total_pnl_pct": 0.023, ...}, ...]
        """
        sql = """
        SELECT
            date(created_at) as date,
            COUNT(*) as trades,
            SUM(pnl_pct) as total_pnl_pct,
            SUM(pnl_usd) as total_pnl_usd,
            AVG(pnl_pct) as avg_pnl_pct,
            SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN pnl_pct <= 0 THEN 1 ELSE 0 END) as losses
        FROM trade_history
        WHERE created_at >= datetime('now', ?)
        GROUP BY date(created_at)
        ORDER BY date DESC
        """
        return await self._execute_read(sql, (f"-{days} days",))

    # ────────────────────────────────────────────
    #  主循环: 定期清理
    # ────────────────────────────────────────────

    async def run_forever(self):
        """
        主循环: 定期清理过期数据 + VACUUM

        并入 asyncio.gather 运行。
        """
        if not self._config.enabled:
            logger.info("🔇 模块四 (LeanStorage) 已禁用")
            while True:
                await asyncio.sleep(3600)

        if not self._initialized:
            await self.initialize()

        logger.info(
            f"💾 存储模块启动 | "
            f"DB={self._db_path} | "
            f"保留={self._config.retention_days}天 | "
            f"清理周期={self._config.cleanup_interval_hours}h"
        )

        interval = self._config.cleanup_interval_hours * 3600
        while True:
            await asyncio.sleep(interval)
            try:
                await self._cleanup_old_records()
            except Exception as e:
                logger.warning(f"⚠️ 数据清理异常: {e}")

    async def _cleanup_old_records(self):
        """清理过期记录并 VACUUM"""
        threshold = f"-{self._config.retention_days} days"
        delete_sql = "DELETE FROM trade_history WHERE created_at < datetime('now', ?)"

        try:
            await self._execute_write(delete_sql, (threshold,))
            await self._execute_write("VACUUM")
            count = await self.get_trade_count()
            logger.info(f"🧹 数据清理完成 | 剩余 {count} 条记录")
        except Exception as e:
            logger.warning(f"⚠️ 清理失败: {e}")

    # ────────────────────────────────────────────
    #  底层: 异步 DB 操作
    # ────────────────────────────────────────────

    async def _execute_write(self, sql: str, params: tuple = ()):
        """
        异步执行写操作 (通过持久连接 + 写锁串行化)

        所有写操作共享同一个连接，通过 asyncio.Lock 避免并发冲突。
        """
        async with self._write_lock:
            if _HAS_AIOSQLITE and self._db_conn and hasattr(self._db_conn, 'execute'):
                try:
                    if ";" in sql and sql.strip().count(";") > 1:
                        await self._db_conn.executescript(sql)
                    else:
                        await self._db_conn.execute(sql, params)
                    await self._db_conn.commit()
                except Exception:
                    # 连接可能断了，尝试重建
                    await self._reconnect_db()
                    raise
            elif self._db_conn:
                # sqlite3 同步回退
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._sync_write, sql, params)
            else:
                raise RuntimeError("数据库未初始化")

    async def _execute_read(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        异步执行读操作

        读操作在 WAL 模式下不需要与写锁互斥，但为简单起见仍走同一连接。
        """
        async with self._write_lock:
            if _HAS_AIOSQLITE and self._db_conn and hasattr(self._db_conn, 'execute'):
                self._db_conn.row_factory = aiosqlite.Row
                cursor = await self._db_conn.execute(sql, params)
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]
            elif self._db_conn:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, self._sync_read, sql, params)
            else:
                return []

    async def _reconnect_db(self):
        """尝试重建持久连接"""
        try:
            if _HAS_AIOSQLITE and self._db_conn:
                await self._db_conn.close()
            self._db_conn = await aiosqlite.connect(
                self._db_path,
                timeout=self._config.db_busy_timeout_ms / 1000,
            )
            await self._db_conn.execute("PRAGMA journal_mode=WAL")
            logger.info("🔄 数据库连接已重建")
        except Exception as e:
            logger.error(f"❌ 数据库重连失败: {e}")

    # --- 同步回退 ---

    def _sync_write(self, sql: str, params: tuple = ()):
        """sqlite3 同步写入 (用于 run_in_executor)"""
        conn = sqlite3.connect(self._db_path, timeout=self._config.db_busy_timeout_ms / 1000)
        try:
            if ";" in sql and sql.strip().count(";") > 1:
                conn.executescript(sql)
            else:
                conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()

    def _sync_read(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """sqlite3 同步读取 (用于 run_in_executor)"""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(sql, params)
            return [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

    # ────────────────────────────────────────────
    #  统计
    # ────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        return {
            "writes": self._write_count,
            "errors": self._error_count,
            "db_path": self._db_path,
            "initialized": self._initialized,
            "backend": "aiosqlite" if _HAS_AIOSQLITE else "sqlite3+executor",
        }

    async def close(self):
        """清理资源"""
        # 关闭持久 DB 连接
        if self._db_conn:
            try:
                if _HAS_AIOSQLITE and hasattr(self._db_conn, 'close'):
                    await self._db_conn.close()
                elif hasattr(self._db_conn, 'close'):
                    self._db_conn.close()
            except Exception:
                pass
            self._db_conn = None

        # 移除日志 handler
        if self._log_handler:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler.close()
            self._log_handler = None

        self._initialized = False
        logger.info("💾 存储模块已关闭")
