"""
deep_anchor/main.py
===================
深海锚 (Deep Anchor) - 系统主控制器

本文件定义 DeepAnchorSystem 类，它是整个系统的总调度器。
使用 asyncio.gather 将五个解耦的协程模块并发运行。

模块协同架构:
    ┌─────────────────────────────────────────────┐
    │           DeepAnchorSystem (主控)             │
    │                                             │
    │  asyncio.gather(                            │
    │    module_1: MacroEngine.run_forever()       │  ← 宏观测算 & 阶梯预埋
    │    module_2: MicroRadar.run_forever()        │  ← 微观雷达 & 熔断器
    │    module_3: ExitManager.run_forever()       │  ← 平仓管家
    │    module_4: LeanStorage.run_forever()       │  ← 极简运维
    │    module_5: PerformanceMonitor.run_forever()│  ← 效能监控
    │  )                                          │
    └─────────────────────────────────────────────┘

启动流程:
    1. 加载配置
    2. 初始化数据源 (Binance K线)
    3. 初始化交易所连接 (01 DEX) [dry_run 时可跳过]
    4. 实例化五个模块
    5. asyncio.gather 并发启动
"""

import asyncio
import logging
import signal
import sys
from typing import Optional

from .config import DeepAnchorConfig
from .core.interfaces import ExchangeInterface, DataFeedInterface
from .core.data_feed import BinanceFuturesDataFeed
from .core.macro_engine import MacroEngine
from .core.micro_radar import MicroRadar
from .core.exit_manager import ExitManager
from .core.storage import LeanStorage
from .core.performance import PerformanceMonitor


logger = logging.getLogger("DeepAnchor.System")


class DeepAnchorSystem:
    """
    深海锚系统 - 主控制器

    职责:
    - 管理系统生命周期 (启动、运行、优雅关闭)
    - 实例化和注入所有子模块的依赖
    - 并发调度五个协程模块
    """

    def __init__(self, config: DeepAnchorConfig):
        """
        Args:
            config: 系统总配置 (包含所有子模块配置)
        """
        self._config = config
        self._running = False

        # --- 基础设施层 ---
        self._data_feed: Optional[DataFeedInterface] = None
        self._exchange: Optional[ExchangeInterface] = None

        # --- 五大模块 ---
        self._macro_engine: Optional[MacroEngine] = None
        self._micro_radar: Optional[MicroRadar] = None
        self._exit_manager: Optional[ExitManager] = None
        self._storage: Optional[LeanStorage] = None
        self._performance: Optional[PerformanceMonitor] = None

    async def initialize(self) -> bool:
        """
        系统初始化 (启动前的准备工作)

        Returns:
            True = 初始化成功，可以启动
            False = 初始化失败，不要启动
        """
        logger.info("=" * 60)
        logger.info("🐋 深海锚 (Deep Anchor) 系统初始化中...")
        logger.info("=" * 60)

        # --- 1. 初始化数据源 (Binance K线) ---
        logger.info("📡 初始化数据源: 币安公共合约 API")
        self._data_feed = BinanceFuturesDataFeed(self._config.data_feed)

        # --- 2. 初始化交易所 (01 DEX) ---
        if not self._config.dry_run:
            logger.info("🔗 初始化交易所: 01 DEX (实盘模式)")
            try:
                from .exchange.adapter_01dex import ZeroOneExchangeAdapter
                self._exchange = ZeroOneExchangeAdapter(self._config.exchange)
                connected = await self._exchange.connect()
                if not connected:
                    logger.error("❌ 01 DEX 连接失败!")
                    return False
            except Exception as e:
                logger.error(f"❌ 交易所初始化失败: {e}")
                return False
        else:
            logger.info("🏖️ dry_run 模式: 跳过交易所连接")

        # --- 3. 实例化五大模块 ---
        logger.info("⚙️ 实例化策略模块...")

        # 模块一: 宏观引擎 (本阶段完整实现)
        self._macro_engine = MacroEngine(
            config=self._config.macro,
            data_feed=self._data_feed,
            exchange=self._exchange,
            symbol=self._config.symbol,
            dry_run=self._config.dry_run,
        )

        # 模块二: 微观雷达 (占位)
        self._micro_radar = MicroRadar(
            config=self._config.micro,
            exchange=self._exchange,
        )

        # 模块三: 平仓管家 (占位)
        self._exit_manager = ExitManager(
            config=self._config.exit_mgr,
            exchange=self._exchange,
        )

        # 模块四: 极简运维 (占位)
        self._storage = LeanStorage(config=self._config.storage)

        # 模块五: 效能监控 (占位)
        self._performance = PerformanceMonitor(config=self._config.performance)

        logger.info("✅ 系统初始化完成!")
        return True

    async def run(self):
        """
        系统主运行入口

        使用 asyncio.gather 并发启动五个模块。
        任何一个模块崩溃不会影响其他模块运行。
        """
        if not await self.initialize():
            logger.error("初始化失败，系统终止")
            return

        self._running = True

        logger.info("\n" + "🐋" * 30)
        logger.info("  深海锚 (Deep Anchor) 系统已启动!")
        logger.info(f"  交易对: {self._config.symbol}")
        logger.info(f"  模式:   {'🏖️ 模拟 (DRY RUN)' if self._config.dry_run else '🔴 实盘 (LIVE)'}")
        logger.info("🐋" * 30 + "\n")

        try:
            # 五个模块并发运行，return_exceptions=True 避免一个崩溃拖垮全部
            results = await asyncio.gather(
                self._macro_engine.run_forever(),       # 模块一: 宏观引擎
                self._micro_radar.run_forever(),        # 模块二: 微观雷达
                self._exit_manager.run_forever(),       # 模块三: 平仓管家
                self._storage.run_forever(),            # 模块四: 极简运维
                self._performance.run_forever(),        # 模块五: 效能监控
                return_exceptions=True,
            )

            # 检查是否有模块抛出异常
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"模块 {i+1} 异常退出: {result}")

        except asyncio.CancelledError:
            logger.info("⏹️ 系统收到取消信号")
        except Exception as e:
            logger.error(f"💥 系统致命错误: {e}", exc_info=True)
        finally:
            await self.shutdown()

    async def run_single_tick(self):
        """
        单次执行模式 (用于测试)
        
        只初始化 + 执行一次模块一的 tick，然后退出。
        """
        if not await self.initialize():
            logger.error("初始化失败")
            return None

        result = await self._macro_engine.single_shot()

        # 清理
        if self._data_feed and hasattr(self._data_feed, 'close'):
            await self._data_feed.close()

        return result

    async def shutdown(self):
        """优雅关闭"""
        logger.info("🛑 深海锚系统正在关闭...")
        self._running = False

        # 关闭数据源连接
        if self._data_feed and hasattr(self._data_feed, 'close'):
            await self._data_feed.close()

        # 断开交易所
        if self._exchange:
            await self._exchange.disconnect()

        logger.info("👋 深海锚系统已安全关闭")
