"""
deep_anchor/main.py
===================
深海锚 (Deep Anchor) - 主控引擎 (Main Engine)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
角色: 组装车间 + 总机房

不包含任何具体业务逻辑。只负责:
  1. 初始化全局配置和基础设施
  2. 实例化五个子模块并注入依赖
  3. 编排七个并发协程 (5 模块主循环 + 2 联动协程)
  4. 优雅启停 (SIGINT / SIGTERM)

并发任务编排 (asyncio.gather):
  ┌────────────────────────────────────────────────────┐
  │  1. _task_macro_loop()      宏观引擎 (可暂停)      │
  │  2. _micro_radar.run_forever()  微观雷达 WebSocket  │
  │  3. _exit_manager.run_forever() 平仓管家 定时扫描   │
  │  4. _storage.run_forever()      存储 定期清理       │
  │  5. _performance.run_forever()  效能 定期报告       │
  │  ─── 联动协程 ───                                  │
  │  6. _task_price_feeder()    价格喂养 → Exit + Shadow │
  │  7. _task_breaker_watcher() 熔断监听 → 撤单 + 暂停  │
  │  8. _task_action_executor() 动作消费 → 执行 + 持久化 │
  └────────────────────────────────────────────────────┘

数据流:
  币安 K线 ──→ MacroEngine ──→ 01 DEX (挂单)
                                  │
  币安 WS ───→ MicroRadar        │ (挂单 ID 注册到 ExitManager)
                  │               │
              熔断信号 ──→ ExitManager ← 最新价 (price_feeder)
                               │
                          action_queue
                               │
                     action_executor ──→ 01 DEX (撤单/平仓)
                               │
                          LeanStorage (SQLite)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import signal
import sys
import time
from typing import List, Optional

from .config import DeepAnchorConfig
from .core.interfaces import (
    ExchangeInterface, DataFeedInterface,
    OrderRequest, OrderSideEnum, OrderTypeEnum,
)
from .core.data_feed import BinanceFuturesDataFeed
from .core.macro_engine import MacroEngine
from .core.micro_radar import MicroRadar
from .core.exit_manager import (
    ExitManager, ExitAction, ActionType, CloseReason,
)
from .core.storage import LeanStorage, TradeRecord
from .core.performance import (
    PerformanceMonitor, ShadowOrder,
)


logger = logging.getLogger("DeepAnchor.System")


class DeepAnchorSystem:
    """
    深海锚 - 主控引擎

    职责:
    - 系统生命周期管理 (初始化 → 运行 → 关闭)
    - 模块间数据流联动 (不含业务逻辑)
    - 信号处理 (SIGINT / SIGTERM → 优雅退出)
    """

    def __init__(self, config: DeepAnchorConfig):
        self._config = config
        self._running = False

        # --- 基础设施 ---
        self._data_feed: Optional[DataFeedInterface] = None
        self._exchange: Optional[ExchangeInterface] = None

        # --- 五大模块 ---
        self._macro: Optional[MacroEngine] = None
        self._radar: Optional[MicroRadar] = None
        self._exit_mgr: Optional[ExitManager] = None
        self._storage: Optional[LeanStorage] = None
        self._perf: Optional[PerformanceMonitor] = None

        # --- 联动状态 ---
        self._macro_paused = False          # 宏观引擎暂停标志 (熔断时置 True)
        self._shutdown_event = asyncio.Event()

    # ════════════════════════════════════════════
    #  初始化
    # ════════════════════════════════════════════

    async def initialize(self) -> bool:
        """
        系统初始化:
        1. 数据源 (Binance K线)
        2. 交易所 (01 DEX, dry_run 时跳过)
        3. 五个子模块实例化
        4. 存储初始化 (建表 + 日志轮转)
        """
        logger.info("═" * 60)
        logger.info("🐋 深海锚 (Deep Anchor) 系统初始化中...")
        logger.info("═" * 60)

        # --- 1. 数据源 ---
        logger.info("📡 初始化数据源: 币安公共合约 API")
        self._data_feed = BinanceFuturesDataFeed(self._config.data_feed)

        # --- 2. 交易所 ---
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
                logger.error(f"❌ 交易所初始化失败: {e}", exc_info=True)
                return False
        else:
            logger.info("🏖️ dry_run 模式: 跳过交易所连接")

        # --- 3. 实例化五大模块 ---
        logger.info("⚙️ 实例化策略模块...")

        self._macro = MacroEngine(
            config=self._config.macro,
            data_feed=self._data_feed,
            exchange=self._exchange,
            symbol=self._config.symbol,
            dry_run=self._config.dry_run,
        )

        self._radar = MicroRadar(
            config=self._config.micro,
            on_circuit_break=self._on_circuit_break_callback,
        )

        self._exit_mgr = ExitManager(
            config=self._config.exit_mgr,
            on_action=self._on_exit_action_callback,
        )

        self._storage = LeanStorage(config=self._config.storage)

        self._perf = PerformanceMonitor(config=self._config.performance)

        # --- 4. 存储初始化 ---
        try:
            await self._storage.initialize()
        except Exception as e:
            logger.warning(f"⚠️ 存储初始化失败 (非致命): {e}")

        logger.info("✅ 系统初始化完成!")
        return True

    # ════════════════════════════════════════════
    #  主运行入口
    # ════════════════════════════════════════════

    async def run(self):
        """
        系统主入口: 初始化 → 注册信号 → 并发启动 → 等待退出
        """
        if not await self.initialize():
            logger.error("初始化失败，系统终止")
            return

        self._running = True

        # 注册信号处理 (SIGINT=Ctrl+C, SIGTERM=kill)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._signal_handler)
            except NotImplementedError:
                pass  # Windows 不支持 add_signal_handler

        logger.info("\n" + "🐋" * 30)
        logger.info("  深海锚 (Deep Anchor) 系统已启动!")
        logger.info(f"  交易对: {self._config.symbol}")
        logger.info(f"  模式:   {'🏖️ 模拟 (DRY RUN)' if self._config.dry_run else '🔴 实盘 (LIVE)'}")
        logger.info("🐋" * 30 + "\n")

        try:
            # 八个并发任务
            results = await asyncio.gather(
                # --- 五大模块主循环 ---
                self._task_macro_loop(),            # 宏观引擎 (受暂停控制)
                self._radar.run_forever(),           # 微观雷达 WebSocket
                self._exit_mgr.run_forever(),        # 平仓管家 定时扫描
                self._storage.run_forever(),         # 存储 定期清理
                self._perf.run_forever(),            # 效能 定期报告
                # --- 联动协程 ---
                self._task_price_feeder(),           # 价格喂养
                self._task_breaker_watcher(),        # 熔断监听
                self._task_action_executor(),        # 动作执行
                return_exceptions=True,
            )

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"任务 {i} 异常: {result}")

        except asyncio.CancelledError:
            logger.info("⏹️ 系统收到取消信号")
        except Exception as e:
            logger.error(f"💥 系统致命错误: {e}", exc_info=True)
        finally:
            await self.shutdown()

    async def run_single_tick(self):
        """单次测试模式: 只执行一次宏观引擎 tick"""
        if not await self.initialize():
            return None
        result = await self._macro.single_shot()
        if self._data_feed and hasattr(self._data_feed, 'close'):
            await self._data_feed.close()
        return result

    # ════════════════════════════════════════════
    #  联动协程 1: 宏观引擎包装 (可暂停)
    # ════════════════════════════════════════════

    async def _task_macro_loop(self):
        """
        宏观引擎调度器

        不直接调 macro.run_forever()，而是自己控制 tick 节奏，
        以便在熔断时暂停挂单，冷却后恢复。
        """
        logger.info(
            f"🚀 宏观引擎启动 | 交易对={self._config.symbol} | "
            f"刷新={self._config.macro.refresh_interval_sec}s | "
            f"dry_run={self._config.dry_run}"
        )

        while self._running:
            # 检查暂停
            if self._macro_paused:
                logger.info("⏸️ 宏观引擎已暂停 (等待熔断冷却)")
                while self._macro_paused and self._running:
                    await asyncio.sleep(1)
                if self._running:
                    logger.info("▶️ 宏观引擎恢复运行")

            try:
                # 执行一次 tick，获取生成的订单
                orders = await self._macro.tick()

                # 将成功挂出的订单注册到 ExitManager 追踪池
                if orders and not self._config.dry_run:
                    self._register_orders_to_exit_manager(orders)

                # 同时喂给影子引擎
                if orders:
                    self._feed_orders_to_shadow(orders)

            except Exception as e:
                logger.error(f"❌ 宏观引擎 tick 异常: {e}", exc_info=True)

            # 等待下一个周期
            await asyncio.sleep(self._config.macro.refresh_interval_sec)

    def _register_orders_to_exit_manager(self, orders: List[OrderRequest]):
        """
        将宏观引擎成功挂出的订单注册到 ExitManager 追踪池

        active_order_ids 和 orders 是按下单顺序一一对应的，
        用位置索引配对，避免错误匹配。
        """
        active_ids = self._macro._active_order_ids
        if not active_ids:
            return

        # 构建 ID → 订单详情的映射
        # active_ids 是成功下单的 ID，orders 是全部请求（含失败的）
        # 按顺序配对: 第一个成功的 ID 对应第一个 order，以此类推
        idx = 0
        for order in orders:
            if idx >= len(active_ids):
                break
            self._exit_mgr.register_order(
                order_id=active_ids[idx],
                symbol=order.symbol,
                side=order.side.value,
                price=order.price,
                size=order.size,
                zone_name=order.zone_name,
            )
            idx += 1

    def _feed_orders_to_shadow(self, orders: List[OrderRequest]):
        """
        用独立的 shadow_zones 计算影子挂单，与实盘完全解耦。

        数据流:
          实盘: zones (2.5/4.0/7.0) → 01 DEX
          影子: shadow_zones (1.5/3.0/5.0) → ShadowEngine

        共享的只有 EMA 和 ATR (来自宏观引擎的最新计算结果)。
        """
        # 清理旧的未成交挂单
        self._perf.shadow._pending.clear()

        # 读取宏观引擎最新的 EMA 和 ATR
        ema = self._macro._last_ema
        atr = self._macro._last_atr
        if ema <= 0 or atr <= 0:
            return  # 指标未就绪，跳过

        symbol = self._config.symbol
        shadow_cfg = self._config.performance
        total_capital = shadow_cfg.shadow_capital_per_side
        shadow_zones = shadow_cfg.shadow_zones

        # 用独立的 zones 参数计算影子挂单
        for zone in shadow_zones:
            buy_price = round(ema - (zone.atr_multiplier * atr), 1)
            sell_price = round(ema + (zone.atr_multiplier * atr), 1)
            zone_capital = total_capital * zone.capital_ratio
            buy_size = round(zone_capital / buy_price, 6)
            sell_size = round(zone_capital / sell_price, 6)

            self._perf.shadow.place_order(ShadowOrder(
                order_id=f"SB-{zone.atr_multiplier}-{int(time.time())}",
                symbol=symbol,
                side="BUY",
                price=buy_price,
                size=buy_size,
                zone_name=zone.name,
            ))
            self._perf.shadow.place_order(ShadowOrder(
                order_id=f"SS-{zone.atr_multiplier}-{int(time.time())}",
                symbol=symbol,
                side="SELL",
                price=sell_price,
                size=sell_size,
                zone_name=zone.name,
            ))

        logger.debug(
            f"🎭 影子挂单更新: {len(shadow_zones)*2} 个 | "
            f"EMA={ema:,.1f} ATR={atr:,.1f}"
        )

    # ════════════════════════════════════════════
    #  联动协程 2: 价格喂养
    # ════════════════════════════════════════════

    async def _task_price_feeder(self):
        """
        定期获取最新市场价，同时喂给:
        - ExitManager (止盈止损判定)
        - PerformanceMonitor.shadow (影子撮合)

        数据源: 实盘用 01 DEX ticker，dry_run 用币安 K 线最新收盘价
        """
        symbol = self._config.symbol
        interval = self._config.exit_mgr.check_interval_sec

        # 等待系统完全启动
        await asyncio.sleep(3)

        logger.info(f"📊 价格喂养启动 | 交易对={symbol} | 间隔={interval}s")

        while self._running:
            try:
                price = await self._fetch_latest_price(symbol)
                if price and price > 0:
                    # 喂给 ExitManager
                    self._exit_mgr.update_price(symbol, price)
                    # 喂给影子引擎
                    self._perf.shadow.update_price(symbol, price)

            except Exception as e:
                logger.debug(f"价格获取异常: {e}")

            await asyncio.sleep(interval)

    async def _fetch_latest_price(self, symbol: str) -> Optional[float]:
        """
        获取最新价格

        实盘: 通过 01 DEX ticker (带延迟监控)
        dry_run: 通过币安 K 线最新收盘价
        """
        try:
            if self._exchange and not self._config.dry_run:
                async with self._perf.latency.measure("get_ticker"):
                    ticker = await self._exchange.get_ticker(symbol)
                return ticker.mark_price or ticker.last_price
            else:
                # dry_run: 快速拉 1 根最新 K 线
                candles = await self._data_feed.fetch_ohlcv(symbol, "1m", 1)
                if candles:
                    return candles[-1].close
        except Exception:
            pass
        return None

    # ════════════════════════════════════════════
    #  联动协程 3: 熔断监听
    # ════════════════════════════════════════════

    async def _task_breaker_watcher(self):
        """
        监听 MicroRadar 的 circuit_break_event

        一旦触发:
        1. 暂停宏观引擎 (停止新挂单)
        2. 通知 ExitManager 紧急全部平仓
        3. 撤销所有未成交挂单
        4. 等待冷却期结束后恢复

        这是系统的"紧急制动闸"。
        """
        await asyncio.sleep(5)  # 等各模块启动

        logger.info("🚨 熔断监听协程已就绪")

        while self._running:
            # 等待熔断事件
            await self._radar.circuit_break_event.wait()

            if not self._running:
                break

            snapshot = self._radar.snapshot
            logger.warning(
                f"\n{'🚨' * 25}\n"
                f"  熔断联动触发!\n"
                f"  原因: {snapshot.trigger_reason}\n"
                f"{'🚨' * 25}"
            )

            # 1. 暂停宏观引擎
            self._macro_paused = True

            # 2. ExitManager 紧急全部平仓
            self._exit_mgr.force_close_all(CloseReason.CIRCUIT_BREAK)

            # 3. 撤销所有未成交挂单 (实盘)
            if self._exchange and not self._config.dry_run:
                try:
                    async with self._perf.latency.measure("cancel_all"):
                        cancelled = await self._exchange.cancel_all_orders(self._config.symbol)
                    logger.info(f"🗑️ 熔断撤单: 已撤销 {cancelled} 个挂单")
                except Exception as e:
                    logger.error(f"❌ 熔断撤单失败: {e}")

            # 4. 等待冷却期结束
            cooldown = self._config.micro.cooldown_seconds
            logger.info(f"⏳ 进入冷却期: {cooldown}s")
            await asyncio.sleep(cooldown + 2)  # 多等 2s 余量

            # 5. 恢复宏观引擎
            self._macro_paused = False
            logger.info("✅ 冷却结束，宏观引擎即将恢复")

            # 清除 event (radar 冷却结束后也会自动清除)
            self._radar.circuit_break_event.clear()

    # ════════════════════════════════════════════
    #  联动协程 4: 动作执行器
    # ════════════════════════════════════════════

    async def _task_action_executor(self):
        """
        消费 ExitManager 的 action_queue，执行实际操作:

        CANCEL_ORDER    → exchange.cancel_orders()
        CLOSE_POSITION  → exchange.place_orders() (IOC)

        所有已完结的交易写入 LeanStorage。
        """
        await asyncio.sleep(3)  # 等系统启动
        logger.info("⚡ 动作执行器已就绪")

        while self._running:
            try:
                # 带超时的 get，避免永久阻塞导致无法退出
                action: ExitAction = await asyncio.wait_for(
                    self._exit_mgr.action_queue.get(),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

            try:
                if action.action_type == ActionType.CANCEL_ORDER:
                    await self._execute_cancel(action)
                elif action.action_type == ActionType.CLOSE_POSITION:
                    await self._execute_close(action)
            except Exception as e:
                logger.error(f"❌ 动作执行异常: {e}", exc_info=True)

    async def _execute_cancel(self, action: ExitAction):
        """执行撤单动作"""
        logger.info(f"🗑️ 执行撤单: {action.order_id} | {action.cancel_reason}")

        if self._exchange and not self._config.dry_run:
            try:
                async with self._perf.latency.measure("cancel_order"):
                    await self._exchange.cancel_orders(
                        action.symbol, [action.order_id]
                    )
            except Exception as e:
                logger.warning(f"撤单执行失败: {e}")
        else:
            logger.debug(f"[DRY RUN] 模拟撤单: {action.order_id}")

    async def _execute_close(self, action: ExitAction):
        """
        执行平仓动作:
        1. 向交易所发送 IOC 平仓订单
        2. 将交易记录写入 SQLite
        """
        logger.warning(
            f"⚡ 执行平仓: {action.close_side} {action.symbol} "
            f"x{action.close_size} @ IOC {action.close_price:,.2f} | "
            f"原因={action.close_reason}"
        )

        # 1. 发送平仓订单 (实盘)
        if self._exchange and not self._config.dry_run:
            try:
                close_order = OrderRequest(
                    symbol=action.symbol,
                    side=OrderSideEnum(action.close_side),
                    order_type=OrderTypeEnum.LIMIT,  # IOC 限价
                    price=action.close_price,
                    size=action.close_size,
                    post_only=False,  # 平仓不用 post_only
                )

                async with self._perf.latency.measure("close_position"):
                    results = await self._exchange.place_orders([close_order])

                if results and results[0].success:
                    logger.info(f"✅ 平仓成交: ID={results[0].order_id}")
                else:
                    err = results[0].error_msg if results else "无响应"
                    logger.warning(f"⚠️ 平仓可能失败: {err}")

            except Exception as e:
                logger.error(f"❌ 平仓执行异常: {e}")
        else:
            logger.info(f"[DRY RUN] 模拟平仓: {action.close_side} {action.symbol}")

        # 2. 写入 SQLite (无论实盘或 dry_run 都记录)
        try:
            await self._storage.log_trade_from_action(action)
        except Exception as e:
            logger.warning(f"⚠️ 交易记录写入失败: {e}")

    # ════════════════════════════════════════════
    #  即时回调 (同步, 用于日志)
    # ════════════════════════════════════════════

    def _on_circuit_break_callback(self, snapshot):
        """MicroRadar 熔断时的即时回调 (仅日志)"""
        logger.warning(f"🚨 [回调] 熔断信号: {snapshot.trigger_reason}")

    def _on_exit_action_callback(self, action: ExitAction):
        """ExitManager 产生动作时的即时回调 (仅日志)"""
        if action.action_type == ActionType.CANCEL_ORDER:
            logger.info(f"📋 [回调] 撤单指令: {action.order_id}")
        elif action.action_type == ActionType.CLOSE_POSITION:
            logger.info(
                f"📋 [回调] 平仓指令: {action.close_side} {action.symbol} "
                f"| {action.close_reason} | PnL={action.unrealized_pnl_pct:+.2%}"
            )

    # ════════════════════════════════════════════
    #  信号处理 & 优雅关闭
    # ════════════════════════════════════════════

    def _signal_handler(self):
        """SIGINT / SIGTERM 处理"""
        logger.info("📡 收到终止信号 (SIGINT/SIGTERM)")
        self._running = False
        self._shutdown_event.set()
        # 取消所有运行中的任务
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()

    async def shutdown(self):
        """
        优雅关闭流程:
        1. 撤销所有未成交挂单
        2. 等待存储队列清空
        3. 关闭数据源和交易所连接
        4. 生成最终报告
        """
        logger.info("🛑 深海锚系统正在关闭...")
        self._running = False

        # 1. 撤销全部挂单 (实盘)
        if self._exchange and not self._config.dry_run:
            try:
                cancelled = await self._exchange.cancel_all_orders(self._config.symbol)
                logger.info(f"🗑️ 关闭撤单: 已撤销 {cancelled} 个挂单")
            except Exception as e:
                logger.warning(f"关闭撤单异常: {e}")

        # 2. 输出最终调参报告
        if self._perf:
            try:
                report = self._perf.report(title="🐋 深海锚 最终调参报告")
                logger.info(f"\n{report}")
            except Exception:
                pass

        # 3. 关闭存储
        if self._storage:
            try:
                stats = self._storage.stats
                logger.info(f"💾 存储统计: {stats}")
                await self._storage.close()
            except Exception:
                pass

        # 4. 关闭数据源
        if self._data_feed and hasattr(self._data_feed, 'close'):
            try:
                await self._data_feed.close()
            except Exception:
                pass

        # 5. 断开交易所
        if self._exchange:
            try:
                await self._exchange.disconnect()
            except Exception:
                pass

        # 6. 停止子模块
        if self._radar:
            self._radar.stop()
        if self._exit_mgr:
            self._exit_mgr.stop()
        if self._perf:
            self._perf.stop()

        logger.info("👋 深海锚系统已安全关闭")
