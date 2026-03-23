"""
deep_anchor/core/macro_engine.py
================================
深海锚 - 模块一: 宏观测算与阶梯预埋引擎 (Macro Engine)

核心职责:
1. 低频拉取 1H K 线 (来自币安公共 API)
2. 计算 EMA(20) 作为基准价格锚点
3. 计算 ATR(14) 作为波动率标尺
4. 在 EMA 上下方，基于 ATR 乘数分层预埋挂单 (三道防线)

挂单公式:
    买单价格 = EMA - (N × ATR)
    卖单价格 = EMA + (N × ATR)
    其中 N 是各层级的 ATR 乘数

三道防线:
    浅水区 (Shallow): N=2.5, 资金占比 20%
    甜点区 (Sweet):   N=4.0, 资金占比 30%
    深海区 (Deep):    N=7.0, 资金占比 50%

运行模式:
    dry_run=True  -> 仅计算并打印，不发送任何订单 (安全测试)
    dry_run=False -> 计算后通过 ExchangeInterface 实际下单
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from .interfaces import (
    DataFeedInterface, ExchangeInterface,
    OrderRequest, OrderResult, OrderSideEnum, OrderTypeEnum, OHLCV
)
from .indicators import calculate_ema, calculate_atr
from ..config import MacroEngineConfig, OrderZone


logger = logging.getLogger("DeepAnchor.MacroEngine")


# ============================================================
#  宏观引擎核心类
# ============================================================

class MacroEngine:
    """
    模块一: 宏观测算与阶梯预埋引擎

    生命周期:
        1. __init__: 注入配置、数据源、交易所接口
        2. run_forever: 进入主循环，周期性执行 tick()
        3. tick: 单次执行 = 拉取K线 → 计算指标 → 生成订单 → 发送/打印

    设计亮点:
        - 数据源与交易所完全解耦 (都通过接口注入)
        - 支持 dry_run 模式，可安全测试不碰真金白银
        - 每次 tick 会先撤销旧挂单再布新单，避免重复挂单
    """

    def __init__(
        self,
        config: MacroEngineConfig,
        data_feed: DataFeedInterface,
        exchange: Optional[ExchangeInterface] = None,
        symbol: str = "BTC-PERP",
        dry_run: bool = True,
    ):
        self._config = config
        self._data_feed = data_feed
        self._exchange = exchange
        self._symbol = symbol
        self._dry_run = dry_run

        # 内部状态
        self._active_order_ids: List[str] = []  # 当前挂单 ID 列表
        self._last_ema: float = 0.0
        self._last_atr: float = 0.0
        self._tick_count: int = 0

    # --------------------------------------------------------
    #  公开方法
    # --------------------------------------------------------

    async def run_forever(self):
        """
        模块一主循环 (永续运行)
        
        每隔 refresh_interval_sec 秒执行一次 tick。
        首次启动立即执行，不等待。
        """
        logger.info(
            f"🚀 宏观引擎启动 | 交易对={self._symbol} | "
            f"刷新间隔={self._config.refresh_interval_sec}s | "
            f"dry_run={self._dry_run}"
        )

        while True:
            try:
                await self.tick()
            except Exception as e:
                logger.error(f"❌ 宏观引擎 tick 异常: {e}", exc_info=True)

            # 等待下一个周期
            await asyncio.sleep(self._config.refresh_interval_sec)

    async def tick(self) -> Optional[List[OrderRequest]]:
        """
        单次执行周期:
        1. 拉取 K 线
        2. 计算 EMA & ATR
        3. 生成三道防线的挂单方案
        4. 执行 (dry_run 时仅打印)

        Returns:
            生成的订单请求列表 (无论是否实际执行)
        """
        self._tick_count += 1
        tick_id = f"T{self._tick_count:04d}"
        logger.info(f"\n{'='*60}")
        logger.info(f"⏰ [{tick_id}] 宏观引擎 Tick 开始 | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        logger.info(f"{'='*60}")

        # --- Step 1: 拉取 K 线 ---
        candles = await self._fetch_candles()
        if not candles:
            logger.error(f"[{tick_id}] K 线数据为空，跳过本轮")
            return None

        # --- Step 2: 计算指标 ---
        ema, atr = self._compute_indicators(candles)
        self._last_ema = ema
        self._last_atr = atr

        last_close = candles[-1].close
        logger.info(
            f"📊 [{tick_id}] 指标计算完成:\n"
            f"   ├── 最新收盘价: {last_close:>12,.2f} USDC\n"
            f"   ├── EMA({self._config.ema_period}):    {ema:>12,.2f} USDC (基准锚点)\n"
            f"   ├── ATR({self._config.atr_period}):    {atr:>12,.2f} USDC (波动标尺)\n"
            f"   └── ATR/EMA:     {atr/ema*100:>11.4f}%  (相对波动率)"
        )

        # --- Step 3: 生成挂单方案 ---
        buy_orders, sell_orders = self._generate_order_plan(ema, atr, last_close)
        all_orders = buy_orders + sell_orders

        # --- Step 4: 漂亮打印挂单方案 ---
        self._print_order_plan(tick_id, ema, atr, last_close, buy_orders, sell_orders)

        # --- Step 5: 执行 (dry_run 时跳过) ---
        if not self._dry_run and self._exchange:
            await self._execute_orders(all_orders)
        elif self._dry_run:
            logger.info(f"🏖️ [{tick_id}] dry_run 模式，挂单方案仅供预览，未发送任何交易指令")

        return all_orders

    async def single_shot(self) -> Optional[List[OrderRequest]]:
        """
        单次执行 (非循环) —— 专为测试入口设计
        
        等价于调用一次 tick()，但语义更清晰。
        """
        return await self.tick()

    # --------------------------------------------------------
    #  内部方法: 数据获取
    # --------------------------------------------------------

    async def _fetch_candles(self) -> List[OHLCV]:
        """拉取 K 线数据"""
        try:
            candles = await self._data_feed.fetch_ohlcv(
                symbol=self._symbol,
                timeframe=self._config.kline_timeframe,
                limit=self._config.kline_limit,
            )
            return candles
        except Exception as e:
            logger.error(f"K 线拉取失败: {e}")
            return []

    # --------------------------------------------------------
    #  内部方法: 指标计算
    # --------------------------------------------------------

    def _compute_indicators(self, candles: List[OHLCV]) -> Tuple[float, float]:
        """
        计算 EMA 和 ATR

        Returns:
            (ema, atr) 元组
        """
        ema = calculate_ema(candles, self._config.ema_period)
        atr = calculate_atr(candles, self._config.atr_period)
        return ema, atr

    # --------------------------------------------------------
    #  内部方法: 挂单方案生成
    # --------------------------------------------------------

    def _generate_order_plan(
        self,
        ema: float,
        atr: float,
        last_close: float,
    ) -> Tuple[List[OrderRequest], List[OrderRequest]]:
        """
        基于 EMA 和 ATR 生成三道防线的挂单方案

        逻辑:
        - 对每个 Zone，计算:
            buy_price  = EMA - (N × ATR)
            sell_price = EMA + (N × ATR)
        - 根据该 Zone 的资金占比，计算分配金额
        - 再根据挂单价格，换算为 base asset 的数量 (size)

        Returns:
            (buy_orders, sell_orders) 两个列表
        """
        buy_orders: List[OrderRequest] = []
        sell_orders: List[OrderRequest] = []
        total_capital = self._config.total_capital_per_side

        for zone in self._config.zones:
            # --- 价格计算 ---
            buy_price = ema - (zone.atr_multiplier * atr)
            sell_price = ema + (zone.atr_multiplier * atr)

            # --- 资金分配 ---
            zone_capital = total_capital * zone.capital_ratio

            # --- 数量计算: size = capital / price ---
            # 保留合理精度 (BTC 用 6 位，其他可调)
            buy_size = round(zone_capital / buy_price, 6)
            sell_size = round(zone_capital / sell_price, 6)

            # --- 价格精度 (BTC 精度 0.1 USDC) ---
            buy_price = round(buy_price, 1)
            sell_price = round(sell_price, 1)

            # --- 构建买单 ---
            buy_orders.append(OrderRequest(
                symbol=self._symbol,
                side=OrderSideEnum.BUY,
                order_type=OrderTypeEnum.POST_ONLY if self._config.post_only else OrderTypeEnum.LIMIT,
                price=buy_price,
                size=buy_size,
                post_only=self._config.post_only,
                notional_usdc=zone_capital,
                zone_name=zone.name,
                client_order_id=f"DA_B_{zone.atr_multiplier}_{int(time.time())}",
            ))

            # --- 构建卖单 ---
            sell_orders.append(OrderRequest(
                symbol=self._symbol,
                side=OrderSideEnum.SELL,
                order_type=OrderTypeEnum.POST_ONLY if self._config.post_only else OrderTypeEnum.LIMIT,
                price=sell_price,
                size=sell_size,
                post_only=self._config.post_only,
                notional_usdc=zone_capital,
                zone_name=zone.name,
                client_order_id=f"DA_S_{zone.atr_multiplier}_{int(time.time())}",
            ))

        return buy_orders, sell_orders

    # --------------------------------------------------------
    #  内部方法: 控制台可视化
    # --------------------------------------------------------

    def _print_order_plan(
        self,
        tick_id: str,
        ema: float,
        atr: float,
        last_close: float,
        buy_orders: List[OrderRequest],
        sell_orders: List[OrderRequest],
    ):
        """在控制台漂亮打印三道防线的挂单方案"""

        header = (
            f"\n"
            f"╔══════════════════════════════════════════════════════════════════╗\n"
            f"║           深海锚 (Deep Anchor) - 阶梯预埋方案 [{tick_id}]           ║\n"
            f"╠══════════════════════════════════════════════════════════════════╣\n"
            f"║  交易对: {self._symbol:<12}  |  单边总额: {self._config.total_capital_per_side:>10,.0f} USDC    ║\n"
            f"║  EMA({self._config.ema_period}): {ema:>12,.2f}   |  ATR({self._config.atr_period}):  {atr:>10,.2f} USDC    ║\n"
            f"║  最新价: {last_close:>12,.2f}   |  EMA偏离:  {(last_close-ema)/ema*100:>+9.4f}%      ║\n"
            f"╠══════════════════════════════════════════════════════════════════╣"
        )
        print(header)

        # --- 卖单区 (由远及近) ---
        print(f"║{'':>2}{'📈 卖单 (SELL / ASK) — 高于 EMA':^62}║")
        print(f"║{'─'*64}║")
        for i, order in enumerate(reversed(sell_orders)):
            dist_pct = (order.price - ema) / ema * 100
            print(
                f"║  {order.zone_name:<25} │ "
                f"${order.price:>10,.1f}  │ "
                f"{order.size:>10.6f} BTC │ "
                f"${order.notional_usdc:>8,.0f} ║"
            )
            print(
                f"║  {'':25} │ "
                f"EMA+{dist_pct:>5.2f}% │ "
                f"N={self._config.zones[len(sell_orders)-1-i].atr_multiplier:>3.1f}×ATR  │ "
                f"{'':>8} ║"
            )

        # --- EMA 基准线 ---
        print(f"║{'─'*64}║")
        print(f"║{'':>4}{'⚓ EMA 基准锚点':^22} ── ${ema:>10,.2f} ──{'─'*22}║")
        print(f"║{'─'*64}║")

        # --- 买单区 (由近及远) ---
        print(f"║{'':>2}{'📉 买单 (BUY / BID) — 低于 EMA':^62}║")
        print(f"║{'─'*64}║")
        for i, order in enumerate(buy_orders):
            dist_pct = (ema - order.price) / ema * 100
            print(
                f"║  {order.zone_name:<25} │ "
                f"${order.price:>10,.1f}  │ "
                f"{order.size:>10.6f} BTC │ "
                f"${order.notional_usdc:>8,.0f} ║"
            )
            print(
                f"║  {'':25} │ "
                f"EMA-{dist_pct:>5.2f}% │ "
                f"N={self._config.zones[i].atr_multiplier:>3.1f}×ATR  │ "
                f"{'':>8} ║"
            )

        # --- 汇总 ---
        total_buy = sum(o.notional_usdc for o in buy_orders)
        total_sell = sum(o.notional_usdc for o in sell_orders)
        print(f"║{'─'*64}║")
        print(
            f"║  💰 汇总: 买单 ${total_buy:>10,.0f}  |  "
            f"卖单 ${total_sell:>10,.0f}  |  "
            f"总计 ${total_buy+total_sell:>10,.0f} ║"
        )
        print(
            f"║  🔒 约束: post_only={'✅' if self._config.post_only else '❌':<5} | "
            f"模式={'🏖️ 模拟(DRY RUN)' if self._dry_run else '🔴 实盘(LIVE)':>20}"
            f"{'':>10}║"
        )
        print(f"╚══════════════════════════════════════════════════════════════════╝\n")

    # --------------------------------------------------------
    #  内部方法: 实际下单执行
    # --------------------------------------------------------

    async def _execute_orders(self, orders: List[OrderRequest]):
        """
        通过 ExchangeInterface 执行下单

        流程:
        1. 撤销此前的所有挂单 (避免重复)
        2. 批量发送新挂单
        3. 记录新的 order_id 列表
        """
        if not self._exchange:
            logger.warning("交易所接口未注入，跳过下单")
            return

        # Step 1: 撤旧
        if self._active_order_ids:
            logger.info(f"🗑️ 撤销旧挂单 ({len(self._active_order_ids)} 个)...")
            cancelled = await self._exchange.cancel_all_orders(self._symbol)
            logger.info(f"✅ 成功撤销 {cancelled} 个旧挂单")
            self._active_order_ids.clear()

        # Step 2: 下新
        logger.info(f"📤 发送 {len(orders)} 个新挂单...")
        results: List[OrderResult] = await self._exchange.place_orders(orders)

        # Step 3: 记录
        success_count = 0
        for req, res in zip(orders, results):
            if res.success and res.order_id:
                self._active_order_ids.append(res.order_id)
                success_count += 1
                logger.info(f"  ✅ {req.zone_name} {req.side.value} @ {req.price:.1f} -> ID: {res.order_id}")
            else:
                logger.warning(f"  ❌ {req.zone_name} {req.side.value} @ {req.price:.1f} -> 失败: {res.error_msg}")

        logger.info(f"📊 下单结果: {success_count}/{len(orders)} 成功")
