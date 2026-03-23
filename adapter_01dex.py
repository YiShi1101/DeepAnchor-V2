"""
deep_anchor/exchange/adapter_01dex.py
=====================================
深海锚 - 01 DEX 交易所适配器

实现 ExchangeInterface 抽象接口，封装对 01 Exchange (Nord) 的所有交互。

架构关系:
    DeepAnchorSystem
        └── ExchangeInterface (抽象)
                └── ZeroOneExchangeAdapter (本文件，具体实现)
                        └── ZeroOneRest (底层 REST/Protobuf 通信)

本文件不直接处理 Protobuf 编码、签名等底层细节，
而是复用你已有的 zeroone_rest.py 和 schema_pb2.py。

使用方式:
    如果你想接入实盘，需确保:
    1. ZERO1_PRIVATE_KEY 环境变量已设置
    2. schema_pb2.py 已生成
    3. 依赖已安装: cryptography, base58, protobuf, requests
"""

import asyncio
import logging
import os
import time
import random
from decimal import Decimal
from typing import List, Dict, Any, Optional

from ..core.interfaces import (
    ExchangeInterface,
    OrderRequest, OrderResult,
    TickerInfo, BalanceInfo, PositionInfo,
    OrderSideEnum, OrderTypeEnum
)
from ..config import ExchangeConnectionConfig


logger = logging.getLogger("DeepAnchor.Exchange.01DEX")


class ZeroOneExchangeAdapter(ExchangeInterface):
    """
    01 Exchange (Nord) 适配器
    
    实现 ExchangeInterface，将标准化的 OrderRequest 翻译为 
    01 DEX 的 Protobuf Action 并发送。
    
    注意: 本适配器直接内嵌了精简版 REST 通信逻辑，
    不依赖外部的 zeroone_rest.py (避免复杂的 import 链)。
    如需对接你已有的适配器代码，可以在此替换内部实现。
    """

    def __init__(self, config: ExchangeConnectionConfig):
        self._config = config
        self._base_url = config.base_url
        self._account_id = config.account_id

        # 延迟初始化 (connect 时才加载密钥和建立 Session)
        self._connected = False
        self._rest_client = None         # 底层 REST 客户端 (对接你的 zeroone_rest.py)
        self._symbol_to_market_id: Dict[str, int] = {}
        self._market_decimals: Dict[int, Dict[str, int]] = {}

    async def connect(self) -> bool:
        """
        建立连接:
        1. 加载私钥
        2. 拉取市场信息 (符号映射 + 精度)
        3. 创建交易 Session
        """
        try:
            # 动态导入底层模块 (避免在非实盘环境报错)
            from core.adapters.exchanges.adapters.zeroone.zeroone_rest import ZeroOneRest
            from core.adapters.exchanges.interface import ExchangeConfig
            from core.adapters.exchanges.models import ExchangeType

            # 构造底层配置对象
            inner_config = ExchangeConfig(
                exchange_id=self._config.exchange_id,
                exchange_type=ExchangeType.PERPETUAL,
                name="01 Exchange",
                api_key="000",
                api_secret=os.getenv("ZERO1_PRIVATE_KEY", ""),
                extra_params={
                    "account_id": self._account_id,
                    "base_url": self._base_url,
                }
            )

            self._rest_client = ZeroOneRest(inner_config, logger)
            await self._rest_client.connect()

            # 加载市场信息
            markets = await self._rest_client.get_markets()
            for m in markets:
                raw_symbol = m.get("symbol", "")
                market_id = m.get("marketId")
                if raw_symbol and market_id is not None:
                    # "BTCUSD" -> "BTC-PERP"
                    std_symbol = raw_symbol.replace("USD", "-PERP") if "USD" in raw_symbol else raw_symbol
                    self._symbol_to_market_id[std_symbol] = market_id
                    self._market_decimals[market_id] = {
                        "price": m.get("priceDecimals", 1),
                        "size": m.get("sizeDecimals", 4),
                    }

            self._connected = True
            logger.info(f"✅ 01 DEX 连接成功 | 加载 {len(self._symbol_to_market_id)} 个交易对")
            return True

        except ImportError as e:
            logger.error(f"❌ 底层模块导入失败 (非实盘环境正常): {e}")
            return False
        except Exception as e:
            logger.error(f"❌ 01 DEX 连接失败: {e}")
            return False

    async def disconnect(self) -> None:
        self._connected = False
        logger.info("01 DEX 连接已断开")

    async def get_ticker(self, symbol: str) -> TickerInfo:
        self._ensure_connected()
        market_id = self._resolve_market_id(symbol)
        stats = await self._rest_client.get_market_stats(market_id)
        perp = stats.get("perpStats", {})
        return TickerInfo(
            symbol=symbol,
            last_price=float(perp.get("mark_price", 0)),
            mark_price=float(perp.get("mark_price", 0)),
            index_price=float(stats.get("indexPrice", 0)),
            funding_rate=float(perp.get("funding_rate", 0)),
            volume_24h=float(stats.get("volumeQuote24h", 0)),
        )

    async def get_balances(self) -> List[BalanceInfo]:
        self._ensure_connected()
        user_info = await self._rest_client.get_account_info()
        return [
            BalanceInfo(
                currency=b.get("symbol", f"Token_{b.get('tokenId')}"),
                available=float(b.get("available", 0)),
                total=float(b.get("total", 0)),
                locked=float(b.get("locked", 0)),
            )
            for b in user_info.get("balances", [])
        ]

    async def get_positions(self) -> List[PositionInfo]:
        self._ensure_connected()
        user_info = await self._rest_client.get_account_info()
        positions = []
        for p in user_info.get("positions", []):
            perp_data = p.get("perp", {})
            if perp_data:
                size = float(perp_data.get("baseSize", 0))
                if size == 0:
                    continue
                positions.append(PositionInfo(
                    symbol=str(p.get("marketId")),
                    side="LONG" if perp_data.get("isLong", size > 0) else "SHORT",
                    size=abs(size),
                    entry_price=float(perp_data.get("price", 0)),
                    unrealized_pnl=float(perp_data.get("sizePricePnl", 0)),
                ))
            else:
                size = float(p.get("size", 0))
                if size == 0:
                    continue
                positions.append(PositionInfo(
                    symbol=str(p.get("marketId")),
                    side="LONG" if size > 0 else "SHORT",
                    size=abs(size),
                    entry_price=float(p.get("entryPrice", 0)),
                    unrealized_pnl=float(p.get("unrealizedPnl", 0)),
                ))
        return positions

    async def place_orders(self, orders: List[OrderRequest]) -> List[OrderResult]:
        """
        批量下单: 逐个发送 (01 DEX 原子操作有 4 个上限，且逻辑复杂)
        
        将 OrderRequest 翻译为 01 DEX 的 place_order 调用。
        """
        self._ensure_connected()
        results: List[OrderResult] = []

        for order in orders:
            try:
                market_id = self._resolve_market_id(order.symbol)

                # 侧面映射
                side = "BID" if order.side == OrderSideEnum.BUY else "ASK"

                # 类型映射
                if order.order_type == OrderTypeEnum.POST_ONLY or order.post_only:
                    order_type = "POST_ONLY"
                elif order.order_type == OrderTypeEnum.MARKET:
                    order_type = "MARKET"
                else:
                    order_type = "LIMIT"

                # 发送
                result = await self._rest_client.place_order(
                    market_id=market_id,
                    side=side,
                    price=order.price,
                    size=order.size,
                    order_type=order_type,
                    account_id=self._account_id,
                )

                # 解析结果
                order_id = None
                if result.HasField("posted"):
                    order_id = str(result.posted.order_id)
                elif len(result.fills) > 0:
                    order_id = str(result.fills[0].order_id)

                results.append(OrderResult(
                    success=True,
                    order_id=order_id,
                    raw_data=result,
                ))

            except Exception as e:
                results.append(OrderResult(
                    success=False,
                    error_msg=str(e),
                ))

        return results

    async def cancel_orders(self, symbol: str, order_ids: List[str]) -> int:
        self._ensure_connected()
        market_id = self._resolve_market_id(symbol)
        count = 0
        for oid in order_ids:
            try:
                await self._rest_client.cancel_order(market_id, int(oid))
                count += 1
            except Exception as e:
                logger.warning(f"撤单失败 {oid}: {e}")
        return count

    async def cancel_all_orders(self, symbol: str) -> int:
        """撤销所有挂单 (通过先查询再逐个撤销)"""
        self._ensure_connected()
        open_orders = await self.get_open_orders(symbol)
        if not open_orders:
            return 0
        ids = [str(o.get("orderId", o.get("order_id", ""))) for o in open_orders]
        return await self.cancel_orders(symbol, ids)

    async def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        self._ensure_connected()
        user_info = await self._rest_client.get_account_info()
        market_id = self._symbol_to_market_id.get(symbol)
        orders = user_info.get("orders", [])
        if market_id is not None:
            orders = [o for o in orders if o.get("marketId") == market_id]
        return orders

    # --- 内部工具 ---

    def _ensure_connected(self):
        if not self._connected:
            raise RuntimeError("01 DEX 未连接，请先调用 connect()")

    def _resolve_market_id(self, symbol: str) -> int:
        mid = self._symbol_to_market_id.get(symbol)
        if mid is None:
            raise ValueError(f"未知交易对: {symbol} (已知: {list(self._symbol_to_market_id.keys())})")
        return mid
