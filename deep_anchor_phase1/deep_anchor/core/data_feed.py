"""
deep_anchor/core/data_feed.py
=============================
深海锚 - 数据管道: 币安公共合约 K 线拉取器

核心设计:
01 DEX 不提供历史 K 线接口，因此直接调用 **币安公共合约 API**
作为行情数据源。该接口无需 API Key，完全公开免费。

HTTP 实现策略:
    优先使用 aiohttp (如已安装，性能更好)
    回退使用 stdlib urllib + run_in_executor (零依赖)

API: GET https://fapi.binance.com/fapi/v1/klines
"""

import asyncio
import json
import logging
import ssl
import urllib.request
import urllib.parse
from typing import List, Optional

from .interfaces import DataFeedInterface, OHLCV
from ..config import DataFeedConfig

# 尝试导入 aiohttp (可选依赖)
try:
    import aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    _HAS_AIOHTTP = False


logger = logging.getLogger("DeepAnchor.DataFeed")


class BinanceFuturesDataFeed(DataFeedInterface):
    """
    币安合约公共 K 线数据源

    自动选择最佳 HTTP 后端:
    - 有 aiohttp: 使用原生异步连接池
    - 无 aiohttp: 使用 urllib + ThreadPool (同样非阻塞)
    """

    def __init__(self, config: DataFeedConfig):
        self._config = config
        self._base_url = config.binance_futures_base
        self._endpoint = config.kline_endpoint
        self._symbol_map = config.symbol_map
        self._timeout = config.request_timeout_sec
        self._aio_session = None
        self._ssl_ctx = ssl.create_default_context()

        backend = "aiohttp" if _HAS_AIOHTTP else "urllib (stdlib)"
        logger.info(f"数据管道初始化 | HTTP后端: {backend}")

    async def close(self):
        """关闭连接池"""
        if _HAS_AIOHTTP and self._aio_session is not None:
            try:
                if not self._aio_session.closed:
                    await self._aio_session.close()
            except Exception:
                pass

    def _map_symbol(self, internal_symbol: str) -> str:
        """系统符号 -> 币安合约符号 (如 BTC-PERP -> BTCUSDT)"""
        if internal_symbol in self._symbol_map:
            return self._symbol_map[internal_symbol]
        base = internal_symbol.replace("-PERP", "").replace("-perp", "")
        guessed = f"{base}USDT"
        logger.warning(f"符号 '{internal_symbol}' 不在映射表，推断为 '{guessed}'")
        return guessed

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100
    ) -> List[OHLCV]:
        """
        从币安合约 API 拉取历史 K 线

        每根 K 线返回格式: [open_time, open, high, low, close, volume, close_time, ...]
        """
        binance_symbol = self._map_symbol(symbol)
        url = f"{self._base_url}{self._endpoint}"
        params = {
            "symbol": binance_symbol,
            "interval": timeframe,
            "limit": min(limit, 1500),
        }

        logger.info(f"📡 拉取K线: {binance_symbol} | 周期={timeframe} | 数量={params['limit']}")

        if _HAS_AIOHTTP:
            raw_data = await self._fetch_with_aiohttp(url, params)
        else:
            raw_data = await self._fetch_with_urllib(url, params)

        candles: List[OHLCV] = []
        for row in raw_data:
            candles.append(OHLCV(
                timestamp=int(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
                close_time=int(row[6]),
            ))

        if candles:
            logger.info(f"✅ 获取 {len(candles)} 根K线 | 最新收盘: {candles[-1].close:,.2f}")
        else:
            logger.warning("⚠️ K线数据为空!")

        return candles

    async def _fetch_with_aiohttp(self, url: str, params: dict) -> list:
        """aiohttp 原生异步拉取"""
        if self._aio_session is None or self._aio_session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._aio_session = aiohttp.ClientSession(timeout=timeout)

        async with self._aio_session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"币安 API HTTP {resp.status}: {text[:200]}")
            return await resp.json()

    async def _fetch_with_urllib(self, url: str, params: dict) -> list:
        """urllib + ThreadPool 非阻塞拉取 (零依赖)"""
        query_string = urllib.parse.urlencode(params)
        full_url = f"{url}?{query_string}"
        loop = asyncio.get_running_loop()

        def _blocking_fetch():
            req = urllib.request.Request(
                full_url,
                headers={"User-Agent": "DeepAnchor/1.0"},
            )
            with urllib.request.urlopen(req, timeout=self._timeout, context=self._ssl_ctx) as resp:
                if resp.status != 200:
                    raise Exception(f"币安 API HTTP {resp.status}")
                return json.loads(resp.read())

        return await loop.run_in_executor(None, _blocking_fetch)
