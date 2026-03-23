"""
deep_anchor/exchange/adapter_01dex.py
=====================================
深海锚 - 01 DEX 交易所适配器 (V2 - 完全自包含版)

⚠️ 重要改动 (V2):
本文件不再依赖旧项目的 core.adapters.exchanges... 模块链。
所有 REST/Protobuf 通信逻辑直接内嵌，只需要:
  1. schema_pb2.py 放在同级目录 (exchange/)
  2. 环境变量 ZERO1_PRIVATE_KEY (Base58 编码的私钥)

依赖:
  pip install protobuf cryptography base58 requests
"""

import asyncio
import binascii
import json
import logging
import os
import random
import time
from typing import List, Dict, Any, Optional
from decimal import Decimal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import base58

# --- Protobuf schema 导入 (从同级目录) ---
import importlib
import sys

def _load_schema_pb2():
    """
    动态加载 schema_pb2.py
    按优先级搜索以下路径:
      1. 本文件同级目录 (exchange/schema_pb2.py)
      2. 环境变量 SCHEMA_PB2_PATH 指定的目录
      3. 常见的旧项目路径
    """
    # 路径候选列表
    this_dir = os.path.dirname(os.path.abspath(__file__))
    search_paths = [
        this_dir,
        os.getenv("SCHEMA_PB2_PATH", ""),
        # 旧项目常见路径 (相对于 /root/deep-anchor/)
        os.path.join(this_dir, "..", "..", "core", "adapters", "exchanges", "adapters", "zeroone"),
        os.path.join(this_dir, "..", "..", "..", "core", "adapters", "exchanges", "adapters", "zeroone"),
        "/root/deep-anchor/core/adapters/exchanges/adapters/zeroone",
    ]

    for path in search_paths:
        if not path:
            continue
        candidate = os.path.join(path, "schema_pb2.py")
        if os.path.exists(candidate):
            # 动态加载
            spec = importlib.util.spec_from_file_location("schema_pb2", candidate)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod

    raise ImportError(
        "❌ 找不到 schema_pb2.py！\n"
        "   请将 schema_pb2.py 复制到以下任一位置:\n"
        f"   1. {this_dir}/schema_pb2.py\n"
        "   2. 设置环境变量: export SCHEMA_PB2_PATH=/path/to/dir\n"
        "   或从 proto 文件重新生成:\n"
        "   curl https://zo-devnet.n1.xyz/schema.proto -o schema.proto\n"
        "   protoc --python_out=. schema.proto"
    )


# 延迟加载 (在 connect 时才真正需要)
schema_pb2 = None

from google.protobuf.internal import encoder, decoder

from ..core.interfaces import (
    ExchangeInterface,
    OrderRequest, OrderResult,
    TickerInfo, BalanceInfo, PositionInfo,
    OrderSideEnum, OrderTypeEnum
)
from ..config import ExchangeConnectionConfig


logger = logging.getLogger("DeepAnchor.Exchange.01DEX")


# ============================================================
#  内嵌 REST 客户端 (源自 zeroone_rest.py，精简自包含)
# ============================================================

class _NordRestClient:
    """
    01 Exchange (Nord) REST 通信客户端
    
    职责:
    - 管理 Ed25519 密钥对 (User Key + Session Key)
    - 创建交易 Session
    - 序列化/签名/发送 Protobuf Action
    - 解析 Protobuf Receipt
    - 执行公共 HTTP GET 查询
    """

    def __init__(self, base_url: str, account_id: int = 0):
        self.base_url = base_url.rstrip("/")
        self.account_id = account_id

        # --- 加载私钥 ---
        private_key_str = os.getenv("ZERO1_PRIVATE_KEY", "")
        if not private_key_str:
            raise ValueError(
                "🚨 未设置 ZERO1_PRIVATE_KEY 环境变量!\n"
                "   请执行: export ZERO1_PRIVATE_KEY='your_base58_private_key'"
            )

        key_bytes = base58.b58decode(private_key_str)
        if len(key_bytes) == 64:
            key_bytes = key_bytes[:32]  # Solana keypair 格式: 前 32 字节是私钥

        self._master_key = Ed25519PrivateKey.from_private_bytes(key_bytes)
        self._user_pubkey_bytes = self._master_key.public_key().public_bytes_raw()
        self._user_pubkey_str = base58.b58encode(self._user_pubkey_bytes).decode()
        logger.info(f"🔑 User PubKey: {self._user_pubkey_str}")

        # --- Session 状态 ---
        self._session_key: Optional[Ed25519PrivateKey] = None
        self._session_id: Optional[int] = None
        self._session_expiry: int = 0

        # --- HTTP Session (带重试) ---
        self._http = requests.Session()
        retries = Retry(total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
        self._http.mount("https://", HTTPAdapter(max_retries=retries))
        self._http.headers.update({"User-Agent": "DeepAnchor/1.0"})

        # --- 市场精度缓存 ---
        self._market_decimals: Dict[int, Dict[str, int]] = {}

    # --------------------------------------------------------
    #  连接 & Session
    # --------------------------------------------------------

    async def connect(self):
        """建立连接: 加载市场 + 创建 Session"""
        await self.load_markets()
        await self.ensure_session()
        logger.info("✅ 01 DEX REST 客户端连接成功")

    async def ensure_session(self):
        """确保有一个有效的 Session (过期则自动续期)"""
        now = int(time.time())
        if self._session_id and now < self._session_expiry - 60:
            return  # Session 仍有效

        logger.info("🔄 创建新的交易 Session...")
        self._session_key = Ed25519PrivateKey.generate()
        session_pubkey = self._session_key.public_key().public_bytes_raw()

        server_ts = await self._get_server_timestamp()

        action = schema_pb2.Action()
        action.nonce = random.randint(1, 1_000_000)
        action.current_timestamp = server_ts
        action.create_session.user_pubkey = self._user_pubkey_bytes
        action.create_session.session_pubkey = session_pubkey
        action.create_session.expiry_timestamp = server_ts + 3600

        receipt = await self._post_action(action, is_create_session=True)

        if receipt.HasField("err"):
            err_name = self._error_name(receipt.err)
            raise Exception(f"Session 创建失败: {err_name}")

        self._session_id = receipt.create_session_result.session_id
        self._session_expiry = server_ts + 3600
        logger.info(f"✅ Session 创建成功! ID: {self._session_id}")

    # --------------------------------------------------------
    #  交易操作
    # --------------------------------------------------------

    async def place_order(
        self, market_id: int, side: str, price: float, size: float,
        order_type: str = "LIMIT"
    ):
        """
        下单

        Args:
            market_id: 市场 ID (0=BTC, 1=ETH, ...)
            side: "BID" / "ASK"
            price: 人类可读价格 (如 67000.5)
            size: 人类可读数量 (如 0.15)
            order_type: "LIMIT" / "POST_ONLY" / "MARKET"

        Returns:
            schema_pb2.Receipt.PlaceOrderResult
        """
        await self.ensure_session()

        decimals = self._market_decimals.get(market_id)
        if not decimals:
            raise Exception(f"未知 Market ID: {market_id}")

        # 转换为链上整数
        raw_price = int(round(price * (10 ** decimals["price_decimals"])))
        raw_size = int(round(size * (10 ** decimals["size_decimals"])))

        server_ts = await self._get_server_timestamp()

        action = schema_pb2.Action()
        action.nonce = random.randint(1, 1_000_000)
        action.current_timestamp = server_ts
        action.place_order.session_id = self._session_id
        action.place_order.market_id = market_id
        action.place_order.price = raw_price
        action.place_order.size = raw_size

        # 设置 sender_account_id (如果配置了特定子账户)
        if self.account_id and self.account_id > 0:
            action.place_order.sender_account_id = self.account_id

        # Side
        if side.upper() in ("BID", "BUY"):
            action.place_order.side = schema_pb2.Side.BID
        else:
            action.place_order.side = schema_pb2.Side.ASK

        # FillMode
        if order_type.upper() == "MARKET":
            action.place_order.fill_mode = schema_pb2.FillMode.FILL_OR_KILL
        elif order_type.upper() == "POST_ONLY":
            action.place_order.fill_mode = schema_pb2.FillMode.POST_ONLY
        else:
            action.place_order.fill_mode = schema_pb2.FillMode.LIMIT

        receipt = await self._post_action(action)

        if receipt.HasField("err"):
            err_name = self._error_name(receipt.err)
            raise Exception(f"下单失败: {err_name} (Code: {receipt.err})")

        return receipt.place_order_result

    async def cancel_order(self, order_id: int):
        """撤销指定订单"""
        await self.ensure_session()
        server_ts = await self._get_server_timestamp()

        action = schema_pb2.Action()
        action.nonce = random.randint(1, 1_000_000)
        action.current_timestamp = server_ts
        action.cancel_order_by_id.session_id = self._session_id
        action.cancel_order_by_id.order_id = int(order_id)

        if self.account_id and self.account_id > 0:
            action.cancel_order_by_id.sender_account_id = self.account_id

        receipt = await self._post_action(action)

        if receipt.HasField("err"):
            err_name = self._error_name(receipt.err)
            if "NOT_FOUND" in err_name:
                logger.warning(f"撤单: {err_name} (订单可能已成交/已撤)")
                return True
            raise Exception(f"撤单失败: {err_name}")

        return True

    # --------------------------------------------------------
    #  查询操作 (公共 GET)
    # --------------------------------------------------------

    async def load_markets(self) -> list:
        """加载市场信息 (符号、精度)"""
        data = await self._http_get(f"{self.base_url}/info")
        markets = data.get("markets", [])
        for m in markets:
            mid = m.get("marketId")
            if mid is not None:
                self._market_decimals[mid] = {
                    "price_decimals": int(m.get("priceDecimals", 0)),
                    "size_decimals": int(m.get("sizeDecimals", 0)),
                }
        return markets

    async def get_market_stats(self, market_id: int) -> dict:
        """获取市场统计 (价格、funding rate)"""
        return await self._http_get(f"{self.base_url}/market/{market_id}/stats")

    async def get_user_info(self) -> dict:
        """
        获取用户信息 (余额、持仓、挂单)
        如果配置了 account_id，会额外请求子账户详情
        """
        user_data = await self._http_get(f"{self.base_url}/user/{self._user_pubkey_str}")

        # 如果配置了特定子账户，请求子账户详情
        if self.account_id and self.account_id > 0:
            try:
                acct_data = await self._http_get(f"{self.base_url}/account/{self.account_id}")
                # 合并 balances
                if "balances" in acct_data:
                    user_data["balances"] = [
                        {
                            "symbol": b.get("token", "UNKNOWN"),
                            "available": str(b.get("amount", 0)),
                            "total": str(b.get("amount", 0)),
                            "locked": "0",
                        }
                        for b in acct_data["balances"]
                    ]
                # 合并 positions & orders
                if "positions" in acct_data:
                    user_data["positions"] = acct_data["positions"]
                if "orders" in acct_data:
                    user_data["orders"] = acct_data["orders"]
            except Exception as e:
                logger.warning(f"⚠️ 获取子账户 {self.account_id} 失败: {e}")

        return user_data

    # --------------------------------------------------------
    #  底层通信
    # --------------------------------------------------------

    async def _post_action(self, action, is_create_session=False):
        """
        序列化、签名、发送 Protobuf Action，返回 Receipt

        签名方案:
        - CreateSession: user_sign = ed25519(hex(raw_msg))
        - 其他操作:     session_sign = ed25519(raw_msg)
        """
        payload = action.SerializeToString()
        varint_len = encoder._VarintBytes(len(payload))
        raw_msg = varint_len + payload

        if is_create_session:
            # User 签名: 对 hex 编码的消息签名
            signature = self._master_key.sign(binascii.hexlify(raw_msg))
        else:
            # Session 签名: 对原始字节签名
            signature = self._session_key.sign(raw_msg)

        final_payload = raw_msg + signature
        url = f"{self.base_url}/action"

        loop = asyncio.get_running_loop()

        def _do_post():
            resp = self._http.post(
                url,
                data=final_payload,
                headers={"Content-Type": "application/octet-stream"},
                timeout=10,
            )
            if resp.status_code != 200:
                raise Exception(f"HTTP {resp.status_code}: {resp.text[:200]}")
            if not resp.content:
                raise Exception("空响应")

            msg_len, pos = decoder._DecodeVarint32(resp.content, 0)
            receipt = schema_pb2.Receipt()
            receipt.ParseFromString(resp.content[pos : pos + msg_len])
            return receipt

        return await loop.run_in_executor(None, _do_post)

    async def _get_server_timestamp(self) -> int:
        """获取服务器时间戳 (用于签名)"""
        loop = asyncio.get_running_loop()
        def _do():
            resp = self._http.get(f"{self.base_url}/timestamp", timeout=5)
            return int(resp.json())
        return await loop.run_in_executor(None, _do)

    async def _http_get(self, url: str, params=None) -> dict:
        """通用 GET 请求"""
        loop = asyncio.get_running_loop()
        def _do():
            resp = self._http.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                raise Exception(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return resp.json()
        return await loop.run_in_executor(None, _do)

    @staticmethod
    def _error_name(err_code) -> str:
        """将错误码转为人类可读名称"""
        try:
            return schema_pb2.Error.Name(err_code)
        except (ValueError, AttributeError):
            return str(err_code)


# ============================================================
#  适配器主类 (实现 ExchangeInterface)
# ============================================================

class ZeroOneExchangeAdapter(ExchangeInterface):
    """
    01 Exchange (Nord) 适配器 - 完全自包含版

    实现 ExchangeInterface，将标准化的 OrderRequest
    翻译为 01 DEX 的 Protobuf Action。

    不依赖旧项目的任何文件 (除了 schema_pb2.py)。
    """

    def __init__(self, config: ExchangeConnectionConfig):
        self._config = config
        self._base_url = config.base_url
        self._account_id = config.account_id
        self._connected = False

        self._rest: Optional[_NordRestClient] = None
        self._symbol_to_market_id: Dict[str, int] = {}
        self._market_id_to_symbol: Dict[int, str] = {}

    async def connect(self) -> bool:
        """建立连接"""
        global schema_pb2
        try:
            # 加载 protobuf schema
            schema_pb2 = _load_schema_pb2()
            logger.info(f"✅ schema_pb2.py 加载成功")

            # 创建 REST 客户端
            self._rest = _NordRestClient(self._base_url, self._account_id)
            await self._rest.connect()

            # 构建符号映射 (BTCUSD → BTC-PERP)
            markets = await self._rest.load_markets()
            for m in markets:
                raw_sym = m.get("symbol", "")
                mid = m.get("marketId")
                if raw_sym and mid is not None:
                    std = raw_sym.replace("USD", "-PERP") if "USD" in raw_sym else raw_sym
                    self._symbol_to_market_id[std] = mid
                    self._market_id_to_symbol[mid] = std

            self._connected = True
            logger.info(
                f"✅ 01 DEX 适配器连接成功 | "
                f"账户={self._account_id} | "
                f"交易对={len(self._symbol_to_market_id)}个 | "
                f"已知: {list(self._symbol_to_market_id.keys())[:5]}"
            )
            return True

        except Exception as e:
            logger.error(f"❌ 01 DEX 连接失败: {e}", exc_info=True)
            return False

    async def disconnect(self) -> None:
        self._connected = False
        logger.info("01 DEX 连接已断开")

    def _ensure_connected(self):
        if not self._connected:
            raise RuntimeError("01 DEX 未连接，请先调用 connect()")

    def _resolve_market_id(self, symbol: str) -> int:
        mid = self._symbol_to_market_id.get(symbol)
        if mid is None:
            raise ValueError(f"未知交易对: {symbol} (已知: {list(self._symbol_to_market_id.keys())})")
        return mid

    # --------------------------------------------------------
    #  ExchangeInterface 实现
    # --------------------------------------------------------

    async def get_ticker(self, symbol: str) -> TickerInfo:
        self._ensure_connected()
        mid = self._resolve_market_id(symbol)
        stats = await self._rest.get_market_stats(mid)
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
        info = await self._rest.get_user_info()
        return [
            BalanceInfo(
                currency=b.get("symbol", f"Token_{b.get('tokenId')}"),
                available=float(b.get("available", 0)),
                total=float(b.get("total", 0)),
                locked=float(b.get("locked", 0)),
            )
            for b in info.get("balances", [])
        ]

    async def get_positions(self) -> List[PositionInfo]:
        self._ensure_connected()
        info = await self._rest.get_user_info()
        positions = []
        for p in info.get("positions", []):
            perp_data = p.get("perp", {})
            if perp_data:
                sz = float(perp_data.get("baseSize", 0))
                if sz == 0:
                    continue
                positions.append(PositionInfo(
                    symbol=self._market_id_to_symbol.get(p.get("marketId"), str(p.get("marketId"))),
                    side="LONG" if perp_data.get("isLong", sz > 0) else "SHORT",
                    size=abs(sz),
                    entry_price=float(perp_data.get("price", 0)),
                    unrealized_pnl=float(perp_data.get("sizePricePnl", 0)),
                ))
            else:
                sz = float(p.get("size", 0))
                if sz == 0:
                    continue
                positions.append(PositionInfo(
                    symbol=self._market_id_to_symbol.get(p.get("marketId"), str(p.get("marketId"))),
                    side="LONG" if sz > 0 else "SHORT",
                    size=abs(sz),
                    entry_price=float(p.get("entryPrice", 0)),
                    unrealized_pnl=float(p.get("unrealizedPnl", 0)),
                ))
        return positions

    async def place_orders(self, orders: List[OrderRequest]) -> List[OrderResult]:
        """批量下单 (逐个发送，01 DEX 原子操作有 4 个上限)"""
        self._ensure_connected()
        results: List[OrderResult] = []

        for order in orders:
            try:
                mid = self._resolve_market_id(order.symbol)
                side = "BID" if order.side == OrderSideEnum.BUY else "ASK"

                if order.order_type == OrderTypeEnum.POST_ONLY or order.post_only:
                    otype = "POST_ONLY"
                elif order.order_type == OrderTypeEnum.MARKET:
                    otype = "MARKET"
                else:
                    otype = "LIMIT"

                result = await self._rest.place_order(
                    market_id=mid, side=side,
                    price=order.price, size=order.size,
                    order_type=otype,
                )

                oid = None
                if result.HasField("posted"):
                    oid = str(result.posted.order_id)
                elif len(result.fills) > 0:
                    oid = str(result.fills[0].order_id)

                results.append(OrderResult(success=True, order_id=oid, raw_data=result))

            except Exception as e:
                results.append(OrderResult(success=False, error_msg=str(e)))

        return results

    async def cancel_orders(self, symbol: str, order_ids: List[str]) -> int:
        self._ensure_connected()
        count = 0
        for oid in order_ids:
            try:
                await self._rest.cancel_order(int(oid))
                count += 1
            except Exception as e:
                logger.warning(f"撤单失败 {oid}: {e}")
        return count

    async def cancel_all_orders(self, symbol: str) -> int:
        """撤销所有挂单 (先查询再逐个撤)"""
        self._ensure_connected()
        open_orders = await self.get_open_orders(symbol)
        if not open_orders:
            return 0
        ids = [str(o.get("orderId", o.get("order_id", ""))) for o in open_orders if o.get("orderId") or o.get("order_id")]
        return await self.cancel_orders(symbol, ids)

    async def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        self._ensure_connected()
        info = await self._rest.get_user_info()
        mid = self._symbol_to_market_id.get(symbol)
        orders = info.get("orders", [])
        if mid is not None:
            orders = [o for o in orders if o.get("marketId") == mid]
        return orders
