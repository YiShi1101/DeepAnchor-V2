"""
deep_anchor/core/interfaces.py
==============================
深海锚 (Deep Anchor) - 核心抽象接口层

定义所有外部依赖 (交易所、数据源) 的抽象协议。
上层策略逻辑只依赖接口，不依赖具体实现，便于测试和替换。

设计原则:
- 采用 Python ABC (Abstract Base Class) 强制子类实现关键方法
- 所有 I/O 方法均为 async，适配高并发事件循环
- 返回值使用标准 dataclass，避免裸 dict 传递
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum


# ============================================================
#  一、标准化数据结构 (Data Transfer Objects)
# ============================================================

@dataclass
class OHLCV:
    """单根 K 线数据"""
    timestamp: int          # 开盘时间 (毫秒级 Unix 时间戳)
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int = 0     # 收盘时间 (毫秒)


class OrderSideEnum(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderTypeEnum(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    POST_ONLY = "POST_ONLY"


class OrderStatusEnum(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass
class OrderRequest:
    """
    标准化下单请求
    由策略引擎生成，传递给 ExchangeInterface.place_orders()
    """
    symbol: str
    side: OrderSideEnum
    order_type: OrderTypeEnum
    price: float
    size: float                     # 以 base asset 计量 (如 BTC 的数量)
    post_only: bool = True          # 强制 Maker 单
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    zone_name: str = ""             # 来自哪个防线层级 (用于追踪)

    # 以 quote asset (USDC) 计量的名义金额 (由策略层计算，交易所层可验证)
    notional_usdc: float = 0.0


@dataclass
class OrderResult:
    """下单结果"""
    success: bool
    order_id: Optional[str] = None
    error_msg: str = ""
    raw_data: Any = None


@dataclass
class PositionInfo:
    """持仓信息"""
    symbol: str
    side: str               # "LONG" / "SHORT"
    size: float             # 绝对值
    entry_price: float
    unrealized_pnl: float
    liquidation_price: float = 0.0


@dataclass
class BalanceInfo:
    """余额信息"""
    currency: str
    available: float
    total: float
    locked: float = 0.0


@dataclass
class TickerInfo:
    """实时行情"""
    symbol: str
    last_price: float
    mark_price: float
    index_price: float
    best_bid: float = 0.0
    best_ask: float = 0.0
    funding_rate: float = 0.0
    volume_24h: float = 0.0


# ============================================================
#  二、交易所接口 (Exchange Interface)
# ============================================================

class ExchangeInterface(ABC):
    """
    交易所抽象接口
    
    所有与交易所交互的操作在此定义。
    具体实现可以是 01 DEX、Binance、模拟器等。
    """

    @abstractmethod
    async def connect(self) -> bool:
        """建立连接 (含 Session 初始化)"""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """安全断开连接"""
        ...

    @abstractmethod
    async def get_ticker(self, symbol: str) -> TickerInfo:
        """获取指定交易对的实时行情"""
        ...

    @abstractmethod
    async def get_balances(self) -> List[BalanceInfo]:
        """获取账户余额列表"""
        ...

    @abstractmethod
    async def get_positions(self) -> List[PositionInfo]:
        """获取所有持仓"""
        ...

    @abstractmethod
    async def place_orders(self, orders: List[OrderRequest]) -> List[OrderResult]:
        """
        批量下单
        
        Args:
            orders: 订单请求列表
        Returns:
            与 orders 一一对应的执行结果列表
        """
        ...

    @abstractmethod
    async def cancel_orders(self, symbol: str, order_ids: List[str]) -> int:
        """
        批量撤单
        
        Returns:
            成功撤销的订单数量
        """
        ...

    @abstractmethod
    async def cancel_all_orders(self, symbol: str) -> int:
        """撤销指定交易对的全部挂单"""
        ...

    @abstractmethod
    async def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        """获取当前挂单列表"""
        ...


# ============================================================
#  三、数据源接口 (Data Feed Interface)
# ============================================================

class DataFeedInterface(ABC):
    """
    外部数据源抽象接口
    
    负责获取历史 K 线等行情数据。
    由于 01 DEX 无历史 K 线 API，默认实现将使用币安公共接口。
    """

    @abstractmethod
    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 100
    ) -> List[OHLCV]:
        """
        拉取历史 K 线数据
        
        Args:
            symbol: 系统内部符号 (如 "BTC-PERP")
            timeframe: K 线周期 (如 "1h", "15m", "1d")
            limit: 拉取根数
        Returns:
            按时间升序排列的 OHLCV 列表
        """
        ...
