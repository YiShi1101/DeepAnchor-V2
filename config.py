"""
deep_anchor/config.py
=====================
深海锚 (Deep Anchor) - 全局配置中心

所有策略参数、交易所连接信息、数据源地址均在此集中管理。
采用 dataclass 确保类型安全，便于未来接入 YAML/JSON 热加载。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ============================================================
#  一、挂单层级定义 (Order Layer / Zone)
# ============================================================
@dataclass
class OrderZone:
    """单个挂单层级的参数包"""
    name: str               # 层级名称 (用于日志和报表)
    atr_multiplier: float   # ATR 乘数 N — 距 EMA 的倍数
    capital_ratio: float    # 该层级占单边总资金的比例 (0.0 ~ 1.0)


# ============================================================
#  二、模块一 - 宏观引擎配置 (Macro Engine Config)
# ============================================================
@dataclass
class MacroEngineConfig:
    """模块一：宏观测算与阶梯预埋 的全部参数"""

    # --- 技术指标参数 ---
    ema_period: int = 20            # EMA 周期 (基于 1H K线)
    atr_period: int = 14            # ATR 周期 (基于 1H K线)
    kline_timeframe: str = "1h"     # K 线周期
    kline_limit: int = 100          # 每次拉取的 K 线根数 (需 >= max(ema_period, atr_period) + 缓冲)

    # --- 资金配置 ---
    total_capital_per_side: float = 5_000.0    # 单边总额 (USDC)

    # --- 三道防线 ---
    zones: List[OrderZone] = field(default_factory=lambda: [
        OrderZone(name="🌊 浅水区 (Shallow)", atr_multiplier=2.5, capital_ratio=0.20),
        OrderZone(name="🎯 甜点区 (Sweet Spot)", atr_multiplier=4.0, capital_ratio=0.30),
        OrderZone(name="🐋 深海区 (Deep Sea)", atr_multiplier=7.0, capital_ratio=0.50),
    ])

    # --- 挂单刷新间隔 ---
    refresh_interval_sec: int = 300     # 每 5 分钟重新计算一次 (秒)
    post_only: bool = True              # 强制 Maker Only


# ============================================================
#  三、模块二 ~ 五占位配置
# ============================================================
@dataclass
class MicroRadarConfig:
    """模块二：微观雷达 (Phase 2 实现)"""
    enabled: bool = False
    poll_interval_sec: float = 0.5

@dataclass
class ExitManagerConfig:
    """模块三：平仓管家 (Phase 2 实现)"""
    enabled: bool = False
    check_interval_sec: float = 1.0
    # 止盈止损百分比 (相对持仓成本)
    take_profit_pct: float = 0.008      # 0.8%
    stop_loss_pct: float = 0.005        # 0.5%
    max_hold_time_sec: int = 180        # 最大持仓时间

@dataclass
class StorageConfig:
    """模块四：极简运维 (Phase 2 实现)"""
    enabled: bool = False
    db_path: str = "data/deep_anchor.db"

@dataclass
class PerformanceConfig:
    """模块五：效能监控 (Phase 2 实现)"""
    enabled: bool = False
    shadow_mode: bool = True    # 影子交易模式 (只记录不执行)


# ============================================================
#  四、交易所连接配置
# ============================================================
@dataclass
class ExchangeConnectionConfig:
    """01 DEX 交易所连接参数"""
    exchange_id: str = "zeroone"
    base_url: str = "https://zo-mainnet.n1.xyz"
    account_id: int = 0
    # api_secret 通过环境变量 ZERO1_PRIVATE_KEY 注入，不写入配置文件


# ============================================================
#  五、数据源配置
# ============================================================
@dataclass
class DataFeedConfig:
    """
    外部数据源配置
    因 01 DEX 无历史 K 线接口，使用币安公共合约 API 作为替代数据源。
    """
    binance_futures_base: str = "https://fapi.binance.com"
    kline_endpoint: str = "/fapi/v1/klines"
    request_timeout_sec: int = 10

    # 符号映射: 系统内部符号 -> 币安合约符号
    symbol_map: Dict[str, str] = field(default_factory=lambda: {
        "BTC-PERP": "BTCUSDT",
        "ETH-PERP": "ETHUSDT",
        "SOL-PERP": "SOLUSDT",
    })


# ============================================================
#  六、Telegram 通知配置
# ============================================================
@dataclass
class TelegramConfig:
    """Telegram 机器人通知"""
    enabled: bool = False
    bot_token: str = ""         # 通过环境变量 TELEGRAM_BOT_TOKEN 注入
    chat_id: str = ""
    report_interval_hours: int = 4


# ============================================================
#  七、系统总配置 (聚合入口)
# ============================================================
@dataclass
class DeepAnchorConfig:
    """
    深海锚系统总配置
    所有子模块配置在此汇聚，运行时只需实例化此类即可。
    """
    # 交易标的
    symbol: str = "BTC-PERP"

    # 运行模式
    dry_run: bool = True        # True = 只计算不下单 (安全测试模式)

    # 各模块配置
    exchange: ExchangeConnectionConfig = field(default_factory=ExchangeConnectionConfig)
    data_feed: DataFeedConfig = field(default_factory=DataFeedConfig)
    macro: MacroEngineConfig = field(default_factory=MacroEngineConfig)
    micro: MicroRadarConfig = field(default_factory=MicroRadarConfig)
    exit_mgr: ExitManagerConfig = field(default_factory=ExitManagerConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
