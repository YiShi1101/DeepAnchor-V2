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
    total_capital_per_side: float = 10_000.0    # 单边总额 (USDC)

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
    """
    模块二：微观雷达与物理熔断器 (Micro Radar)

    监听币安爆仓流 + 聚合成交流，检测极端微观异动。
    """
    enabled: bool = True

    # --- 监听目标 ---
    # 币安永续合约 WebSocket 基地址
    ws_base_url: str = "wss://fstream.binance.com"
    # 监听的交易对 (小写，用于 aggTrade 流)
    watch_symbol: str = "btcusdt"

    # --- 滑动窗口参数 ---
    window_seconds: int = 10               # 滑动窗口长度 (秒)

    # --- 熔断阈值 ---
    liquidation_threshold_usd: float = 5_000_000.0   # 窗口内累计爆仓 > 此值触发 (美元)
    tps_spike_ratio: float = 5.0                      # 瞬时 TPS / 基线 TPS > 此倍数触发
    tps_baseline_window_sec: int = 60                 # 计算基线 TPS 的更长窗口 (秒)

    # --- 熔断冷却 ---
    cooldown_seconds: int = 30             # 熔断后冷却期，期间不重复触发

    # --- 连接健壮性 ---
    reconnect_delay_sec: float = 3.0       # 断线后重连等待 (秒)
    max_reconnect_attempts: int = 50       # 最大连续重连次数
    ping_interval_sec: float = 20.0        # 心跳间隔 (秒)

@dataclass
class ExitManagerConfig:
    """
    模块三：状态维持与平仓管家 (Exit Manager)

    管理活跃挂单的生命周期 (TTL 超时撤单) 和
    已成交持仓的止盈止损 (动态 TP/SL + 超时平仓)。
    """
    enabled: bool = True

    # --- 循环频率 ---
    check_interval_sec: float = 0.5        # 主循环检查间隔 (秒)

    # --- 挂单保鲜期 (Order TTL) ---
    order_ttl_sec: float = 330.0            # 挂单最大存活时间 (秒)，需 > macro 刷新周期

    # --- 止盈止损 (相对持仓成本的百分比) ---
    take_profit_pct: float = 0.015          # 1.5% 止盈
    stop_loss_pct: float = 0.02            # 2% 止损

    # --- 持仓超时 ---
    max_hold_time_sec: float = 300.0       # 最大持仓时间 (秒)，超时强制平仓

    # --- 平仓订单参数 ---
    close_order_type: str = "IOC"          # 平仓使用 IOC (Immediate-Or-Cancel)
    slippage_tolerance_pct: float = 0.003  # 0.3% 滑点容忍度 (限价 = mark ± 此比例)

@dataclass
class StorageConfig:
    """
    模块四：极简运维与状态持久化 (Lean Storage)

    SQLite 异步写入 + 日志轮转，严格控制磁盘占用。
    面向 15GB 小盘 VPS 设计。
    """
    enabled: bool = True

    # --- SQLite ---
    db_path: str = "data/deep_anchor.db"    # 数据库文件路径 (自动创建目录)
    db_busy_timeout_ms: int = 5000          # 数据库忙等待超时 (毫秒)

    # --- 日志文件 ---
    log_dir: str = "logs"                   # 日志目录
    log_filename: str = "deep_anchor.log"   # 日志文件名
    log_max_days: int = 7                   # 最多保留天数 (旧文件物理删除)
    log_level: str = "INFO"                 # 文件日志级别

    # --- 数据降维 ---
    retention_days: int = 30                # trade_history 保留天数
    cleanup_interval_hours: int = 6         # 降维清理周期 (小时)

@dataclass
class PerformanceConfig:
    """
    模块五：效能监控与参数调试 (Performance & Tuning)

    API 延迟追踪 + 影子撮合引擎 + 调参报告。
    影子引擎拥有独立的入场网格 (shadow_zones) 和止盈止损参数，
    可与实盘使用完全不同的策略参数进行对比测试。
    """
    enabled: bool = True

    # --- 延迟监控 ---
    latency_warn_ms: float = 500.0          # 延迟超过此值记录 WARNING (毫秒)
    latency_history_size: int = 1000        # 保留最近 N 条延迟记录

    # --- 影子交易 (独立参数) ---
    shadow_mode: bool = True                # True = 只模拟不执行
    shadow_take_profit_pct: float = 0.015    # 影子止盈 3% (实盘 5%)
    shadow_stop_loss_pct: float = 0.02      # 影子止损 1% (实盘 2%)
    shadow_capital_per_side: float = 10_000.0   # 影子单边资金 (虚拟)

    # 影子独立入场网格 (更激进的参数，用于对比测试)
    shadow_zones: List[OrderZone] = field(default_factory=lambda: [
        OrderZone(name="🔵 影-浅水 (S-Shallow)", atr_multiplier=1.5, capital_ratio=0.20),
        OrderZone(name="🟣 影-甜点 (S-Sweet)",   atr_multiplier=3.0, capital_ratio=0.30),
        OrderZone(name="⚫ 影-深海 (S-Deep)",     atr_multiplier=5.0, capital_ratio=0.50),
    ])

    # --- 报告 ---
    report_interval_sec: int = 3600         # 自动出报告周期 (秒)


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
