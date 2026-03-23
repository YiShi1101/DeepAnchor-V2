# 🐋 深海锚 (Deep Anchor) - Phase 1

加密货币高频被动做市与接针系统

## 📁 项目结构

```
deep_anchor_phase1/
│
├── run_deep_anchor.py              # 🚀 主入口 (直接运行此文件)
│
└── deep_anchor/                    # 核心包
    ├── __init__.py
    ├── config.py                   # ⚙️ 全局配置中心 (所有参数集中管理)
    ├── main.py                     # 🐋 系统主控 DeepAnchorSystem
    │
    ├── core/                       # 核心策略模块
    │   ├── interfaces.py           # 📐 抽象接口 (ExchangeInterface, DataFeedInterface)
    │   ├── data_feed.py            # 📡 数据管道 (币安K线拉取器)
    │   ├── indicators.py           # 📈 技术指标 (EMA, ATR 纯数学实现)
    │   ├── macro_engine.py         # ✅ 模块一: 宏观测算与阶梯预埋 [已完整实现]
    │   ├── micro_radar.py          # 🔇 模块二: 微观雷达 (Phase 2 占位)
    │   ├── exit_manager.py         # 🔇 模块三: 平仓管家 (Phase 2 占位)
    │   ├── storage.py              # 🔇 模块四: 极简运维 (Phase 2 占位)
    │   └── performance.py          # 🔇 模块五: 效能监控 (Phase 2 占位)
    │
    └── exchange/                   # 交易所适配器
        └── adapter_01dex.py        # 01 DEX 适配器 (实现 ExchangeInterface)
```

## 🛠️ 安装

```bash
# 最小依赖 (测试模式零外部依赖，使用 Python 内置 urllib)
# 无需安装任何包即可运行 test 模式

# 可选: 安装 aiohttp 提升 HTTP 性能
pip install aiohttp

# 实盘额外依赖
pip install protobuf cryptography base58 requests pyyaml
```

## 🚀 运行方式

```bash
# 模式一: 单次测试 (默认) — 拉取K线 → 计算 → 打印挂单方案
python run_deep_anchor.py test

# 模式二: 独立指标验证 — 只验证数据拉取和计算
python run_deep_anchor.py validate

# 模式三: 模拟循环 — 持续运行但不下单
python run_deep_anchor.py dry

# 模式四: 实盘 (需私钥)
export ZERO1_PRIVATE_KEY='your_base58_key'
export ZERO1_ACCOUNT_ID='1334'
python run_deep_anchor.py live
```

## ⚠️ 注意事项

- `test` 和 `validate` 模式需要网络访问币安 API (公共接口，无需密钥)
- `live` 模式需要 01 DEX 私钥和现有适配器代码
- 所有挂单默认 `post_only=True` (纯 Maker)
- Phase 1 只实现了模块一，其余模块为占位符
