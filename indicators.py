"""
deep_anchor/core/indicators.py
==============================
深海锚 - 技术指标计算器

纯数学实现，零外部依赖 (不依赖 pandas/talib)。
适合部署在轻量级 VPS 上，避免安装大型数值库。

包含:
- EMA (Exponential Moving Average): 指数移动平均线
- ATR (Average True Range): 平均真实波幅

所有函数均接收 OHLCV 列表，返回浮点数。
"""

import logging
from typing import List

from .interfaces import OHLCV


logger = logging.getLogger("DeepAnchor.Indicators")


def calculate_ema(candles: List[OHLCV], period: int) -> float:
    """
    计算 EMA (指数移动平均线)

    公式:
        EMA_today = Close_today × K + EMA_yesterday × (1 - K)
        其中 K = 2 / (period + 1)

    初始值:
        前 `period` 根 K 线的收盘价简单平均值 (SMA) 作为 EMA 的种子值。

    Args:
        candles: K 线数据列表 (按时间升序)
        period: EMA 周期

    Returns:
        最新一根 K 线对应的 EMA 值

    Raises:
        ValueError: 当 K 线数量不足时
    """
    if len(candles) < period:
        raise ValueError(
            f"EMA 计算需要至少 {period} 根 K 线，当前仅有 {len(candles)} 根"
        )

    closes = [c.close for c in candles]
    multiplier = 2.0 / (period + 1)

    # Step 1: 用前 period 根收盘价的 SMA 作为 EMA 的初始种子
    sma_seed = sum(closes[:period]) / period
    ema = sma_seed

    # Step 2: 从第 period 根开始，逐根迭代计算 EMA
    for i in range(period, len(closes)):
        ema = closes[i] * multiplier + ema * (1 - multiplier)

    logger.debug(f"EMA({period}) = {ema:.2f} | 基于 {len(closes)} 根 K 线")
    return ema


def calculate_atr(candles: List[OHLCV], period: int) -> float:
    """
    计算 ATR (平均真实波幅)

    True Range (TR) 定义:
        TR = max(
            High - Low,                       # 当根振幅
            abs(High - Previous_Close),        # 高点与前收的差
            abs(Low  - Previous_Close)         # 低点与前收的差
        )

    ATR 就是 TR 的指数移动平均 (EMA 平滑):
        ATR_today = (ATR_yesterday × (period - 1) + TR_today) / period
        (这是 Wilder 平滑法，等效于 EMA multiplier = 1/period)

    Args:
        candles: K 线数据列表 (按时间升序)
        period: ATR 周期

    Returns:
        最新一根 K 线对应的 ATR 值

    Raises:
        ValueError: 当 K 线数量不足时
    """
    # ATR 计算 TR 时需要"前一根收盘价"，因此至少需要 period + 1 根
    if len(candles) < period + 1:
        raise ValueError(
            f"ATR 计算需要至少 {period + 1} 根 K 线，当前仅有 {len(candles)} 根"
        )

    # Step 1: 计算所有 True Range
    true_ranges: List[float] = []
    for i in range(1, len(candles)):
        high = candles[i].high
        low = candles[i].low
        prev_close = candles[i - 1].close

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        true_ranges.append(tr)

    # Step 2: 用前 period 个 TR 的 SMA 作为 ATR 的初始种子
    if len(true_ranges) < period:
        raise ValueError(
            f"有效 TR 数量不足: 需要 {period}，仅有 {len(true_ranges)}"
        )

    atr = sum(true_ranges[:period]) / period

    # Step 3: Wilder 平滑法迭代
    for i in range(period, len(true_ranges)):
        atr = (atr * (period - 1) + true_ranges[i]) / period

    logger.debug(f"ATR({period}) = {atr:.2f} | 基于 {len(true_ranges)} 个 TR")
    return atr


def calculate_indicators(candles: List[OHLCV], ema_period: int = 20, atr_period: int = 14) -> dict:
    """
    一次性计算所有需要的指标 (便捷封装)

    Returns:
        {
            "ema": float,           # 当前 EMA 值
            "atr": float,           # 当前 ATR 值
            "last_close": float,    # 最新收盘价
            "candle_count": int,    # 使用的 K 线总数
        }
    """
    ema_val = calculate_ema(candles, ema_period)
    atr_val = calculate_atr(candles, atr_period)

    return {
        "ema": ema_val,
        "atr": atr_val,
        "last_close": candles[-1].close,
        "candle_count": len(candles),
    }
