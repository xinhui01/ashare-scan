"""交易日历 + 同步目标交易日三态判定。

用于历史缓存同步模型。调用方按 `phase` 决定：
- `CLOSED`    : 当天是交易日且已过 15:00 收盘，目标日 = 当天，允许写 history
- `INTRADAY`  : 当天是交易日但未收盘（含盘前、盘中、竞价前），目标日 = 上一交易日，
                **禁止将当日快照写入 history**，只能用于预览
- `NON_TRADING`: 当天非交易日（周末/法定节假日），目标日 = 最近一个已收盘交易日

参考：`docs/claude-code-history-cache-sync-brief.md` 第五章第一节。
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from enum import Enum
from typing import Callable, Iterable, Optional, Set, Tuple

logger = logging.getLogger(__name__)

MARKET_CLOSE_TIME = dtime(15, 0, 0)

# 首次降级到周末 fallback 时 log 一次，避免 spam
_FALLBACK_WARNED = False
_FALLBACK_WARN_LOCK = threading.Lock()


class TradePhase(str, Enum):
    CLOSED = "closed"
    INTRADAY = "intraday"
    NON_TRADING = "non_trading"


@dataclass(frozen=True)
class SyncTarget:
    target_date: str
    phase: TradePhase
    # True 表示本次判定没有可靠的交易日历数据，降级到了"只过滤周末"的兜底。
    # 调用方可据此在日志里额外警示或选择不信任 NON_TRADING 判定。
    calendar_degraded: bool = False

    def allows_history_write(self) -> bool:
        """只有 CLOSED / NON_TRADING 态允许把本轮数据写入 history。"""
        return self.phase is not TradePhase.INTRADAY

    def as_tuple(self) -> Tuple[str, TradePhase]:
        return self.target_date, self.phase


_CAL_LOCK = threading.Lock()
_CAL_CACHE: Optional[Set[date]] = None
_CAL_CACHE_EXPIRES_AT: float = 0.0
_CAL_TTL_SECONDS = 24 * 3600


def _load_trade_calendar_from_akshare() -> Set[date]:
    """优先使用 akshare 的 tool_trade_date_hist_sina。失败返回空集合。"""
    try:
        import akshare as ak  # type: ignore

        df = ak.tool_trade_date_hist_sina()
        if df is None or getattr(df, "empty", True):
            return set()
        col = "trade_date" if "trade_date" in df.columns else df.columns[0]
        out: Set[date] = set()
        for value in df[col].tolist():
            parsed = _coerce_to_date(value)
            if parsed is not None:
                out.add(parsed)
        return out
    except Exception:
        return set()


def _coerce_to_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip().replace("/", "-").replace(".", "-")
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _weekend_fallback(d: date) -> bool:
    """akshare 不可用时的兜底判定：只过滤周末，节假日无能为力。"""
    return d.weekday() < 5


def _get_trade_calendar(
    loader: Optional[Callable[[], Set[date]]] = None,
    force_refresh: bool = False,
) -> Optional[Set[date]]:
    """返回交易日集合。加载失败返回 None，调用方自行降级到周末判定。"""
    global _CAL_CACHE, _CAL_CACHE_EXPIRES_AT
    now_ts = datetime.now().timestamp()
    with _CAL_LOCK:
        if (
            not force_refresh
            and _CAL_CACHE is not None
            and now_ts < _CAL_CACHE_EXPIRES_AT
        ):
            return _CAL_CACHE
        fetch = loader or _load_trade_calendar_from_akshare
        fetched = fetch()
        if fetched:
            _CAL_CACHE = fetched
            _CAL_CACHE_EXPIRES_AT = now_ts + _CAL_TTL_SECONDS
            return _CAL_CACHE
    return None


def _is_trading_day(d: date, calendar: Optional[Set[date]]) -> bool:
    # 非空日历：严格按集合判定（能识别节假日）
    # 空集合 / None：降级到周末过滤（外部数据源失败时的兜底）
    if calendar:
        return d in calendar
    return _weekend_fallback(d)


def _previous_trading_day(d: date, calendar: Optional[Set[date]]) -> date:
    """向前回溯找最近一个交易日；最多回溯 30 天作为保护。"""
    for i in range(1, 31):
        candidate = d - timedelta(days=i)
        if _is_trading_day(candidate, calendar):
            return candidate
    return d - timedelta(days=1)


def resolve_sync_target_trade_date(
    now: Optional[datetime] = None,
    *,
    calendar: Optional[Iterable] = None,
    close_time: dtime = MARKET_CLOSE_TIME,
) -> SyncTarget:
    """计算本轮同步的目标交易日与阶段。

    参数 `calendar` 允许测试注入固定的交易日集合；传 None 时走默认加载器
    （akshare + 24h 缓存 + 周末 fallback）。
    """
    now = now or datetime.now()
    today = now.date()

    if calendar is not None:
        cal_set: Optional[Set[date]] = {c for c in (_coerce_to_date(x) for x in calendar) if c is not None}
    else:
        cal_set = _get_trade_calendar()

    degraded = not cal_set  # 空集合或 None → 走周末 fallback，节假日判断不可靠
    if degraded:
        _warn_calendar_degraded_once()

    if not _is_trading_day(today, cal_set):
        prev = _previous_trading_day(today, cal_set)
        return SyncTarget(prev.strftime("%Y-%m-%d"), TradePhase.NON_TRADING, degraded)

    if now.time() < close_time:
        prev = _previous_trading_day(today, cal_set)
        return SyncTarget(prev.strftime("%Y-%m-%d"), TradePhase.INTRADAY, degraded)

    return SyncTarget(today.strftime("%Y-%m-%d"), TradePhase.CLOSED, degraded)


def _warn_calendar_degraded_once() -> None:
    """首次检测到日历缺失时 log 一次警告，避免每次调用都 spam。"""
    global _FALLBACK_WARNED
    if _FALLBACK_WARNED:
        return
    with _FALLBACK_WARN_LOCK:
        if _FALLBACK_WARNED:
            return
        _FALLBACK_WARNED = True
    logger.warning(
        "交易日历不可用，降级到周末过滤；法定节假日判定可能不准确。"
        "建议检查 akshare.tool_trade_date_hist_sina 是否可达。"
    )


def invalidate_calendar_cache() -> None:
    """测试或运行时强制刷新交易日历缓存。"""
    global _CAL_CACHE, _CAL_CACHE_EXPIRES_AT
    with _CAL_LOCK:
        _CAL_CACHE = None
        _CAL_CACHE_EXPIRES_AT = 0.0
