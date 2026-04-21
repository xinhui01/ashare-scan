from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
from data_source_models import HistoryRequestPlan


@dataclass(frozen=True)
class FilterSettings:
    trend_days: int = 5
    ma_period: int = 5
    limit_up_lookback_days: int = 5
    volume_lookback_days: int = 5
    volume_expand_enabled: bool = True
    volume_expand_factor: float = 2.0
    require_limit_up_within_days: bool = False
    # 涨停后承接强势形态（T 日涨停 → T+1 回落但抛压弱 → 后续不破位）
    strong_ft_enabled: bool = False
    strong_ft_max_pullback_pct: float = 3.0    # T+1 最大回撤（以 T 收盘价为基准，%）
    strong_ft_max_volume_ratio: float = 0.7    # T+1 成交量 / T 成交量的上限，越小越缩量
    strong_ft_min_hold_days: int = 1           # T+1 之后至少站稳几天（0 表示允许 T+1 就是最新一天）

    def to_signature(self) -> Dict[str, Any]:
        return {
            "trend_days": int(self.trend_days),
            "ma_period": int(self.ma_period),
            "limit_up_lookback_days": int(self.limit_up_lookback_days),
            "volume_lookback_days": int(self.volume_lookback_days),
            "volume_expand_enabled": bool(self.volume_expand_enabled),
            "volume_expand_factor": float(self.volume_expand_factor),
            "require_limit_up_within_days": bool(self.require_limit_up_within_days),
            "strong_ft_enabled": bool(self.strong_ft_enabled),
            "strong_ft_max_pullback_pct": float(self.strong_ft_max_pullback_pct),
            "strong_ft_max_volume_ratio": float(self.strong_ft_max_volume_ratio),
            "strong_ft_min_hold_days": int(self.strong_ft_min_hold_days),
        }


@dataclass(frozen=True)
class ScanRequest:
    filter_settings: FilterSettings
    max_stocks: int = 0
    scan_workers: int = 3
    history_source: str = "auto"
    allowed_boards: tuple[str, ...] = ()
    refresh_universe: bool = False
    ignore_result_snapshot: bool = False

    def to_signature(self) -> Dict[str, Any]:
        signature = self.filter_settings.to_signature()
        signature.update(
            {
                "allowed_boards": sorted(
                    {str(board).strip() for board in self.allowed_boards if str(board).strip()}
                ),
                "history_source": str(self.history_source or "auto").strip().lower() or "auto",
                "max_stocks": int(self.max_stocks),
            }
        )
        return signature
