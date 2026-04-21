from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from scan_models import FilterSettings


@dataclass(frozen=True)
class HistoryAnalysisConfig:
    trend_days: int
    ma_period: int
    limit_up_lookback_days: int
    volume_lookback_days: int
    volume_expand_enabled: bool
    volume_expand_factor: float
    # 承接强势形态参数（配合 analyze_limit_up_followthrough）
    strong_ft_max_pullback_pct: float = 3.0
    strong_ft_max_volume_ratio: float = 0.7
    strong_ft_min_hold_days: int = 1

    @classmethod
    def from_filter_settings(
        cls,
        settings: FilterSettings,
        *,
        trend_days: Optional[int] = None,
        ma_period: Optional[int] = None,
        limit_up_lookback_days: Optional[int] = None,
        volume_lookback_days: Optional[int] = None,
        volume_expand_enabled: Optional[bool] = None,
        volume_expand_factor: Optional[float] = None,
        strong_ft_max_pullback_pct: Optional[float] = None,
        strong_ft_max_volume_ratio: Optional[float] = None,
        strong_ft_min_hold_days: Optional[int] = None,
    ) -> "HistoryAnalysisConfig":
        return cls(
            trend_days=max(1, int(settings.trend_days if trend_days is None else trend_days)),
            ma_period=max(1, int(settings.ma_period if ma_period is None else ma_period)),
            limit_up_lookback_days=max(
                1,
                int(
                    settings.limit_up_lookback_days
                    if limit_up_lookback_days is None
                    else limit_up_lookback_days
                ),
            ),
            volume_lookback_days=max(
                1,
                int(
                    settings.volume_lookback_days
                    if volume_lookback_days is None
                    else volume_lookback_days
                ),
            ),
            volume_expand_enabled=bool(
                settings.volume_expand_enabled
                if volume_expand_enabled is None
                else volume_expand_enabled
            ),
            volume_expand_factor=max(
                1.0,
                float(
                    settings.volume_expand_factor
                    if volume_expand_factor is None
                    else volume_expand_factor
                ),
            ),
            strong_ft_max_pullback_pct=max(
                0.0,
                float(
                    settings.strong_ft_max_pullback_pct
                    if strong_ft_max_pullback_pct is None
                    else strong_ft_max_pullback_pct
                ),
            ),
            strong_ft_max_volume_ratio=max(
                0.0,
                float(
                    settings.strong_ft_max_volume_ratio
                    if strong_ft_max_volume_ratio is None
                    else strong_ft_max_volume_ratio
                ),
            ),
            strong_ft_min_hold_days=max(
                0,
                int(
                    settings.strong_ft_min_hold_days
                    if strong_ft_min_hold_days is None
                    else strong_ft_min_hold_days
                ),
            ),
        )
