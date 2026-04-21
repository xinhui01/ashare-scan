"""交易日历三态判定测试。"""
from datetime import datetime, time as dtime
import unittest

from src.utils.trade_calendar import (
    SyncTarget,
    TradePhase,
    resolve_sync_target_trade_date,
)

# 固定注入的交易日集合，覆盖五一节前后（2026-04-28 / 04-29 / 04-30 交易；
# 05-01 ~ 05-05 休市；05-06 恢复交易）以及一个普通工作周。
_TEST_CAL = [
    "2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24",
    "2026-04-27", "2026-04-28", "2026-04-29", "2026-04-30",
    "2026-05-06", "2026-05-07", "2026-05-08",
]


class TestResolveSyncTargetTradeDate(unittest.TestCase):
    def test_closed_after_15(self):
        # 交易日 15:30 → CLOSED，目标日 = 当天
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 22, 15, 30, 0),
            calendar=_TEST_CAL,
        )
        self.assertEqual(target.phase, TradePhase.CLOSED)
        self.assertEqual(target.target_date, "2026-04-22")
        self.assertTrue(target.allows_history_write())

    def test_intraday_before_open(self):
        # 交易日 09:00 → INTRADAY，目标日 = 上一交易日
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 22, 9, 0, 0),
            calendar=_TEST_CAL,
        )
        self.assertEqual(target.phase, TradePhase.INTRADAY)
        self.assertEqual(target.target_date, "2026-04-21")
        self.assertFalse(target.allows_history_write())

    def test_intraday_midday(self):
        # 交易日 14:00 → INTRADAY，禁止写 history
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 22, 14, 0, 0),
            calendar=_TEST_CAL,
        )
        self.assertEqual(target.phase, TradePhase.INTRADAY)
        self.assertFalse(target.allows_history_write())

    def test_non_trading_saturday(self):
        # 周六 → NON_TRADING，目标日 = 上一交易日（周五）
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 25, 12, 0, 0),
            calendar=_TEST_CAL,
        )
        self.assertEqual(target.phase, TradePhase.NON_TRADING)
        self.assertEqual(target.target_date, "2026-04-24")

    def test_non_trading_holiday_tail(self):
        # 2026-05-05（劳动节最后一天，非交易日）→ 目标日 = 节前 2026-04-30
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 5, 5, 10, 0, 0),
            calendar=_TEST_CAL,
        )
        self.assertEqual(target.phase, TradePhase.NON_TRADING)
        self.assertEqual(target.target_date, "2026-04-30")

    def test_non_trading_holiday_mid(self):
        # 2026-05-03（劳动节中间）→ 目标日 = 节前 2026-04-30
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 5, 3, 10, 0, 0),
            calendar=_TEST_CAL,
        )
        self.assertEqual(target.phase, TradePhase.NON_TRADING)
        self.assertEqual(target.target_date, "2026-04-30")

    def test_closed_exactly_at_15(self):
        # 15:00:00 整 → 已达收盘时间，视为 CLOSED
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 22, 15, 0, 0),
            calendar=_TEST_CAL,
        )
        self.assertEqual(target.phase, TradePhase.CLOSED)

    def test_custom_close_time(self):
        # 允许注入自定义收盘时间（例如测试期货品种 23:00）
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 22, 22, 0, 0),
            calendar=_TEST_CAL,
            close_time=dtime(23, 0, 0),
        )
        self.assertEqual(target.phase, TradePhase.INTRADAY)

    def test_sync_target_dataclass(self):
        target = SyncTarget("2026-04-22", TradePhase.CLOSED)
        date, phase = target.as_tuple()
        self.assertEqual(date, "2026-04-22")
        self.assertEqual(phase, TradePhase.CLOSED)

    def test_weekend_fallback_when_calendar_missing(self):
        """未提供交易日历时退化到周末过滤。周六不应判为 CLOSED。"""
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 25, 15, 30, 0),
            calendar=[],  # 空集合 → 显式"无日历"，走 fallback
        )
        # 空列表也算无日历；fallback 下周六判为 NON_TRADING
        self.assertEqual(target.phase, TradePhase.NON_TRADING)

    def test_degraded_flag_set_when_calendar_missing(self):
        """日历缺失时 SyncTarget.calendar_degraded 应为 True。"""
        import src.utils.trade_calendar as tc
        # 重置单次警告标志，测试之间隔离
        tc._FALLBACK_WARNED = False
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 22, 15, 30, 0),
            calendar=[],
        )
        self.assertTrue(target.calendar_degraded)

    def test_degraded_flag_not_set_with_calendar(self):
        target = resolve_sync_target_trade_date(
            now=datetime(2026, 4, 22, 15, 30, 0),
            calendar=_TEST_CAL,
        )
        self.assertFalse(target.calendar_degraded)

    def test_degraded_warning_logged_once(self):
        """降级警告只 log 一次，避免每次调用 spam。"""
        import src.utils.trade_calendar as tc
        tc._FALLBACK_WARNED = False
        with self.assertLogs(tc.logger, level="WARNING") as captured:
            resolve_sync_target_trade_date(
                now=datetime(2026, 4, 22, 15, 30, 0), calendar=[]
            )
            resolve_sync_target_trade_date(
                now=datetime(2026, 4, 23, 15, 30, 0), calendar=[]
            )
        # 期望只有一次警告
        warnings = [r for r in captured.records if r.levelname == "WARNING"]
        self.assertEqual(len(warnings), 1)


if __name__ == "__main__":
    unittest.main()
