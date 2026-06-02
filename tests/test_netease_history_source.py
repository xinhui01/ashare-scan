"""网易历史源对"不被支持的代码"（如北交所 920xxx）的健壮性。

回归：920225 是北交所新代码段，网易 chddata.html 不按 0/1 前缀提供，返回的
不是标准股票 CSV。解析后 df 非空但缺少 date/close 列，旧代码在
``dropna(subset=["date", "close"])`` 处抛出裸 ``KeyError('date')``，一路冒泡成
``获取股票 920225 历史数据失败: 'date'``，并让该票彻底拿不到数据。
修复后应优雅降级（描述性 RuntimeError，会被上层切换到备用源），绝不能是 KeyError。
"""
import pandas as pd
import pytest

from src.sources import netease


class _FakeResp:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code


def test_netease_unsupported_code_degrades_without_keyerror(monkeypatch):
    # 模拟一个非空、但列里没有 日期/收盘价 的响应（错误页/不支持的代码常见）
    junk_csv = "foo,bar\n1,2\n".encode("gbk")

    monkeypatch.setattr(netease, "on_cooldown", lambda host: False)
    monkeypatch.setattr(netease, "throttle", lambda: None)
    monkeypatch.setattr(netease, "mark_failed", lambda host: None)
    monkeypatch.setattr(netease, "mark_ok", lambda host: None)
    monkeypatch.setattr("requests.get", lambda *a, **k: _FakeResp(junk_csv))

    try:
        out = netease.fetch_hist_frame("920225", "20260401", "20260601")
    except KeyError as exc:  # 这正是要修掉的崩溃
        pytest.fail(f"netease 对不支持的代码抛出了裸 KeyError({exc!r})，未优雅降级")
    except RuntimeError as exc:
        # 优雅降级为带描述的错误：上层 per-provider try 会捕获并切到备用源
        assert "date" in str(exc).lower() or "missing" in str(exc).lower()
        return

    # 或者直接返回空 DataFrame 也可接受
    assert isinstance(out, pd.DataFrame)
    assert out.empty


def test_netease_valid_response_still_parses(monkeypatch):
    # 正常的网易 CSV（含 日期/收盘价 等中文列）应照常解析出统一 schema
    valid_csv = (
        "日期,股票代码,名称,收盘价,最高价,最低价,开盘价,前收盘,涨跌额,涨跌幅,换手率,成交量,成交金额\n"
        "2026-04-21,'600000,测试,10.50,10.80,9.90,10.00,10.00,0.50,5.00,1.23,123456,1296288\n"
    ).encode("gbk")

    monkeypatch.setattr(netease, "on_cooldown", lambda host: False)
    monkeypatch.setattr(netease, "throttle", lambda: None)
    monkeypatch.setattr(netease, "mark_failed", lambda host: None)
    monkeypatch.setattr(netease, "mark_ok", lambda host: None)
    monkeypatch.setattr("requests.get", lambda *a, **k: _FakeResp(valid_csv))

    out = netease.fetch_hist_frame("600000", "20260401", "20260601")

    assert isinstance(out, pd.DataFrame)
    assert not out.empty
    assert "date" in out.columns and "close" in out.columns
    assert out.iloc[0]["date"] == "2026-04-21"
    assert out.iloc[0]["close"] == 10.50
