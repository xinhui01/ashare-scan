"""news_feed 缓存 ok 标志：两源全失败不得把空结果钉死一整天。"""
import src.services.news_feed_service as news


def _memory_store(monkeypatch):
    store = {}
    monkeypatch.setattr(news, "load_app_config", lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(news, "save_app_config", lambda key, value: store.__setitem__(key, value))
    return store


def test_all_sources_failed_writes_ok_false_and_retries(monkeypatch):
    store = _memory_store(monkeypatch)
    calls = []

    def _no_briefing(td):
        calls.append("briefing")
        return None

    monkeypatch.setattr(news, "_fetch_morning_briefing", _no_briefing)
    monkeypatch.setattr(news, "_fetch_telegrams", lambda td: [])

    out1 = news.fetch_today_news("20260807")
    assert out1["ok"] is False

    # 第二次调用不允许命中失败缓存，必须重新拉取
    news.fetch_today_news("20260807")
    assert calls.count("briefing") == 2
    assert len(store) == 1  # 失败结果仍会落盘记录，但 ok=False 不拦截重试


def test_partial_success_still_cached(monkeypatch):
    _memory_store(monkeypatch)
    calls = []

    def _briefing(td):
        calls.append("briefing")
        return {"title": "早餐", "summary": "内容", "time": "07:30"}

    monkeypatch.setattr(news, "_fetch_morning_briefing", _briefing)
    monkeypatch.setattr(news, "_fetch_telegrams", lambda td: [])

    out1 = news.fetch_today_news("20260807")
    assert out1["ok"] is True

    news.fetch_today_news("20260807")
    assert calls.count("briefing") == 1  # 第二次命中缓存
