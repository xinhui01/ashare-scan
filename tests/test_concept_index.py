from types import SimpleNamespace

from src.sources import concept_index as ci


class _FakeResponse:
    def __init__(self, text: str):
        self.text = text
        self.status_code = 200

    def raise_for_status(self):
        return None


def test_ths_info_api_is_not_used_as_member_fetcher():
    assert ci._THS_MEMBERS_FN is not getattr(ci.ak, "stock_board_concept_info_ths", None)


def test_ths_members_fallback_parses_paginated_concept_table(monkeypatch):
    page1 = """
    <table>
      <thead><tr><th>序号</th><th>代码</th><th>名称</th></tr></thead>
      <tbody>
        <tr><td>1</td><td>300166</td><td>东方国信</td></tr>
        <tr><td>2</td><td>002579</td><td>中京电子</td></tr>
      </tbody>
    </table>
    <span class="page_info">1/2</span>
    """
    page2 = """
    <table>
      <thead><tr><th>序号</th><th>代码</th><th>名称</th></tr></thead>
      <tbody>
        <tr><td>11</td><td>2273</td><td>水晶光电</td></tr>
      </tbody>
    </table>
    """
    requested = []

    def fake_get(url, headers=None, timeout=None):
        requested.append(url)
        if "/page/2/" in url:
            return _FakeResponse(page2)
        return _FakeResponse(page1)

    monkeypatch.setattr(ci, "_THS_MEMBERS_FN", None)
    monkeypatch.setattr(
        ci,
        "_fetch_ths_concept_name_code_map",
        lambda: {"AI手机": "309120"},
        raising=False,
    )
    monkeypatch.setattr(ci, "_make_ths_headers", lambda referer="": {}, raising=False)
    monkeypatch.setattr(ci, "requests", SimpleNamespace(get=fake_get), raising=False)

    codes = ci._fetch_ths_concept_members("AI手机")

    assert codes == ["300166", "002579", "002273"]
    assert any("/page/2/" in url for url in requested)
