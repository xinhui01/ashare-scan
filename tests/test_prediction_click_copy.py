from types import SimpleNamespace

from src.gui.tabs.predict import PredictTab


class _FakeTree:
    def selection(self):
        return ("row1",)

    def item(self, item_id, option=None):
        assert item_id == "row1"
        assert option == "values"
        return ("603767", "中马传动", "汽车制造业")


class _FakeRoot:
    def __init__(self):
        self.cleared = False
        self.payload = ""
        self.updated = False

    def clipboard_clear(self):
        self.cleared = True

    def clipboard_append(self, payload):
        self.payload = payload

    def update_idletasks(self):
        self.updated = True


class _FakeStatusVar:
    def __init__(self):
        self.value = ""

    def set(self, value):
        self.value = value


def test_prediction_candidate_click_copies_stock_name_to_clipboard():
    tab = PredictTab.__new__(PredictTab)
    root = _FakeRoot()
    status = _FakeStatusVar()
    tab.app = SimpleNamespace(root=root, status_var=status)

    tab._on_stock_select(SimpleNamespace(widget=_FakeTree()))

    assert root.cleared is True
    assert root.payload == "中马传动"
    assert root.updated is True
    assert "已复制名称: 中马传动" in status.value
