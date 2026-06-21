from __future__ import annotations

from src.gui.tree_enhancer import TreeviewEnhancer


class _FakeTree:
    def __init__(self) -> None:
        self._full_cell_texts = {("row1", "reasons"): "完整预测依据 / 不应该省略"}

    def cget(self, name):
        if name == "columns":
            return ("code", "reasons")
        return None

    def set(self, row, col_name):
        return "完整预测依据..."


def test_treeview_enhancer_reads_full_cell_text_before_display_text():
    enhancer = TreeviewEnhancer.__new__(TreeviewEnhancer)
    enhancer.tree = _FakeTree()

    assert enhancer._read_cell_text("row1", "#2") == "完整预测依据 / 不应该省略"
