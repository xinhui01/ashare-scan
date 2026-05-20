"""facade：StockMonitorApp 主类已迁移到 src.gui.app；本文件仅保留向后兼容入口。

外部老调用方（如 main.py、ad-hoc 脚本）通常这样用：
    from stock_gui import StockMonitorApp
本文件转发到 src.gui.app，让旧调用方无需修改。
"""
from src.gui.app import StockMonitorApp

__all__ = ["StockMonitorApp"]
