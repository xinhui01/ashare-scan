"""按 tab 组织的 GUI 模块。

每个 tab 类持有 app 引用（self.app），own 自己的 widget 与 tab 私有状态；
跨 tab 引用走显式 self.app.xxx。详见 docs/superpowers/specs/2026-05-20-stock-gui-modularization-design.md。
"""
