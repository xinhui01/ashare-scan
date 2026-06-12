"""高频进度事件 → UI 刷新的节流器。

背景：缓存更新等后台任务每完成一个元素就回报一次进度（全市场约 4000~5000
只股票）。若每次回报都 `root.after(0, ...)` 推一批 UI 更新，Tk 主线程会被上万
个回调淹没，表现为界面"卡死一下"。

本组件把"该不该真正刷新 UI"的判定收敛成一个纯逻辑单元：按时间间隔采样，
把上万次回报压成每 interval_ms 至多一次，并保证最后一次（is_final）必定推送，
确保界面收尾停在 100% 与最终统计。

时间通过参数注入（now_ms），不读真实时钟，便于单测且与 UIDispatcher /
LogDrainer 的可测风格一致。
"""
from __future__ import annotations

from typing import Optional


class ProgressThrottle:
    """按时间间隔对进度刷新做采样节流。

    Attributes:
        interval_ms: 两次推送之间的最小间隔（毫秒）。
    """

    __slots__ = ("_interval_ms", "_last_emit_ms")

    def __init__(self, interval_ms: float = 120.0) -> None:
        self._interval_ms = float(interval_ms)
        self._last_emit_ms: Optional[float] = None

    def should_emit(self, now_ms: float, *, is_final: bool = False) -> bool:
        """返回这次进度事件是否应该真正刷新 UI。

        - 首次调用：推送（让界面立刻从 0 动起来）。
        - is_final=True：始终推送（保证收尾显示最终值），并重置基准。
        - 否则：距上次推送满 interval_ms 才推送。
        """
        if is_final:
            self._last_emit_ms = now_ms
            return True
        if self._last_emit_ms is None or (now_ms - self._last_emit_ms) >= self._interval_ms:
            self._last_emit_ms = now_ms
            return True
        return False
