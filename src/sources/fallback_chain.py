"""顺序兜底链的统一执行骨架。

项目里的串联兜底链（竞价快照、全市场 spot、指数涨跌幅、个股资金流）共享
同一个形态：按可靠度排好序的源逐个试，抛异常或结果无效就换下一层，第一个
有效结果即返回。本模块只封装这层骨架（尝试/校验/失败日志/命中源标记）；
各源的请求细节、重试策略留在各自 step 里。有损降级、日期正确性守卫、
"返空即停"这类业务语义不属于这里——涨停池/历史 K 线链因此不套用。
"""
from __future__ import annotations

from typing import Any, Callable, Iterable, NamedTuple, Optional, Tuple


class ChainResult(NamedTuple):
    value: Any                            # 首个有效结果；全部落空为 None
    source: str                           # 命中层的名字；全部落空为 ""
    last_error: Optional[BaseException]   # 链上最后一个异常，供终态提示用


def run_fallback_chain(
    steps: Iterable[Tuple[str, Callable[[], Any]]],
    *,
    is_valid: Optional[Callable[[Any], bool]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    chain_name: str = "",
) -> ChainResult:
    """依序执行 (源名, 无参函数) 列表，返回首个有效结果。

    - step 抛异常 → 记一条日志换下一层；
    - 结果未通过 is_valid（缺省=非 None 即有效）→ 静默换下一层；
    - 命中即返回，不再尝试后续层。
    """
    prefix = f"{chain_name} " if chain_name else ""
    last_error: Optional[BaseException] = None
    for name, step in steps:
        try:
            result = step()
        except Exception as exc:  # noqa: BLE001 - 吃掉单层失败正是兜底链的职责
            last_error = exc
            if log_fn:
                log_fn(f"{prefix}{name} 失败: {exc}")
            continue
        valid = is_valid(result) if is_valid is not None else result is not None
        if valid:
            return ChainResult(result, name, last_error)
    return ChainResult(None, "", last_error)
