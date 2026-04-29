"""集中读取环境变量的工具。

历史上 stock_data 等模块里散落了大量 `os.environ.get(...) → 转换 → clamp`
的样板代码，每个 6 行。统一成 `env_int / env_float / env_bool` 三个工具，
让调用方只剩一行。
"""
from __future__ import annotations

import os
from typing import Optional


def env_int(key: str, *, default: int, lo: int, hi: int) -> int:
    raw = os.environ.get(key, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(lo, min(value, hi))


def env_float(key: str, *, default: float, lo: float, hi: float) -> float:
    raw = os.environ.get(key, "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError:
        value = default
    return max(lo, min(value, hi))


def env_bool(key: str, *, default: bool = False) -> bool:
    raw = os.environ.get(key, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes")


def env_str(key: str, *, default: str = "") -> str:
    return os.environ.get(key, default).strip()
