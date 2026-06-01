from __future__ import annotations

import collections
import html
import os
import time
import re
import warnings
import threading
from concurrent.futures import as_completed
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, TypeVar, Tuple
from datetime import datetime, timedelta

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ProxyError as RequestsProxyError
from requests.exceptions import Timeout as RequestsTimeout

def _project_root() -> Path:
    return Path(__file__).resolve().parent

def _use_insecure_ssl() -> bool:
    if os.environ.get("ASHARE_SCAN_INSECURE_SSL", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_INSECURE_SSL").is_file() or (root / ".ashare_scan_insecure_ssl").is_file()

def _use_bypass_proxy() -> bool:
    """不走 HTTP(S)_PROXY 等环境代理（避免公司代理对东方财富断开）。"""
    if os.environ.get("ASHARE_SCAN_BYPASS_PROXY", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_BYPASS_PROXY").is_file() or (root / ".ashare_scan_bypass_proxy").is_file()

# 须在 import akshare 之前执行：统一为 requests 补头；可选 SSL / 忽略环境代理
def _apply_network_patches() -> None:
    need_ssl = _use_insecure_ssl()
    need_no_proxy = _use_bypass_proxy()

    if need_ssl:
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except ImportError:
            pass

    try:
        import requests
        _orig_init = requests.Session.__init__
        _orig_req = requests.Session.request

        def _patched_init(self, *args, **kwargs):
            _orig_init(self, *args, **kwargs)
            if need_no_proxy:
                self.trust_env = False

        def _patched_request(self, method, url, **kwargs):
            if need_ssl:
                kwargs.setdefault("verify", False)
            u = str(url)
            if "eastmoney.com" in u:
                merged = _random_eastmoney_headers()
                merged.setdefault("Connection", "close")
                extra = kwargs.get("headers")
                if isinstance(extra, dict):
                    merged.update(extra)
                elif extra is not None:
                    try:
                        merged.update(dict(extra))
                    except (TypeError, ValueError):
                        pass
                kwargs["headers"] = merged
            elif "sina.com.cn" in u or "sinajs.cn" in u:
                kwargs.setdefault("headers", {})
                kwargs["headers"]["User-Agent"] = _random.choice(_USER_AGENT_POOL)
                kwargs["headers"]["Referer"] = "https://finance.sina.com.cn/"
            return _orig_req(self, method, url, **kwargs)

        requests.Session.__init__ = _patched_init  # type: ignore[method-assign]
        requests.Session.request = _patched_request  # type: ignore[method-assign]
    except ImportError:
        pass

_apply_network_patches()

import pandas as pd
import akshare as ak

from stock_logger import get_logger

logger = get_logger(__name__)

from stock_store import (
    clear_history as clear_history_store,
    clear_scan_snapshots,
    clear_universe as clear_universe_store,
    count_history as count_history_store,
    load_app_config as load_app_config_store,
    save_app_config as save_app_config_store,
    history_coverage_summary as load_history_coverage_summary,
    load_all_history_meta_map as load_all_history_meta_map_store,
    load_fund_flow as load_fund_flow_store,
    load_history as load_history_store,
    load_history_meta as load_history_meta_store,
    load_universe as load_universe_store,
    save_fund_flow as save_fund_flow_store,
    save_history as save_history_store,
    save_history_meta_batch as save_history_meta_batch_store,
    save_history_meta as save_history_meta_store,
    save_history_rows_batch as save_history_rows_batch_store,
    save_universe as save_universe_store,
)
from data_source_models import DATA_SOURCE_OPTIONS, DataProviderPlan, HistoryRequestPlan

T = TypeVar("T")

# DaemonThreadPoolExecutor 已迁移到 src/utils/daemon_executor.py；
# 此处重新导出，保持 `from stock_data import DaemonThreadPoolExecutor` 零修改。
from src.utils.daemon_executor import DaemonThreadPoolExecutor
from src.config import env_int, env_float
from src.utils.snapshot_history import snapshot_rows_to_history_rows
from src.utils.trade_calendar import resolve_sync_target_trade_date


def _history_request_concurrency() -> int:
    return env_int("ASHARE_SCAN_HISTORY_CONCURRENCY", default=2, lo=1, hi=10)


def _history_per_source_concurrency() -> int:
    return env_int("ASHARE_SCAN_HISTORY_PER_SOURCE_CONCURRENCY", default=1, lo=1, hi=4)


def _history_total_concurrency_cap() -> int:
    return env_int("ASHARE_SCAN_HISTORY_TOTAL_CONCURRENCY", default=12, lo=1, hi=32)


def _history_min_request_interval_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_MIN_INTERVAL_SEC", default=2.5, lo=0.5, hi=15.0)


def _history_connect_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_CONNECT_TIMEOUT_SEC", default=2.5, lo=0.5, hi=10.0)


def _history_read_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_READ_TIMEOUT_SEC", default=4.0, lo=1.0, hi=15.0)


def _history_total_timeout_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_TOTAL_TIMEOUT_SEC", default=12.0, lo=3.0, hi=60.0)


def _history_host_cooldown_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_HOST_COOLDOWN_SEC", default=180.0, lo=10.0, hi=1800.0)


# 股票池"上次全量重新拉取"的时间戳标记（存 app_config），用于判断是否该自动刷新。
_UNIVERSE_FULL_REFRESH_KEY = "universe_full_refresh_at"


def _universe_max_age_days() -> float:
    """股票池"全量重新拉取"的最大有效天数；超过则建议自动刷新一次（默认 3 天）。"""
    return env_float("ASHARE_UNIVERSE_MAX_AGE_DAYS", default=3.0, lo=0.5, hi=60.0)


def _history_max_mirrors_per_stock() -> int:
    return env_int("ASHARE_SCAN_HISTORY_MAX_MIRRORS_PER_STOCK", default=3, lo=1, hi=8)


def _history_probe_success_target() -> int:
    return env_int("ASHARE_SCAN_HISTORY_PROBE_SUCCESS_TARGET", default=2, lo=1, hi=4)


def _history_block_window_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_BLOCK_WINDOW_SEC", default=180.0, lo=30.0, hi=3600.0)


def _history_block_threshold() -> int:
    return env_int("ASHARE_SCAN_HISTORY_BLOCK_THRESHOLD", default=3, lo=1, hi=10)


def _history_block_cooldown_sec() -> float:
    return env_float("ASHARE_SCAN_HISTORY_BLOCK_COOLDOWN_SEC", default=900.0, lo=60.0, hi=7200.0)


from src.sources.eastmoney import throttling as _em_throttling
_HISTORY_REQUEST_SEMAPHORE = _em_throttling.REQUEST_SEMAPHORE
_HISTORY_REQUEST_RATE_LOCK = _em_throttling.REQUEST_RATE_LOCK

# ---- 自适应请求间隔 ----
# 实现已迁移到 src/sources/eastmoney/throttling.py
_adaptive_on_success = _em_throttling.adaptive_on_success
_adaptive_on_rate_limit = _em_throttling.adaptive_on_rate_limit
_adaptive_current_interval = _em_throttling.adaptive_current_interval

_HISTORY_DIAGNOSTICS_LOCK = _em_throttling._DIAGNOSTICS_LOCK
_HISTORY_DIAGNOSTICS = _em_throttling.DIAGNOSTICS
from src.sources.eastmoney import history as _em_history
_EASTMONEY_HISTORY_MIRRORS = _em_history.HISTORY_MIRRORS
# ---- 全局主机健康管理器 ----
# 实现已迁移到 src/network/host_health.py；下面用别名保持调用方零修改。
from src.network import host_health as _host_health
_GLOBAL_HOST_HEALTH = _host_health._HOST_HEALTH
_GLOBAL_HOST_HEALTH_LOCK = _host_health._HOST_HEALTH_LOCK
_GLOBAL_HOST_FAIL_COUNT = _host_health._HOST_FAIL_COUNT
_global_host_cooldown_sec = _host_health.cooldown_sec
_global_mark_host_failed = _host_health.mark_failed
_global_mark_host_ok = _host_health.mark_ok
_global_host_on_cooldown = _host_health.on_cooldown
_global_host_cooldown_remaining = _host_health.cooldown_remaining
_limit_host_inflight = _host_health.limit_host_inflight


# ---- 东方财富全局熔断器 ----
# 实现已迁移到 src/utils/em_circuit_breaker.py；此处保留原名的薄薄转发，
# 以便 stock_data / stock_filter 中几十处 `_eastmoney_circuit_breaker_*` 调用
# 零修改。真正的状态存在 EMCircuitBreaker 单例里。
from src.utils import em_circuit_breaker as _em_circuit_breaker


def _eastmoney_circuit_breaker_open() -> bool:
    return _em_circuit_breaker.is_open()


def _eastmoney_circuit_breaker_record_failure() -> None:
    _em_circuit_breaker.record_failure()


def _eastmoney_circuit_breaker_record_success() -> None:
    _em_circuit_breaker.record_success()


_global_filter_healthy_urls = _host_health.filter_healthy_urls


_HISTORY_BLOCK_LOCK = _em_throttling._BLOCK_LOCK

# 东方财富 / 通用 HTTP headers 实现已迁移到 src/network/headers.py。
import random as _random
from src.sources import _common as _sources_common
from src.sources import limit_up_pool_service as _lup_service
from src.utils import codes as _utils_codes
from src.utils import parsing as _utils_parsing
from src.sources.eastmoney import numeric as _em_numeric

from src.network.headers import (
    USER_AGENT_POOL as _USER_AGENT_POOL,
    REFERER_POOL as _REFERER_POOL,
    random_eastmoney_headers as _random_eastmoney_headers,
    random_eastmoney_cookie as _random_eastmoney_cookie,
)
_EASTMONEY_HEADERS = _random_eastmoney_headers()


# ---- 可选免费代理池 ----
# 实现已迁移到 src/network/proxy_pool.py；下面用别名保持调用方零修改。
from src.network.proxy_pool import (
    use_proxy_pool as _use_proxy_pool,
    get_proxy as _get_proxy,
    blacklist_proxy as _blacklist_proxy,
    refresh_proxy_pool as _refresh_proxy_pool,
)


# 拉取全市场列表分页时写入 GUI 日志（由 get_all_stocks 临时注册）
_list_download_log: Optional[Callable[[str], None]] = None


EastmoneyRateLimitError = _em_throttling.EastmoneyRateLimitError
HistoryAccessSuspendedError = _em_throttling.HistoryAccessSuspendedError
_increment_history_diagnostic = _em_throttling.increment_diagnostic
_history_diagnostics_snapshot = _em_throttling.diagnostics_snapshot


def _is_transient_network_error(exc: BaseException) -> bool:
    if isinstance(exc, (RequestsConnectionError, RequestsTimeout, OSError)):
        return True
    r = repr(exc)
    if "RemoteDisconnected" in r or "Connection aborted" in r:
        return True
    if "timed out" in r.lower():
        return True
    return False


def _is_name_resolution_error(exc: BaseException) -> bool:
    text = repr(exc)
    lowered = text.lower()
    return (
        "nameresolutionerror" in lowered
        or "failed to resolve" in lowered
        or "nodename nor servname provided" in lowered
        or "temporary failure in name resolution" in lowered
    )


def _retry_ak_call(fn: Callable[..., T], *args, max_attempts: int = 2, base_delay: float = 1.0, **kwargs) -> T:
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < max_attempts - 1 and _is_transient_network_error(e):
                time.sleep(base_delay * (attempt + 1))
                continue
            raise


def _history_retry_ak_call(fn: Callable[..., T], *args, **kwargs) -> T:
    # 东方财富历史接口对并发和出口网络最敏感，保留东财专用闸门。
    # 其它历史源在各自模块内有独立节流阀，不能再共用这个全局闸门。
    with _HISTORY_REQUEST_SEMAPHORE:
        return fn(*args, **kwargs)


_history_access_blocked_until = _em_throttling.history_access_blocked_until
_record_history_block = _em_throttling.record_history_block
_wait_for_history_request_slot = _em_throttling.wait_for_history_request_slot


from src.sources._jsonp import random_callback as _random_jsonp_callback, strip_wrapper as _strip_jsonp_wrapper
from src.sources.eastmoney import history_parser as _em_history_parser


_eastmoney_history_request_params = _em_history_parser.request_params


# 核心 GET 包装：实现已迁移到 src/sources/eastmoney/session.py
from src.sources.eastmoney import session as _em_session
_request_session_get_json = _em_session.get_json


# 东财 intraday + auction snapshot 实现已迁移到 src/sources/eastmoney/intraday.py
from src.sources.eastmoney import intraday as _em_intraday
_fetch_eastmoney_auction_snapshot = _em_intraday.fetch_auction_snapshot
_fetch_eastmoney_intraday_1min = _em_intraday.fetch_intraday_1min
_empty_intraday_meta_payload = _em_intraday.empty_meta_payload
_normalize_intraday_source_frame = _em_intraday.normalize_source_frame
_resolve_intraday_trade_dates = _em_intraday.resolve_trade_dates
_select_intraday_trade_date = _em_intraday.select_trade_date
_slice_intraday_frame_by_trade_date = _em_intraday.slice_frame_by_trade_date


# 限流检测实现已迁移到 src/sources/eastmoney/rate_limit.py
from src.sources.eastmoney import rate_limit as _em_rate_limit
_looks_like_eastmoney_rate_limit = _em_rate_limit.looks_like_rate_limit
_eastmoney_json_indicates_rate_limit = _em_rate_limit.json_indicates_rate_limit


# Mirror 包装：实现已迁移到 src/sources/eastmoney/rate_limit.py + src/network/host_health.py
_history_mirror_host = _em_rate_limit.mirror_host_of
_mark_history_mirror_failed = _host_health.mark_failed
_mark_history_mirror_ok = _host_health.mark_ok
_history_mirror_on_cooldown = _host_health.on_cooldown


from src.sources.eastmoney import mirrors as _em_mirrors


def _prioritize_history_mirrors(
    mirror_urls,
    preferred_mirror=None,
):
    return _em_mirrors.prioritize_history_mirrors(
        mirror_urls, preferred_mirror, max_count=_history_max_mirrors_per_stock()
    )


_parse_eastmoney_hist_json = _em_history_parser.parse_hist_json


_normalize_history_frame = _sources_common.normalize_history_frame


# ---- 新浪财经反封保护 ----
# 实现已迁移到 src/sources/sina.py；下面是公共函数别名。
from src.sources import sina as _src_sina
_fetch_sina_hist_frame = _src_sina.fetch_hist_frame
_fetch_sina_intraday_1min = _src_sina.fetch_intraday_1min
from src.sources.auction_snapshot import snapshot_from_intraday_frame as _snapshot_from_intraday_frame


# ---- 网易财经历史日线 ----
# 实现已迁移到 src/sources/netease.py；下面是公共函数别名。
from src.sources import netease as _src_netease
_fetch_netease_hist_frame = _src_netease.fetch_hist_frame


# ---- 搜狐财经历史日线 ----
# 实现已迁移到 src/sources/sohu.py；下面是公共函数别名。
from src.sources import sohu as _src_sohu
_fetch_sohu_hist_frame = _src_sohu.fetch_hist_frame


# ---- 同花顺 (THS / 10jqka) 历史日线 ----
# 实现已迁移到 src/sources/ths.py；下面是公共函数别名。
from src.sources import ths as _src_ths
_fetch_ths_hist_frame = _src_ths.fetch_hist_frame
_fetch_ths_fund_flow_frame = _src_ths.fetch_fund_flow_frame


# ---- 华尔街见闻 (WallstreetCN) 历史日线 ----
# 实现已迁移到 src/sources/wscn.py；下面是公共函数别名。
from src.sources import wscn as _src_wscn
_fetch_wscn_hist_frame = _src_wscn.fetch_hist_frame


# ---- BaoStock 历史日线 ----
# 不需要 token；字段完整后进入统一 history schema。
from src.sources import baostock as _src_baostock
_fetch_baostock_hist_frame = _src_baostock.fetch_hist_frame


# ---- 通达信 (TDX / xmtdx) 历史日线 ----
# 可选依赖；auto 模式只有本机安装后端时才加入分流通道。
from src.sources import tdx as _src_tdx
_fetch_tdx_hist_frame = _src_tdx.fetch_hist_frame
_tdx_source_available = _src_tdx.is_available


# 历史抓取主函数 + probe：实现已迁移到 src/sources/eastmoney/history.py
_probe_history_mirror = _em_history.probe_mirror
_fetch_eastmoney_hist_frame = _em_history.fetch_hist_frame


# AkShare 警告抑制实现已迁移到 src/sources/eastmoney/akshare_warnings.py
# import 时即注册全局 filterwarnings，无需在此重复。
from src.sources.eastmoney import akshare_warnings as _ak_warnings
_AkshareWarningCategory = _ak_warnings.AkshareWarningCategory
_call_akshare_quietly = _ak_warnings.call_quietly


def clear_universe_data() -> None:
    """清空已保存的股票池和扫描快照。"""
    clear_universe_store()
    clear_scan_snapshots()


def clear_history_data() -> None:
    """清空已保存的历史日线。"""
    clear_history_store()


# Store 包装层：实现已迁移到 src/services/store_facade.py
from src.services import store_facade as _store_facade
_save_universe_store = _store_facade.save_universe
_load_universe_store = _store_facade.load_universe
_load_history_store = _store_facade.load_history
_save_history_store = _store_facade.save_history
_load_fund_flow_store = _store_facade.load_fund_flow
_save_fund_flow_store = _store_facade.save_fund_flow


_eastmoney_request_mirror_urls = _em_mirrors.request_mirror_urls


# AkShare request_with_retry patch：实现已迁移到 src/sources/eastmoney/akshare_patch.py
from src.sources.eastmoney import akshare_patch as _ak_patch
_ashare_request_with_retry = _ak_patch.request_with_retry
_patch_akshare_request_layer = _ak_patch.apply
_patch_akshare_request_layer()


from src.sources import universe as _src_universe
_use_em_full_spot_for_list = _src_universe.use_em_full_spot_for_list


_em_scalar = _em_numeric.em_scalar


_em_price_yuan = _em_numeric.em_price_yuan


_norm_code_series = _utils_codes.norm_code_series


_norm_code = _utils_codes.norm_code


_infer_sz_board = _utils_codes.infer_sz_board


_infer_exchange = _utils_codes.infer_exchange


_infer_market = _sources_common.infer_market
_market_prefixed_code = _sources_common.market_prefixed_code
_first_existing_column = _sources_common.first_existing_column


_normalize_concepts_text = _utils_parsing.normalize_concepts_text
_find_fund_flow_column = _utils_parsing.find_fund_flow_column


_safe_float = _utils_parsing.safe_float


# 日期 / 缓存新鲜度：实现已迁移到 src/utils/cache_freshness.py
from src.utils import cache_freshness as _cache_fresh
_today_ymd = _cache_fresh.today_ymd
_should_refresh_today_row = _cache_fresh.should_refresh_today_row
_estimate_last_trade_date = _cache_fresh.estimate_last_trade_date
_is_history_cache_fresh = _cache_fresh.is_history_cache_fresh
_history_meta_requires_repair = _cache_fresh.history_meta_requires_repair


# 分时进程内 TTL 缓存：盘中 60s 内重复访问同股秒开
# key=(code, target_trade_date, day_offset) → (saved_at_epoch, payload_dict)
# payload_dict 与 get_intraday_data(include_meta=True) 返回结构一致
_INTRADAY_MEM_CACHE: Dict[Tuple[str, str, int], Tuple[float, Dict[str, Any]]] = {}
_INTRADAY_MEM_TTL_SECONDS = 60.0


def _latest_trade_date_from_history_df(df: Optional[pd.DataFrame]) -> str:
    if df is None or df.empty or "date" not in df.columns:
        return ""
    raw_date = str(df["date"].iloc[-1]).strip()
    try:
        normalized = raw_date.replace("/", "-").replace(".", "-")
        if len(normalized) == 8 and normalized.isdigit():
            return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
        return normalized
    except Exception:
        return raw_date


def _history_partial_fields(df: Optional[pd.DataFrame]) -> str:
    """识别历史 K 线里会显著影响评分的缺失字段。"""
    if df is None or df.empty:
        return ""
    missing: List[str] = []
    for col in ("open", "high", "low"):
        if col not in df.columns or pd.to_numeric(df[col], errors="coerce").notna().sum() == 0:
            missing.append(col)
    if "amount" not in df.columns or pd.to_numeric(df["amount"], errors="coerce").notna().sum() == 0:
        missing.append("amount")
    return ",".join(missing)


def _normalize_history_trade_date(value: Any) -> str:
    raw = str(value or "").strip().replace("/", "-").replace(".", "-")
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def _combine_partial_fields(*values: Any) -> str:
    seen: set[str] = set()
    out: List[str] = []
    for value in values:
        for part in str(value or "").replace("，", ",").split(","):
            field = part.strip()
            if field and field not in seen:
                seen.add(field)
                out.append(field)
    return ",".join(out)


def _history_plan_channel_key(plan: HistoryRequestPlan) -> str:
    providers = tuple(str(p or "").strip().lower() for p in plan.provider_sequence if str(p or "").strip())
    if len(providers) == 1:
        return providers[0]
    if providers:
        return "fallback:" + "+".join(providers)
    return str(plan.reason or "unknown")


def _intraday_market_closed_now() -> bool:
    """本地时间 ≥ 15:30 视为收盘——东财集合竞价 + 收盘清算到 15:30 后基本固化。"""
    now = datetime.now()
    return (now.hour, now.minute) >= (15, 30)


def _intraday_mem_get(key: Tuple[str, str, int]) -> Optional[Dict[str, Any]]:
    entry = _INTRADAY_MEM_CACHE.get(key)
    if entry is None:
        return None
    saved_at, payload = entry
    if (time.time() - saved_at) > _INTRADAY_MEM_TTL_SECONDS:
        _INTRADAY_MEM_CACHE.pop(key, None)
        return None
    return payload


def _intraday_mem_put(key: Tuple[str, str, int], payload: Dict[str, Any]) -> None:
    _INTRADAY_MEM_CACHE[key] = (time.time(), payload)
    # 简单 LRU：超过 256 条就清掉最老的一半，避免无限增长
    if len(_INTRADAY_MEM_CACHE) > 256:
        oldest = sorted(_INTRADAY_MEM_CACHE.items(), key=lambda kv: kv[1][0])[:128]
        for k, _ in oldest:
            _INTRADAY_MEM_CACHE.pop(k, None)


def _clear_intraday_mem_cache() -> None:
    """供测试或运行时清缓存用。"""
    _INTRADAY_MEM_CACHE.clear()


_build_a_share_universe = _src_universe.build_a_share_universe


from src.utils.lru_cache import LRUCache as _LRUCache


# intraday 派生 helper：已迁移到 src/sources/limit_up_pool_service.py。
# 此处保留模块级别名，保持 `from stock_data import _derive_seal_time_from_intraday` 调用零修改。
_derive_seal_time_from_intraday = _lup_service.derive_seal_time_from_intraday
_count_intraday_breaks = _lup_service.count_intraday_breaks


class StockDataFetcher:
    def __init__(self):
        self._log: Optional[Callable[[str], None]] = None
        self._strong_pool_cache: Dict[str, pd.DataFrame] = _LRUCache(maxsize=30)
        self._limit_up_pool_cache: Dict[str, pd.DataFrame] = _LRUCache(maxsize=30)
        self._prev_limit_up_pool_cache: Dict[str, pd.DataFrame] = _LRUCache(maxsize=30)
        # 跟踪每个日期涨停池数据来源："cache_memory" / "cache_db" / "eastmoney" / "spot_fallback" / "empty"
        self._last_pool_source: Dict[str, str] = {}
        self._last_prev_pool_source: Dict[str, str] = {}
        self._limit_up_reason_cache: Dict[str, Dict[str, Dict[str, str]]] = _LRUCache(maxsize=30)
        self._concepts_cache: Optional[Dict[str, str]] = None
        self._universe_concepts_cache: Optional[Dict[str, str]] = None
        self._history_mirror_cache: List[str] = []
        self._history_mirror_checked_at: float = 0.0
        try:
            configured_limit = int(os.environ.get("ASHARE_SCAN_CONCEPT_BOARD_LIMIT", "20").strip() or "20")
        except ValueError:
            configured_limit = 20
        self.concept_board_limit: int = max(5, min(configured_limit, 80))
        try:
            configured_timeout = float(os.environ.get("ASHARE_SCAN_CONCEPT_FILL_TIMEOUT_SEC", "25").strip() or "25")
        except ValueError:
            configured_timeout = 25.0
        self.concept_fill_timeout_sec: float = max(5.0, configured_timeout)
        self._concepts_lock = threading.Lock()
        self._last_history_probe_failures: Dict[str, str] = {}
        self._last_history_block_log_at: float = 0.0
        self._default_history_source: str = "auto"
        self._default_intraday_source: str = "auto"
        self._default_fund_flow_source: str = "auto"
        self._default_limit_up_reason_source: str = "auto"
        # 启动时预加载代理池（后台线程，不阻塞初始化）
        if _use_proxy_pool():
            threading.Thread(target=_refresh_proxy_pool, daemon=True).start()

    def set_log_callback(self, cb: Optional[Callable[[str], None]]) -> None:
        self._log = cb

    def history_request_concurrency_limit(self) -> int:
        return _history_request_concurrency()

    def get_history_cache_summary(self) -> Dict[str, Any]:
        payload = load_history_coverage_summary()
        payload["history_source"] = self._default_history_source
        return payload

    def _normalize_source(self, domain: str, source: str) -> str:
        options = DATA_SOURCE_OPTIONS.get(domain, ("auto",))
        value = str(source or "auto").strip().lower()
        return value if value in options else "auto"

    def normalize_history_source(self, source: str) -> str:
        return self._normalize_source("history", source)

    def normalize_intraday_source(self, source: str) -> str:
        value = str(source or "auto").strip().lower()
        if value == "legacy":
            value = "sina"
        return self._normalize_source("intraday", value)

    def normalize_fund_flow_source(self, source: str) -> str:
        return self._normalize_source("fund_flow", source)

    def normalize_limit_up_reason_source(self, source: str) -> str:
        return self._normalize_source("limit_up_reason", source)

    def set_default_history_source(self, source: str) -> None:
        self._default_history_source = self.normalize_history_source(source)

    def set_default_intraday_source(self, source: str) -> None:
        self._default_intraday_source = self.normalize_intraday_source(source)

    def set_default_fund_flow_source(self, source: str) -> None:
        self._default_fund_flow_source = self.normalize_fund_flow_source(source)

    def set_default_limit_up_reason_source(self, source: str) -> None:
        self._default_limit_up_reason_source = self.normalize_limit_up_reason_source(source)

    def _build_multi_source_plans(self, source: str) -> List[HistoryRequestPlan]:
        """构建多源并行请求计划列表，用于批量更新时分流。
        将可用的 eastmoney 镜像各自作为一个独立 plan，
        再加上其它完整历史源作为补充源，实现负载均衡。
        """
        normalized = self.normalize_history_source(source)

        plans: List[HistoryRequestPlan] = []

        # 东方财富：每个健康镜像各建一个 plan 用于轮转分流。
        # 注意：这些镜像 plan 的通道 key 都是 "eastmoney"（见 _history_plan_channel_key），
        # 因此它们共用同一个 per_source 信号量 → 东财整体同一时刻只有 per_source_limit 个在跑，
        # 镜像之间是“轮转”而非“并发”。这是有意为之：东财容易被封，刻意保守、不让镜像并发放大请求量。
        em_skipped = False
        if normalized in ("auto", "eastmoney"):
            if normalized == "auto" and _eastmoney_circuit_breaker_open():
                em_skipped = True
                logger.debug("auto 模式：东财熔断中，跳过东财镜像")
            else:
                mirrors = self.get_available_history_mirrors()
                for mirror in mirrors:
                    plans.append(HistoryRequestPlan(
                        mode="network",
                        provider_sequence=("eastmoney",),
                        mirror_urls=(mirror,),
                        reason=f"multi-source-eastmoney-{_history_mirror_host(mirror)}",
                    ))

        # 新浪/网易/搜狐/同花顺/华尔街见闻：作为补充分流通道（跳过正在冷却的源）
        if normalized in ("auto", "sina"):
            if not _global_host_on_cooldown("finance.sina.com.cn"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("sina",),
                    mirror_urls=(),
                    reason="multi-source-sina",
                ))
        if normalized in ("auto", "netease"):
            if not _global_host_on_cooldown("quotes.money.163.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("netease",),
                    mirror_urls=(),
                    reason="multi-source-netease",
                ))
        if normalized in ("auto", "sohu"):
            if not _global_host_on_cooldown("q.stock.sohu.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("sohu",),
                    mirror_urls=(),
                    reason="multi-source-sohu",
                ))
        if normalized in ("auto", "ths"):
            if not _global_host_on_cooldown("d.10jqka.com.cn"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("ths",),
                    mirror_urls=(),
                    reason="multi-source-ths",
                ))
        if normalized in ("auto", "wscn"):
            if not _global_host_on_cooldown("api-ddc-wscn.awtmt.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("wscn",),
                    mirror_urls=(),
                    reason="multi-source-wscn",
                ))
        if normalized in ("auto", "baostock"):
            if not _global_host_on_cooldown("baostock.com"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("baostock",),
                    mirror_urls=(),
                    reason="multi-source-baostock",
                ))
        if normalized in ("auto", "tdx"):
            if (normalized == "tdx" or _tdx_source_available()) and not _global_host_on_cooldown("tdx"):
                plans.append(HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("tdx",),
                    mirror_urls=(),
                    reason="multi-source-tdx",
                ))

        # 兜底：至少保证一个 auto plan
        if not plans:
            plans.append(self.build_history_request_plan(source=source, force_refresh=False))

        return plans

    def _fetch_history_cache_spot_snapshot(self) -> Optional[pd.DataFrame]:
        from src.services.scoring.first_board import fetch_spot_snapshot

        return fetch_spot_snapshot(log_fn=self._log)

    def _fast_append_spot_snapshot_to_history_cache(
        self,
        rows: List[Dict[str, Any]],
        *,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Dict[str, Any]:
        """用一次全市场快照批量补最近已收盘交易日的 history 行。"""
        stats: Dict[str, Any] = {
            "enabled": True,
            "appended": 0,
            "failed": 0,
            "converted": 0,
            "skipped_reason": "",
        }
        if should_stop and should_stop():
            stats["skipped_reason"] = "stopped"
            return stats

        try:
            target = resolve_sync_target_trade_date()
        except Exception as exc:
            stats["skipped_reason"] = f"target-error:{exc}"
            if self._log:
                self._log(f"快照补K：交易日判定失败，跳过快照补K: {exc}")
            return stats

        if not target.allows_history_write():
            stats["skipped_reason"] = f"phase={target.phase}"
            if self._log:
                self._log(
                    f"快照补K：当前处于 {target.phase}，不把盘中快照写入历史K线。"
                )
            return stats

        target_codes = {
            str(item.get("code", "") or "").strip().zfill(6)
            for item in rows
            if str(item.get("code", "") or "").strip()
        }
        if not target_codes:
            stats["skipped_reason"] = "empty-universe"
            return stats

        try:
            spot_df = self._fetch_history_cache_spot_snapshot()
        except Exception as exc:
            stats["skipped_reason"] = f"spot-error:{exc}"
            if self._log:
                self._log(f"快照补K：全市场快照获取失败，跳过快照补K: {exc}")
            return stats
        if spot_df is None or spot_df.empty:
            stats["skipped_reason"] = "empty-spot"
            if self._log:
                self._log("快照补K：全市场快照为空，跳过快照补K。")
            return stats

        converted = snapshot_rows_to_history_rows(
            spot_df.to_dict("records"),
            target.target_date,
            target.phase,
        )
        stats["converted"] = len(converted)
        if not converted:
            stats["skipped_reason"] = "no-converted-rows"
            return stats

        meta_map = load_all_history_meta_map_store()
        rows_by_code: Dict[str, pd.DataFrame] = {}
        metas_by_code: Dict[str, Dict[str, Any]] = {}
        target_date = _normalize_history_trade_date(target.target_date)
        for row in converted:
            code = str(row.get("code", "") or "").strip().zfill(6)
            if code not in target_codes:
                continue
            meta = meta_map.get(code) or {}
            latest = _normalize_history_trade_date(meta.get("latest_trade_date"))
            if latest and latest > target_date:
                continue

            frame_row = dict(row)
            frame_row.pop("code", None)
            row_partial = str(frame_row.pop("partial_fields", "") or "")
            frame_row.pop("needs_repair", None)
            if frame_row.get("amount") is None:
                row_partial = _combine_partial_fields(row_partial, "amount")

            rows_by_code[code] = pd.DataFrame([frame_row])

            existing_partial = str(meta.get("partial_fields") or "").strip()
            existing_source = str(meta.get("source") or "").strip()
            existing_needs_repair = _history_meta_requires_repair(meta)
            if existing_needs_repair and not existing_partial and existing_source.lower() == "tencent":
                existing_partial = "amount"
            partial_fields = _combine_partial_fields(
                existing_partial if existing_needs_repair else "",
                row_partial,
            )
            try:
                current_count = int(meta.get("row_count", 0) or 0)
            except (TypeError, ValueError):
                current_count = 0
            count_delta = 1 if not latest or latest < target_date else 0
            row_count = max(1, current_count + count_delta)
            metas_by_code[code] = {
                "code": code,
                "latest_trade_date": target_date,
                "row_count": row_count,
                "source": "spot-snapshot",
                "partial_fields": partial_fields,
                "needs_repair": 1 if partial_fields else 0,
            }

        if not rows_by_code:
            stats["skipped_reason"] = "no-target-rows"
            return stats

        success_codes, failed_codes = save_history_rows_batch_store(rows_by_code)
        metas = [metas_by_code[code] for code in success_codes if code in metas_by_code]
        meta_success, meta_failed = save_history_meta_batch_store(metas)
        stats["appended"] = len(meta_success)
        stats["failed"] = len(set(failed_codes) | set(meta_failed))
        if self._log:
            self._log(
                f"快照补K：目标日 {target_date}，写入 {stats['appended']} 只，"
                f"失败 {stats['failed']} 只。"
            )
        return stats

    def _resolve_history_update_worker_count(
        self,
        requested_workers: Optional[int],
        plans: List[HistoryRequestPlan],
    ) -> Tuple[int, int, int]:
        per_source_limit = _history_per_source_concurrency()
        channel_count = max(1, len({_history_plan_channel_key(plan) for plan in plans}))
        natural_workers = max(1, channel_count * per_source_limit)
        try:
            requested = int(requested_workers or 0)
        except (TypeError, ValueError):
            requested = 0
        if requested <= 0:
            requested = self.history_request_concurrency_limit()
        worker_count = min(max(requested, natural_workers), _history_total_concurrency_cap())
        return max(1, worker_count), per_source_limit, channel_count

    def update_history_cache(
        self,
        max_stocks: int = 0,
        days: int = 60,
        source: Optional[str] = None,
        workers: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, str, str, int, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        refresh_universe: bool = False,
        allowed_boards: Optional[List[str]] = None,
        fast_daily_append: bool = True,
    ) -> Dict[str, Any]:
        universe = self.get_all_stocks(force_refresh=refresh_universe)
        if universe is None or universe.empty:
            return {"total": 0, "updated": 0, "failed": 0, "skipped": 0}
        if allowed_boards and "board" in universe.columns:
            allowed = {str(x).strip() for x in allowed_boards if str(x).strip()}
            if allowed:
                board_col = universe["board"].astype(str).str.strip()
                # 板块为空的票（多是新上市 / 从涨停池补进来、metadata 未补全）不参与板块过滤，
                # 否则会被静默剔除 → 永远不进缓存 → 新涨停票预测被硬中止。
                universe = universe[board_col.isin(allowed) | (board_col == "")].reset_index(drop=True)
        if max_stocks and max_stocks > 0:
            universe = universe.head(max_stocks).reset_index(drop=True)
        rows = universe.to_dict("records")
        total = len(rows)
        if total <= 0:
            return {"total": 0, "updated": 0, "failed": 0, "skipped": 0}

        snapshot_stats: Dict[str, Any] = {
            "enabled": bool(fast_daily_append),
            "appended": 0,
            "failed": 0,
            "converted": 0,
            "skipped_reason": "disabled" if not fast_daily_append else "",
        }
        if fast_daily_append:
            snapshot_stats = self._fast_append_spot_snapshot_to_history_cache(
                rows,
                should_stop=should_stop,
            )

        pre_skipped = 0
        pending_rows: List[Dict[str, Any]] = []
        for item in rows:
            code = str(item.get("code", "")).strip().zfill(6)
            if should_stop and should_stop():
                pre_skipped += 1
                continue
            if _is_history_cache_fresh(code, max(1, days), self._log):
                pre_skipped += 1
            else:
                pending_rows.append(item)

        if not pending_rows:
            return {
                "total": total,
                "updated": 0,
                "failed": 0,
                "skipped": pre_skipped,
                "plan": "snapshot-only/no-pending-history",
                "workers": 0,
                "per_source_concurrency": _history_per_source_concurrency(),
                "source_channels": 0,
                "snapshot_appended": int(snapshot_stats.get("appended", 0) or 0),
                "snapshot_failed": int(snapshot_stats.get("failed", 0) or 0),
                "snapshot_skipped_reason": str(snapshot_stats.get("skipped_reason", "") or ""),
            }

        # ---- 多源并行分流策略 ----
        source_str = source or self._default_history_source
        multi_plans = self._build_multi_source_plans(source_str)
        plan_count = len(multi_plans)
        worker_count, per_source_limit, channel_count = self._resolve_history_update_worker_count(
            workers,
            multi_plans,
        )
        channel_limits = {
            key: threading.BoundedSemaphore(per_source_limit)
            for key in {_history_plan_channel_key(plan) for plan in multi_plans}
        }

        if self._log:
            plan_names = [p.reason for p in multi_plans]
            self._log(
                f"多源分流策略：{plan_count} 个计划/{channel_count} 个源通道，"
                f"每源并发≤{per_source_limit}，执行线程={worker_count} → {', '.join(plan_names)}"
            )

        # 打乱股票顺序，避免同板块集中请求
        rows = pending_rows
        _random.shuffle(rows)

        updated = 0
        failed = 0
        skipped = pre_skipped

        plans_by_channel: Dict[str, HistoryRequestPlan] = {}
        for plan in multi_plans:
            plans_by_channel.setdefault(_history_plan_channel_key(plan), plan)
        fallback_plans = list(plans_by_channel.values())

        def _call_history_with_plan(code: str, plan: HistoryRequestPlan) -> Optional[pd.DataFrame]:
            channel_key = _history_plan_channel_key(plan)
            # channel_limits 已在上方按所有计划的通道预建，这里只读取；
            # 仅在极端竞态下才兜底新建，避免每次调用都白白构造一个一次性信号量。
            semaphore = channel_limits.get(channel_key)
            if semaphore is None:
                semaphore = channel_limits.setdefault(
                    channel_key, threading.BoundedSemaphore(per_source_limit)
                )
            with semaphore:
                return self.get_history_data(
                    code, days=days, force_refresh=True, request_plan=plan,
                )

        def _pick_fallback_plan(assigned_plan: HistoryRequestPlan, item_index: int) -> Optional[HistoryRequestPlan]:
            assigned_key = _history_plan_channel_key(assigned_plan)
            candidates = [
                plan for plan in fallback_plans
                if _history_plan_channel_key(plan) != assigned_key
            ]
            if not candidates:
                return None
            return candidates[item_index % len(candidates)]

        def _work(
            item_index: int,
            item: Dict[str, Any],
            assigned_plan: HistoryRequestPlan,
        ) -> tuple[str, str, bool, bool]:
            """返回 (code, name, success, skipped)"""
            code = str(item.get("code", "")).strip().zfill(6)
            name = str(item.get("name", "") or "")
            if should_stop and should_stop():
                return code, name, False, True
            if _is_history_cache_fresh(code, max(1, days), self._log):
                return code, name, True, True

            # 检查分配的源是否已冷却，是则直接用 fallback
            _host_map = {
                "sina": "finance.sina.com.cn", "netease": "quotes.money.163.com",
                "sohu": "q.stock.sohu.com",
                "ths": "d.10jqka.com.cn", "wscn": "api-ddc-wscn.awtmt.com",
                "baostock": "baostock.com",
                "tdx": "tdx",
            }
            assigned_all_cooled = all(
                _global_host_on_cooldown(_host_map[p])
                for p in assigned_plan.provider_sequence
                if p in _host_map
            ) if assigned_plan.provider_sequence else False

            fallback_plan = _pick_fallback_plan(assigned_plan, item_index)
            use_plan = fallback_plan if (assigned_all_cooled and fallback_plan) else assigned_plan
            df = _call_history_with_plan(code, use_plan)
            if df is not None and not df.empty:
                return code, name, True, False
            # 如果分配源失败，只轮转到一个其它健康源，避免失败任务同时涌向同一备用源。
            if use_plan is assigned_plan and fallback_plan is not None:
                df = _call_history_with_plan(code, fallback_plan)
            return code, name, bool(df is not None and not df.empty), False

        with DaemonThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="hist-cache") as executor:
            futures = [
                executor.submit(_work, idx, item, multi_plans[idx % plan_count])
                for idx, item in enumerate(rows)
            ]
            completed = pre_skipped
            for fut in as_completed(futures):
                completed += 1
                code, name, ok, was_skipped = fut.result()
                if should_stop and should_stop():
                    skipped += max(0, total - completed)
                    break
                if was_skipped:
                    skipped += 1
                elif ok:
                    updated += 1
                else:
                    failed += 1
                if progress_callback:
                    progress_callback(completed, total, code, name, updated, failed, skipped)

        return {
            "total": total,
            "updated": updated,
            "failed": failed,
            "skipped": skipped,
            "plan": f"multi-source/{plan_count}channels",
            "workers": worker_count,
            "per_source_concurrency": per_source_limit,
            "source_channels": channel_count,
            "snapshot_appended": int(snapshot_stats.get("appended", 0) or 0),
            "snapshot_failed": int(snapshot_stats.get("failed", 0) or 0),
            "snapshot_skipped_reason": str(snapshot_stats.get("skipped_reason", "") or ""),
        }

    def build_intraday_request_plan(self, source: str = "auto") -> DataProviderPlan:
        normalized = self.normalize_intraday_source(source)
        if normalized == "eastmoney":
            return DataProviderPlan(mode="network", provider_sequence=("eastmoney",), reason="intraday-provider=eastmoney")
        if normalized == "sina":
            return DataProviderPlan(mode="network", provider_sequence=("sina",), reason="intraday-provider=sina")
        # auto 模式：东财熔断时优先用 sina
        if _eastmoney_circuit_breaker_open():
            return DataProviderPlan(mode="network", provider_sequence=("sina", "eastmoney"), reason="intraday-provider=auto(em-circuit-open)")
        return DataProviderPlan(mode="network", provider_sequence=("eastmoney", "sina"), reason="intraday-provider=auto")

    def build_fund_flow_request_plan(self, source: str = "auto") -> DataProviderPlan:
        normalized = self.normalize_fund_flow_source(source)
        if normalized == "eastmoney":
            return DataProviderPlan(mode="network", provider_sequence=("eastmoney",), reason="fund-flow-provider=eastmoney")
        if normalized == "ths":
            return DataProviderPlan(mode="network", provider_sequence=("ths",), reason="fund-flow-provider=ths")
        # auto 模式：东财熔断时优先用 ths
        if _eastmoney_circuit_breaker_open():
            return DataProviderPlan(mode="network", provider_sequence=("ths", "eastmoney"), reason="fund-flow-provider=auto(em-circuit-open)")
        return DataProviderPlan(mode="network", provider_sequence=("eastmoney", "ths"), reason="fund-flow-provider=auto")

    def build_limit_up_reason_plan(self, source: str = "auto") -> DataProviderPlan:
        normalized = self.normalize_limit_up_reason_source(source)
        if normalized == "fupanwang":
            return DataProviderPlan(mode="network", provider_sequence=("fupanwang",), reason="limit-up-provider=fupanwang")
        return DataProviderPlan(mode="network", provider_sequence=("fupanwang",), reason="limit-up-provider=auto")

    def build_history_request_plan(self, source: str = "auto", force_refresh: bool = False) -> HistoryRequestPlan:
        normalized = self.normalize_history_source(source)
        if normalized == "sina":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("sina",),
                mirror_urls=(),
                reason="history-provider=sina",
            )
        if normalized == "netease":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("netease",),
                mirror_urls=(),
                reason="history-provider=netease",
            )
        if normalized == "sohu":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("sohu",),
                mirror_urls=(),
                reason="history-provider=sohu",
            )
        if normalized == "ths":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("ths",),
                mirror_urls=(),
                reason="history-provider=ths",
            )
        if normalized == "wscn":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("wscn",),
                mirror_urls=(),
                reason="history-provider=wscn",
            )
        if normalized == "baostock":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("baostock",),
                mirror_urls=(),
                reason="history-provider=baostock",
            )
        if normalized == "tdx":
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("tdx",),
                mirror_urls=(),
                reason="history-provider=tdx",
            )

        mirrors = tuple(self.get_available_history_mirrors(force_refresh=force_refresh))
        if normalized == "eastmoney":
            if mirrors:
                return HistoryRequestPlan(
                    mode="network",
                    provider_sequence=("eastmoney",),
                    mirror_urls=mirrors,
                    reason="history-provider=eastmoney",
                )
            failures = self.get_last_history_probe_failures()
            reason = ""
            if failures:
                reason = "；".join(f"{host}: {detail}" for host, detail in list(failures.items())[:3])
            if not reason:
                reason = "history-mirrors-unavailable"
            return HistoryRequestPlan(
                mode="cache_only",
                provider_sequence=("eastmoney",),
                mirror_urls=(),
                reason=reason,
            )

        _non_em_providers = ["sina", "ths", "netease", "sohu", "wscn", "baostock"]
        if _tdx_source_available() and not _global_host_on_cooldown("tdx"):
            _non_em_providers.append("tdx")
        non_em_providers = tuple(_non_em_providers)
        if _eastmoney_circuit_breaker_open():
            # 东财熔断中：auto 模式直接用非东财源，避免无意义的重试
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=non_em_providers,
                mirror_urls=(),
                reason="history-provider=auto(eastmoney-circuit-open)",
            )
        if mirrors:
            return HistoryRequestPlan(
                mode="network",
                provider_sequence=("eastmoney",) + non_em_providers,
                mirror_urls=mirrors,
                reason="history-provider=auto",
            )
        failures = self.get_last_history_probe_failures()
        reason = ""
        if failures:
            reason = "；".join(f"{host}: {detail}" for host, detail in list(failures.items())[:3])
        if not reason:
            reason = "history-mirrors-unavailable"
        return HistoryRequestPlan(
            mode="network",
            provider_sequence=non_em_providers,
            mirror_urls=(),
            reason=reason,
        )

    def get_runtime_diagnostics(self) -> Dict[str, Any]:
        blocked_until = _history_access_blocked_until()
        now = time.time()
        diagnostics: Dict[str, Any] = _history_diagnostics_snapshot()
        diagnostics.update(
            {
                "history_concurrency_limit": _history_request_concurrency(),
                "history_min_interval_sec": _history_min_request_interval_sec(),
                "history_host_cooldown_sec": _history_host_cooldown_sec(),
                "history_block_threshold": _history_block_threshold(),
                "history_block_window_sec": _history_block_window_sec(),
                "history_block_cooldown_sec": _history_block_cooldown_sec(),
                "history_request_blocked": blocked_until > now,
                "history_request_blocked_for_sec": max(0, int(blocked_until - now)) if blocked_until > now else 0,
                "cached_mirror_count": len(self._history_mirror_cache),
                "cached_mirrors": [
                    _history_mirror_host(url) for url in self._history_mirror_cache
                ],
            }
        )
        return diagnostics

    def _log_history_access_suspended(self) -> bool:
        blocked_until = _history_access_blocked_until()
        now = time.time()
        if blocked_until <= now:
            return False
        if self._log and (now - self._last_history_block_log_at >= 10):
            remain = max(1, int(blocked_until - now))
            self._log(
                f"东方财富历史接口已进入冷却保护，接下来约 {remain}s 内不再发新请求，优先回退本地缓存。"
            )
            self._last_history_block_log_at = now
        return True

    def get_available_history_mirrors(self, force_refresh: bool = False) -> List[str]:
        now = time.time()
        if not force_refresh and now - self._history_mirror_checked_at < 180:
            _increment_history_diagnostic("probe_cache_hits")
            return list(self._history_mirror_cache)
        if self._log_history_access_suspended():
            return list(self._history_mirror_cache)

        available: List[str] = []
        failures: Dict[str, str] = {}
        if self._log:
            self._log("开始检测东方财富历史接口镜像可用性...")
        for url in _EASTMONEY_HISTORY_MIRRORS:
            ok, detail = _probe_history_mirror(url)
            host = re.sub(r"^https?://", "", url).split("/", 1)[0]
            if ok:
                available.append(url)
                if self._log:
                    self._log(f"历史镜像可用 {host}，最新日期 {detail}")
                if len(available) >= _history_probe_success_target():
                    break
            else:
                failures[host] = str(detail)
                if self._log:
                    self._log(f"历史镜像不可用 {host}：{detail}")
                if "冷却保护" in str(detail):
                    break
        self._history_mirror_cache = available
        self._history_mirror_checked_at = now
        self._last_history_probe_failures = failures
        if not available and self._log and failures:
            dns_failed = sum(1 for detail in failures.values() if _is_name_resolution_error(RuntimeError(detail)))
            if dns_failed == len(failures):
                self._log("历史镜像全部失败，且都属于 DNS 解析失败；当前更像是本机网络/解析环境异常，不是单个镜像故障。")
        return list(available)

    def get_last_history_probe_failures(self) -> Dict[str, str]:
        return dict(self._last_history_probe_failures)


    def clear_saved_universe_data(self) -> None:
        clear_universe_data()
        self._concepts_cache = None
        self._universe_concepts_cache = None

    def clear_history_data(self) -> None:
        clear_history_data()

    def _load_concepts_map(
        self,
        target_codes: Optional[List[str]] = None,
        max_boards: Optional[int] = None,
    ) -> Dict[str, str]:
        target_set = {
            _norm_code(code) for code in (target_codes or []) if _norm_code(code)
        }
        if not target_set:
            return {}
        with self._concepts_lock:
            if self._concepts_cache is None:
                self._concepts_cache = {}
            pending = {code for code in target_set if not self._concepts_cache.get(code)}
            if not pending:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        board_cap = max(1, int(max_boards or self.concept_board_limit))
        concept_map: Dict[str, List[str]] = {}
        started_at = time.time()
        if self._log:
            self._log(
                f"开始补全股票概念：目标 {len(pending)} 只，最多扫描 {board_cap} 个概念板块。"
            )

        try:
            boards = _call_akshare_quietly(ak.stock_board_concept_name_em)
        except Exception as e:
            if self._log:
                self._log(f"概念板块名称获取失败: {e}")
            with self._concepts_lock:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        if boards is None or boards.empty or "板块名称" not in boards.columns:
            with self._concepts_lock:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        board_names = [
            str(name).strip()
            for name in boards["板块名称"].tolist()
            if str(name).strip()
        ]
        if not board_names:
            with self._concepts_lock:
                return {code: self._concepts_cache.get(code, "") for code in target_set}

        board_names = board_names[:board_cap]
        total = len(board_names)
        found_codes: set[str] = set()

        for idx, board_name in enumerate(board_names, start=1):
            if pending and pending.issubset(found_codes):
                break
            if time.time() - started_at >= self.concept_fill_timeout_sec:
                if self._log:
                    self._log(
                        f"概念补全达到 {self.concept_fill_timeout_sec:.0f}s 超时上限，提前结束本轮。"
                    )
                break
            try:
                cons = _call_akshare_quietly(ak.stock_board_concept_cons_em, symbol=board_name)
            except Exception as e:
                if self._log and (idx % 10 == 0 or idx == total):
                    self._log(f"概念板块 {idx}/{total} {board_name} 获取失败: {e}")
                continue

            if cons is None or cons.empty:
                continue

            code_col = "代码" if "代码" in cons.columns else "code" if "code" in cons.columns else None
            if code_col is None:
                continue

            codes = cons[code_col].astype(str).map(_norm_code).tolist()
            for code in codes:
                if not code or code not in pending:
                    continue
                bucket = concept_map.setdefault(code, [])
                if board_name not in bucket:
                    bucket.append(board_name)
                found_codes.add(code)

            if self._log and (idx % 10 == 0 or idx == total):
                self._log(
                    f"概念板块进度 {idx}/{total}: {board_name}，已命中 {len(concept_map)} / {len(pending)} 只"
                )

        with self._concepts_lock:
            for code, names in concept_map.items():
                self._concepts_cache[code] = _normalize_concepts_text("、".join(names))
            return {code: self._concepts_cache.get(code, "") for code in target_set}

    def preload_stock_concepts(
        self,
        stock_codes: List[str],
        max_boards: Optional[int] = None,
    ) -> Dict[str, str]:
        target_codes = [_norm_code(code) for code in stock_codes if _norm_code(code)]
        if not target_codes:
            return {}
        return self._load_concepts_map(target_codes, max_boards=max_boards)

    def _set_universe_concepts_cache(self, df: pd.DataFrame) -> None:
        cache: Dict[str, str] = {}
        if df is not None and not df.empty and "code" in df.columns and "concepts" in df.columns:
            for code, concepts in zip(df["code"].astype(str), df["concepts"].astype(str)):
                norm_code = _norm_code(code)
                if norm_code:
                    cache[norm_code] = _normalize_concepts_text(concepts)
        self._universe_concepts_cache = cache

    def _normalize_trade_date(self, trade_date: str) -> str:
        return re.sub(r"\D", "", str(trade_date or ""))[:8]

    def _load_strong_pool(self, trade_date: str, source: Optional[str] = None) -> pd.DataFrame:
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return pd.DataFrame()
        cache_key = f"eastmoney:{date_key}"
        cached = self._strong_pool_cache.get(cache_key)
        if cached is not None:
            return cached
        df = pd.DataFrame()
        last_error: Optional[Exception] = None
        if _eastmoney_circuit_breaker_open():
            logger.debug("强势股池 %s：东财熔断中，跳过", date_key)
        else:
            try:
                df = _retry_ak_call(ak.stock_zt_pool_strong_em, date=date_key)
            except Exception as e:
                last_error = e
                if self._log:
                    self._log(f"强势股池 {date_key} 获取失败: {e}")
        if df is None or getattr(df, "empty", True):
            df = pd.DataFrame()
            if last_error is not None and self._log:
                self._log(f"强势股池数据源全部失败 {date_key}: {last_error}")
        self._strong_pool_cache[cache_key] = df
        return df

    @staticmethod
    def _normalize_stock_name(value: str) -> str:
        name = str(value or "").strip()
        if not name:
            return ""
        return re.sub(r"\s+", "", name).upper()

    @staticmethod
    def _normalize_trade_date_display(trade_date: str) -> str:
        date_key = re.sub(r"\D", "", str(trade_date or ""))[:8]
        if len(date_key) != 8:
            return ""
        return f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"

    def _fetch_text_with_optional_proxy_bypass(self, url: str, params: Optional[Dict[str, Any]] = None) -> str:
        import requests

        session_args: Dict[str, Any] = {
            "url": url,
            "params": params or {},
            "timeout": (5, 12),
            "headers": {
                "User-Agent": _random.choice(_USER_AGENT_POOL),
                "Referer": "https://www.fupanwang.com/",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            },
        }
        if _use_insecure_ssl():
            session_args["verify"] = False

        def _get(*, bypass_proxy: bool) -> str:
            with requests.Session() as session:
                if bypass_proxy:
                    session.trust_env = False
                    session.proxies = {"http": None, "https": None}
                response = session.get(**session_args)
                response.raise_for_status()
                response.encoding = response.encoding or response.apparent_encoding or "utf-8"
                return response.text or ""

        try:
            return _get(bypass_proxy=_use_bypass_proxy())
        except RequestsProxyError:
            return _get(bypass_proxy=True)

    @staticmethod
    def _parse_limit_up_reason_page(html_text: str) -> Dict[str, Dict[str, str]]:
        """从复盘页 HTML 中提取 {名称: {reason, detail}}。"""
        if not html_text:
            return {}

        text = re.sub(r"(?is)<script[^>]*>.*?</script>", "\n", html_text)
        text = re.sub(r"(?is)<style[^>]*>.*?</style>", "\n", text)
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</(tr|td|th|li|p|div|section|article|header|footer|main|h\d|a)>", "\n", text)
        text = re.sub(r"(?s)<[^>]+>", "", text)
        text = html.unescape(text)
        lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
        lines = [line for line in lines if line]
        if not lines:
            return {}

        start = 0
        for idx, line in enumerate(lines):
            if all(token in line for token in ("时间", "股票名称", "异动标记", "板块题材", "盘面信息")):
                start = idx + 1
                break
            if lines[idx:idx + 5] == ["时间", "股票名称", "异动标记", "板块题材", "盘面信息"]:
                start = idx + 5
                break

        reason_by_name: Dict[str, Dict[str, str]] = {}
        time_re = re.compile(r"^\d{2}:\d{2}$")
        stop_prefixes = ("ID ", "连板梯队", "板块题材", "市场风向标", "跌停池", "炸板池")
        i = start
        while i < len(lines):
            line = lines[i]
            if any(line.startswith(prefix) for prefix in stop_prefixes):
                break
            if not time_re.match(line):
                i += 1
                continue
            if i + 3 >= len(lines):
                break
            name = lines[i + 1]
            reason = lines[i + 3]
            detail = lines[i + 4] if i + 4 < len(lines) else ""
            normalized_name = StockDataFetcher._normalize_stock_name(name)
            if normalized_name and reason:
                reason_by_name[normalized_name] = {
                    "reason": reason,
                    "detail": detail,
                }
            i += 5
        return reason_by_name

    def _load_limit_up_reason_map(self, trade_date: str, source: Optional[str] = None) -> Dict[str, Dict[str, str]]:
        date_key = self._normalize_trade_date(trade_date)
        if not date_key:
            return {}
        provider = self.normalize_limit_up_reason_source(source or self._default_limit_up_reason_source)
        cache_key = f"{provider}:{date_key}"
        cached = self._limit_up_reason_cache.get(cache_key)
        if cached is not None:
            return cached

        display_date = self._normalize_trade_date_display(date_key)
        if not display_date:
            self._limit_up_reason_cache[cache_key] = {}
            return {}

        # 严格只接受 4 个带日期参数的 URL；之前还有一个无 date 参数的"兜底"
        # 兜底返回的是复盘网首页（今日数据），但我们的日期校验只在 params 非空
        # 时才生效，导致历史日期的缓存被今日数据污染 → 已移除
        url_candidates = [
            ("https://www.fupanwang.com/fupanla/", {"date": display_date}),
            ("https://www.fupanwang.com/fupanla/", {"date": date_key}),
            ("https://www.fupanwang.com/fupanla/", {"day": display_date}),
            ("https://www.fupanwang.com/fupanla/", {"day": date_key}),
        ]

        last_error: Optional[Exception] = None
        result: Dict[str, Dict[str, str]] = {}
        for url, params in url_candidates:
            try:
                html_text = self._fetch_text_with_optional_proxy_bypass(url, params=params)
                parsed = self._parse_limit_up_reason_page(html_text)
                if not parsed:
                    continue
                # 强制要求响应里出现请求的日期，防止"复盘网静默返回今日数据"
                if display_date not in html_text and date_key not in html_text:
                    continue
                result = parsed
                break
            except Exception as exc:
                last_error = exc
                continue
        if not result and last_error is not None and self._log:
            self._log(f"涨停原因页 {date_key} 获取失败: {last_error}")
        self._limit_up_reason_cache[cache_key] = result
        return result

    def get_limit_up_strong_tag(self, stock_code: str, trade_date: str, source: Optional[str] = None) -> str:
        """返回东方财富强势股池的入选理由，作为强势标签而非涨停归因。"""
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return ""

        reason_text = ""
        try:
            pool = self._load_strong_pool(trade_date, source=source)
            if (pool is not None and not pool.empty
                    and "代码" in pool.columns and "入选理由" in pool.columns):
                match = pool[pool["代码"].astype(str).str.strip().str.zfill(6) == code]
                if not match.empty:
                    raw = str(match.iloc[0].get("入选理由", "") or "").strip()
                    if raw and raw.lower() != "nan":
                        reason_text = raw
        except Exception:
            reason_text = ""

        concepts: List[str] = []
        try:
            import stock_store as _ss
            concepts = _ss.lookup_concepts_by_code(code, limit=8) or []
        except Exception:
            concepts = []

        if reason_text and concepts:
            return f"{reason_text} [{' / '.join(concepts)}]"
        if reason_text:
            return reason_text
        if concepts:
            return f"[{' / '.join(concepts)}]"
        return ""

    def get_limit_up_reason(
        self,
        stock_code: str,
        trade_date: str,
        source: Optional[str] = None,
        stock_name: str = "",
    ) -> str:
        """返回更接近题材/事件归因的涨停原因。"""
        code = str(stock_code or "").strip().zfill(6)
        normalized_name = self._normalize_stock_name(stock_name)
        if not code and not normalized_name:
            return ""
        reason_map = self._load_limit_up_reason_map(trade_date, source=source)
        if normalized_name:
            payload = reason_map.get(normalized_name)
            if payload and payload.get("reason"):
                return str(payload.get("reason") or "").strip()
        return ""

    def enrich_limit_up_reason_fields(
        self,
        records: List[Dict[str, Any]],
        trade_date: str,
        source: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """给涨停记录补 reason / detail / strong_tag 字段。

        reason fallback 链：
        1. 复盘网（fupanwang.com）当日真实涨停原因 → 最准
        2. 概念兜底（stock_concept_tags 表 → 该股 TOP-N 概念）→ 盘中复盘网未更新时用
        3. 空字符串

        ``limit_up_reason_source`` 字段标记数据来自哪条链，便于 GUI 区分展示
        ("fupanwang" / "concepts_fallback" / "")。
        """
        if not records:
            return records
        reason_map = self._load_limit_up_reason_map(trade_date, source=source)
        for rec in records:
            code = str(rec.get("code") or "").strip().zfill(6)
            name = str(rec.get("name") or "")
            normalized_name = self._normalize_stock_name(name)
            payload = reason_map.get(normalized_name, {})
            primary_reason = str(payload.get("reason") or "").strip()

            if "limit_up_reason" not in rec:
                if primary_reason:
                    rec["limit_up_reason"] = primary_reason
                    rec["limit_up_reason_source"] = "fupanwang"
                elif code:
                    # 复盘网无数据（多为盘中场景，复盘网通常盘后 16:00+ 才更新）
                    # 兜底用概念标签拼成 reason，UI 上加 [..] 区分非真实原因
                    try:
                        import stock_store as _ss
                        concepts = _ss.lookup_concepts_by_code(code, limit=5) or []
                    except Exception:
                        concepts = []
                    if concepts:
                        rec["limit_up_reason"] = f"[{' / '.join(concepts)}]"
                        rec["limit_up_reason_source"] = "concepts_fallback"
                    else:
                        rec["limit_up_reason"] = ""
                        rec["limit_up_reason_source"] = ""
                else:
                    rec["limit_up_reason"] = ""
                    rec["limit_up_reason_source"] = ""
            if "limit_up_reason_detail" not in rec:
                rec["limit_up_reason_detail"] = str(payload.get("detail") or "").strip()
            if "strong_tag" not in rec:
                rec["strong_tag"] = self.get_limit_up_strong_tag(code, trade_date)
        return records

    @staticmethod
    def _sanitize_limit_up_pool(
        df: pd.DataFrame,
        *,
        drop_missing_seal_time: bool = True,
    ) -> pd.DataFrame:
        """剔除接口返回的脏数据：涨跌幅 ≤ 0 / 最新价 ≤ 0 / 首封时间 全 0。

        akshare `stock_zt_pool_em` 偶尔会塞进异常行（如 涨跌幅=-100、最新价=0、
        首次封板时间='000000'），这些不是真实涨停股，必须在入库前过滤掉。

        drop_missing_seal_time:
            True（默认）= 东财在线池口径：真实涨停股必有封板时间，空/000000 视为脏数据剔除。
            False = 反推/spot 派生池口径：这些来源本就拿不到封板时间（无分钟数据），
                    空封板时间是正常的，不能据此剔除，否则整池被清空。

        注：pandas 3.0 起 `astype(str)` 不再把缺失值变成 "nan" 字符串，会导致 NaN 与
        空串行为分叉。这里用 `isna()` 显式覆盖缺失值，保证两者口径一致、跨版本稳定。
        """
        if df is None or df.empty:
            return df
        keep_mask = pd.Series(True, index=df.index)
        if "涨跌幅" in df.columns:
            chg = pd.to_numeric(df["涨跌幅"], errors="coerce")
            keep_mask &= chg.fillna(-999) > 0
        if "最新价" in df.columns:
            price = pd.to_numeric(df["最新价"], errors="coerce")
            keep_mask &= price.fillna(-1) > 0
        if drop_missing_seal_time and "首次封板时间" in df.columns:
            seal_raw = df["首次封板时间"]
            seal_str = seal_raw.astype(str).str.strip()
            # 缺失值（NaN/NaT/pd.NA）+ 无效占位串，都算无封板时间
            is_missing = seal_raw.isna() | seal_str.isin(
                ["", "000000", "0", "0000", "nan", "NaN", "None", "<NA>", "NaT"]
            )
            keep_mask &= ~is_missing
        return df[keep_mask].reset_index(drop=True)

    def _fetch_spot_with_fallback(self) -> Optional[pd.DataFrame]:
        """全市场实时行情快照，东财→新浪自动兜底（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.fetch_spot_with_fallback(self, log_fn=self._log)

    def _derive_limit_up_pool_from_spot(
        self,
        trade_date: str,
        prev_pool_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """从全市场 spot 派生今日涨停池（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.derive_limit_up_pool_from_spot(
            self, trade_date, log_fn=self._log, prev_pool_df=prev_pool_df,
        )

    def get_limit_up_pool(self, trade_date: str) -> pd.DataFrame:
        """获取指定日期的涨停板池（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.get_limit_up_pool(self, trade_date, log_fn=self._log)

    def get_previous_limit_up_pool(self, trade_date: str) -> pd.DataFrame:
        """获取指定日期的昨日涨停板池（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.get_previous_limit_up_pool(self, trade_date, log_fn=self._log)

    def get_pool_source(self, date_key: str, *, previous: bool = False) -> str:
        """返回涨停池数据来源（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.get_pool_source(self, date_key, previous=previous)

    def _recent_trade_dates(self, end_date: str, count: int) -> List[str]:
        date_key = self._normalize_trade_date(end_date)
        if not date_key:
            return []
        try:
            cursor = datetime.strptime(date_key, "%Y%m%d").date()
        except ValueError:
            return []

        target = max(1, int(count))
        dates: List[str] = []
        checked = 0
        max_checked = max(target * 7, 20)
        while len(dates) < target and checked < max_checked:
            if cursor.weekday() < 5:
                dates.append(cursor.strftime("%Y%m%d"))
            cursor -= timedelta(days=1)
            checked += 1
        dates.reverse()
        return dates

    def compare_limit_up_pools(
        self,
        today_date: str,
        yesterday_date: str,
    ) -> Dict[str, Any]:
        """对比今日与昨日首次涨停股票的差异（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.compare_limit_up_pools(
            self, today_date, yesterday_date, log_fn=self._log,
        )

    def compare_limit_up_pools_window(
        self,
        today_date: str,
        compare_days: int = 2,
    ) -> Dict[str, Any]:
        """涨停对比窗口聚合（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.compare_limit_up_pools_window(
            self, today_date, compare_days, log_fn=self._log,
        )

    def _pool_to_records(self, df: pd.DataFrame, tag: str) -> List[Dict[str, Any]]:
        """涨停池 DataFrame -> 标准记录列表（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.pool_to_records(df, tag)

    @staticmethod
    def _count_industry(df: pd.DataFrame) -> Dict[str, int]:
        """涨停池行业分布统计（thin delegate -> limit_up_pool_service）。"""
        return _lup_service.count_industry(df)

    def get_stock_concepts(self, stock_code: str) -> str:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return ""
        if self._universe_concepts_cache is None:
            universe_df = _load_universe_store(None)
            if universe_df is not None and not universe_df.empty and "concepts" in universe_df.columns:
                self._set_universe_concepts_cache(universe_df)
            else:
                self._set_universe_concepts_cache(pd.DataFrame())
        cached = self._universe_concepts_cache.get(code, "") if self._universe_concepts_cache else ""
        if cached:
            return _normalize_concepts_text(cached)
        mapped = self._load_concepts_map([code], max_boards=self.concept_board_limit)
        return _normalize_concepts_text(mapped.get(code, ""))

    def get_fund_flow_data(
        self,
        stock_code: str,
        days: int = 30,
        force_refresh: bool = False,
        source: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None
        min_rows = max(1, int(days))
        if not force_refresh:
            cached = _load_fund_flow_store(code, min_rows=min_rows, log=self._log)
            if cached is not None and not cached.empty:
                has_big_order_data = False
                if "big_order_amount" in cached.columns:
                    big_order_series = pd.to_numeric(cached["big_order_amount"], errors="coerce")
                    has_big_order_data = bool(big_order_series.notna().any())
                if has_big_order_data and not _should_refresh_today_row(cached):
                    return cached.tail(days).reset_index(drop=True)
                if not has_big_order_data:
                    if self._log:
                        self._log(f"资金流 {code} 缓存缺少大单净额，自动刷新最新数据。")
                elif self._log:
                    self._log(f"资金流 {code} 命中当天缓存，但尚未收盘，改为刷新最新数据。")
        market = _infer_market(code)
        plan = self.build_fund_flow_request_plan(source or self._default_fund_flow_source)
        flow_df = None
        last_error: Optional[Exception] = None
        for provider in plan.provider_sequence:
            if provider == "eastmoney":
                try:
                    flow_df = _retry_ak_call(ak.stock_individual_fund_flow, stock=code, market=market)
                    break
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"个股资金流 {code} 获取失败: {e}")
            elif provider == "ths":
                try:
                    if self._log:
                        self._log(f"个股资金流 {code} 正在使用同花顺源补位。")
                    flow_df = _fetch_ths_fund_flow_frame(code)
                    if flow_df is not None and not flow_df.empty:
                        flow_df = flow_df.copy()
                        today_text = datetime.now().strftime("%Y-%m-%d")
                        if "日期" not in flow_df.columns and "date" not in flow_df.columns and "交易日" not in flow_df.columns:
                            flow_df["日期"] = today_text
                        break
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"个股资金流 {code} 使用同花顺源失败: {e}")
        if flow_df is None:
            return None
        if flow_df is None or flow_df.empty:
            if last_error is not None and self._log:
                self._log(f"个股资金流 {code} 所有数据源失败: {last_error}")
            return None
        source_columns = [str(col) for col in flow_df.columns.tolist()]
        rename_map: Dict[str, str] = {}

        date_col = _first_existing_column(source_columns, ["日期", "交易日", "date"])
        close_col = _first_existing_column(source_columns, ["收盘价", "收盘", "close"])
        change_pct_col = _first_existing_column(source_columns, ["涨跌幅", "change_pct"])
        main_amount_col = _first_existing_column(source_columns, ["主力净流入-净额", "主力净额"])
        main_ratio_col = _first_existing_column(source_columns, ["主力净流入-净占比", "主力净占比"])
        big_amount_col = _first_existing_column(source_columns, ["大单净流入-净额", "大单净额"])
        big_ratio_col = _first_existing_column(source_columns, ["大单净流入-净占比", "大单净占比"])
        super_amount_col = _first_existing_column(source_columns, ["超大单净流入-净额", "超大单净额"])
        super_ratio_col = _first_existing_column(source_columns, ["超大单净流入-净占比", "超大单净占比"])

        if main_amount_col is None:
            main_amount_col = _find_fund_flow_column(source_columns, ["主力", "净", "额"], excludes=["占比"])
        if big_amount_col is None:
            big_amount_col = _find_fund_flow_column(source_columns, ["大单", "净", "额"], excludes=["占比", "超大单"])
        if super_amount_col is None:
            super_amount_col = _find_fund_flow_column(source_columns, ["超大单", "净", "额"], excludes=["占比"])
        if main_ratio_col is None:
            main_ratio_col = _find_fund_flow_column(source_columns, ["主力", "净", "占比"])
        if big_ratio_col is None:
            big_ratio_col = _find_fund_flow_column(source_columns, ["大单", "净", "占比"], excludes=["超大单"])
        if super_ratio_col is None:
            super_ratio_col = _find_fund_flow_column(source_columns, ["超大单", "净", "占比"])

        for src, dst in [
            (date_col, "date"),
            (close_col, "close"),
            (change_pct_col, "change_pct"),
            (main_amount_col, "main_force_amount"),
            (main_ratio_col, "main_force_ratio"),
            (big_amount_col, "big_order_amount"),
            (big_ratio_col, "big_order_ratio"),
            (super_amount_col, "super_big_order_amount"),
            (super_ratio_col, "super_big_order_ratio"),
        ]:
            if src:
                rename_map[src] = dst

        df = flow_df.rename(columns=rename_map).copy()
        if "date" not in df.columns:
            return None
        if "big_order_amount" not in df.columns and self._log:
            self._log(f"个股资金流 {code} 未匹配到大单净额字段，返回列: {', '.join(source_columns)}")
        df["date"] = df["date"].astype(str).str.strip()
        for col in [
            "close",
            "change_pct",
            "main_force_amount",
            "main_force_ratio",
            "big_order_amount",
            "big_order_ratio",
            "super_big_order_amount",
            "super_big_order_ratio",
        ]:
            if col in df.columns:
                df[col] = df[col].map(_parse_cn_numeric)
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        _save_fund_flow_store(code, df, keep_rows=max(60, days + 10))
        return df.tail(days).reset_index(drop=True)

    def _mark_universe_refreshed(self) -> None:
        """记录一次"全量重新拉取股票池"完成的时间戳。"""
        try:
            save_app_config_store(
                _UNIVERSE_FULL_REFRESH_KEY,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        except Exception:
            pass

    def universe_refresh_overdue(self, max_age_days: Optional[float] = None) -> bool:
        """股票池距上次"全量重新拉取"是否已超过 N 天。缺标记 / 解析失败一律视为 overdue。"""
        threshold = _universe_max_age_days() if max_age_days is None else float(max_age_days)
        try:
            raw = str(load_app_config_store(_UNIVERSE_FULL_REFRESH_KEY, "") or "").strip()
        except Exception:
            raw = ""
        if not raw:
            return True
        try:
            last = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return True
        return (datetime.now() - last).total_seconds() >= threshold * 86400.0

    def get_all_stocks(self, force_refresh: bool = False) -> pd.DataFrame:
        if _use_em_full_spot_for_list():
            df = self._get_all_stocks_em_spot()
            if df is not None and not df.empty:
                self._mark_universe_refreshed()
            return df
        if os.environ.get("ASHARE_SCAN_REFRESH_UNIVERSE", "").strip().lower() in (
            "1",
            "true",
            "yes",
        ):
            force_refresh = True
        if not force_refresh:
            universe_df = _load_universe_store(self._log)
            if universe_df is not None and not universe_df.empty:
                self._set_universe_concepts_cache(universe_df)
                return universe_df
        if self._log:
            self._log(
                "从交易所构建股票池（深交所+上交所含科创板，不含北交所）…"
            )
        df = _build_a_share_universe(self._log)
        if not df.empty:
            _save_universe_store(df, self._log)
            self._set_universe_concepts_cache(df)
            self._mark_universe_refreshed()
        return df

    def _get_all_stocks_em_spot(self) -> pd.DataFrame:
        global _list_download_log
        prev_log = _list_download_log
        _list_download_log = self._log
        try:
            if self._log:
                self._log("已开启 ASHARE_SCAN_LIST_SOURCE=em：东方财富分页全表（耗时长，易限流）…")
            stock_list = _retry_ak_call(ak.stock_zh_a_spot_em)
            stock_list = stock_list.rename(columns={
                "代码": "code",
                "名称": "name",
                "最新价": "price",
                "涨跌幅": "change_pct",
                "涨跌额": "change_amount",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "最高": "high",
                "最低": "low",
                "今开": "open",
                "昨收": "pre_close",
                "量比": "volume_ratio",
                "换手率": "turnover_rate",
                "市盈率-动态": "pe_ratio",
                "市净率": "pb_ratio",
                "总市值": "total_mv",
                "流通市值": "circ_mv",
            })
            if "code" in stock_list.columns:
                stock_list["code"] = _norm_code_series(stock_list["code"])
            if "code" in stock_list.columns:
                stock_list["exchange"] = stock_list["code"].map(
                    lambda x: "上交所"
                    if str(x).startswith(("5", "6", "9"))
                    else "深交所"
                )
                stock_list["board"] = stock_list["code"].map(
                    lambda x: "科创板"
                    if str(x).startswith("688")
                    else _infer_sz_board(x)
                )
            save_universe_store(stock_list)
            self._set_universe_concepts_cache(stock_list)
            if self._log:
                self._log(f"东方财富全表下载完成，共 {len(stock_list)} 条。")
            return stock_list
        except Exception as e:
            if self._log:
                self._log(f"东方财富全表失败: {e}")
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()
        finally:
            _list_download_log = prev_log

    def get_history_data(
        self,
        stock_code: str,
        days: int = 10,
        force_refresh: bool = False,
        preferred_mirror: Optional[str] = None,
        mirror_pool: Optional[List[str]] = None,
        request_plan: Optional[HistoryRequestPlan] = None,
        as_of_trade_date: str = "",
    ) -> Optional[pd.DataFrame]:
        history_df: Optional[pd.DataFrame] = None
        try:
            stock_code = str(stock_code).strip().zfill(6)
            end_date = str(as_of_trade_date or "").strip().replace("-", "")
            if len(end_date) != 8 or not end_date.isdigit():
                end_date = datetime.now().strftime('%Y%m%d')
            min_rows = max(1, days)
            history_meta = None if force_refresh else load_history_meta_store(stock_code)
            cache_requires_repair = _history_meta_requires_repair(history_meta)
            if request_plan is None:
                request_plan = self.build_history_request_plan(source=self._default_history_source, force_refresh=False)

            if not force_refresh:
                # ---- 企业级缓存策略：先查 meta 判断新鲜度，再查数据 ----
                if (not cache_requires_repair) and _is_history_cache_fresh(stock_code, min_rows, self._log):
                    history_df = _load_history_store(stock_code, min_rows, end_date, self._log)
                    if history_df is not None and not history_df.empty:
                        _increment_history_diagnostic("cache_hits")
                        return history_df.tail(days).reset_index(drop=True)

                history_df = _load_history_store(stock_code, min_rows, end_date, self._log)
                if history_df is not None and not history_df.empty:
                    _increment_history_diagnostic("cache_hits")
                    if cache_requires_repair:
                        if self._log:
                            partial = str((history_meta or {}).get("partial_fields") or "").strip()
                            source = str((history_meta or {}).get("source") or "").strip()
                            hint = partial or (f"source={source}" if source else "needs_repair=1")
                            self._log(f"历史 {stock_code} 命中缓存但字段残缺({hint})，继续联网补齐。")
                    elif not _should_refresh_today_row(history_df):
                        return history_df.tail(days).reset_index(drop=True)
                    elif self._log:
                        self._log(f"历史 {stock_code} 命中当天缓存，但尚未收盘，改为刷新最新日线。")

            eastmoney_only = bool(request_plan.provider_sequence) and all(
                provider == "eastmoney" for provider in request_plan.provider_sequence
            )
            if eastmoney_only and self._log_history_access_suspended():
                if history_df is not None and not history_df.empty:
                    _increment_history_diagnostic("fallback_cache_returns")
                    return history_df.tail(days).reset_index(drop=True)
                return None

            if request_plan is not None and request_plan.cache_only:
                if self._log:
                    self._log(f"历史 {stock_code} 使用扫描上下文 cache-only 策略，本次不访问东方财富。")
                if history_df is not None and not history_df.empty:
                    _increment_history_diagnostic("fallback_cache_returns")
                    return history_df.tail(days).reset_index(drop=True)
                return None

            start_date = (datetime.now() - timedelta(days=days + 15)).strftime('%Y%m%d')
            provider_sequence = list(request_plan.provider_sequence) if request_plan is not None else ["eastmoney"]
            if not provider_sequence:
                provider_sequence = ["eastmoney"]

            _PROVIDER_HOST = {
                "sina": "finance.sina.com.cn",
                "netease": "quotes.money.163.com",
                "sohu": "q.stock.sohu.com",
                "ths": "d.10jqka.com.cn",
                "wscn": "api-ddc-wscn.awtmt.com",
                "baostock": "baostock.com",
                "tdx": "tdx",
            }

            last_error: Optional[BaseException] = None
            best_partial: Optional[Tuple[pd.DataFrame, str, str]] = None
            for idx, provider in enumerate(provider_sequence):
                provider_used = provider
                # 跳过正在冷却中的源，避免无意义的调用和日志刷屏
                host = _PROVIDER_HOST.get(provider)
                if host and _global_host_on_cooldown(host):
                    last_error = RuntimeError(f"{provider} on cooldown, skipped")
                    continue

                if provider == "eastmoney":
                    if request_plan is not None:
                        raw_mirror_pool = list(request_plan.mirror_urls)
                    else:
                        raw_mirror_pool = mirror_pool if mirror_pool is not None else self.get_available_history_mirrors()
                    selected_mirrors = [x for x in raw_mirror_pool if x]
                    selected_mirrors = _prioritize_history_mirrors(
                        selected_mirrors,
                        preferred_mirror=preferred_mirror,
                    )
                    if not selected_mirrors:
                        last_error = RuntimeError("eastmoney-no-mirror")
                        continue
                    try:
                        df = _history_retry_ak_call(
                            _fetch_eastmoney_hist_frame,
                            stock_code,
                            days,
                            start_date,
                            end_date,
                            selected_mirrors,
                            self._log,
                        )
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用东财源失败，准备切换备用源: {e}")
                        continue
                    df = _normalize_history_frame(df)
                elif provider == "sina":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用新浪源补位。")
                        with _limit_host_inflight(host, default_limit=2):
                            df = _fetch_sina_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用新浪源失败: {e}")
                        continue
                elif provider == "netease":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用网易源补位。")
                        with _limit_host_inflight(host, default_limit=2):
                            df = _fetch_netease_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用网易源失败: {e}")
                        continue
                elif provider == "sohu":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用搜狐源补位。")
                        with _limit_host_inflight(host, default_limit=2):
                            df = _fetch_sohu_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用搜狐源失败: {e}")
                        continue
                elif provider == "ths":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用同花顺源补位。")
                        with _limit_host_inflight(host, default_limit=1):
                            df = _fetch_ths_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用同花顺源失败: {e}")
                        continue
                elif provider == "wscn":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用华尔街见闻源补位。")
                        with _limit_host_inflight(host, default_limit=2):
                            df = _fetch_wscn_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用华尔街见闻源失败: {e}")
                        continue
                elif provider == "baostock":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用 BaoStock 源补位。")
                        with _limit_host_inflight(host, default_limit=2):
                            df = _fetch_baostock_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用 BaoStock 源失败: {e}")
                        continue
                elif provider == "tdx":
                    try:
                        if self._log:
                            self._log(f"历史 {stock_code} 正在使用通达信源补位。")
                        with _limit_host_inflight(host, default_limit=2):
                            df = _fetch_tdx_hist_frame(stock_code, start_date, end_date)
                    except Exception as e:
                        last_error = e
                        if self._log:
                            self._log(f"历史 {stock_code} 使用通达信源失败: {e}")
                        continue
                else:
                    last_error = RuntimeError(f"unsupported-history-provider: {provider}")
                    continue

                if df is None or df.empty:
                    last_error = RuntimeError(f"{provider}-empty-history")
                    continue

                partial_fields = _history_partial_fields(df)
                needs_repair = int(bool(partial_fields))
                if partial_fields:
                    if self._log:
                        self._log(f"历史 {stock_code} {provider_used} 返回字段残缺({partial_fields})。")
                    if best_partial is None:
                        best_partial = (df, provider_used, partial_fields)
                    if idx < len(provider_sequence) - 1:
                        last_error = RuntimeError(f"{provider_used}-partial-history:{partial_fields}")
                        continue

                _save_history_store(stock_code, df)
                latest_td = _latest_trade_date_from_history_df(df)
                save_history_meta_store(
                    stock_code,
                    latest_td,
                    # 用表里真实总行数，而非本次增量 df 的行数；否则缓存永远判不新鲜、每轮重拉。
                    count_history_store(stock_code) or len(df),
                    source=provider_used,
                    partial_fields=partial_fields,
                    needs_repair=needs_repair,
                )
                return df.tail(days).reset_index(drop=True)

            if best_partial is not None and (history_df is None or history_df.empty):
                df, provider, partial_fields = best_partial
                _save_history_store(stock_code, df)
                latest_td = _latest_trade_date_from_history_df(df)
                save_history_meta_store(
                    stock_code,
                    latest_td,
                    count_history_store(stock_code) or len(df),
                    source=provider,
                    partial_fields=partial_fields,
                    needs_repair=1,
                )
                if self._log:
                    self._log(
                        f"历史 {stock_code} 完整源均失败，暂存 {provider} 残缺缓存({partial_fields})，后续继续修复。"
                    )
                return df.tail(days).reset_index(drop=True)

            if history_df is not None and not history_df.empty:
                if cache_requires_repair and history_meta is not None:
                    latest_td = _latest_trade_date_from_history_df(history_df)
                    partial_fields = str(history_meta.get("partial_fields") or "").strip()
                    if not partial_fields and str(history_meta.get("source") or "").strip().lower() == "tencent":
                        partial_fields = "amount"
                    save_history_meta_store(
                        stock_code,
                        latest_td or str(history_meta.get("latest_trade_date") or ""),
                        count_history_store(stock_code) or len(history_df),
                        source=str(history_meta.get("source") or ""),
                        partial_fields=partial_fields,
                        needs_repair=1,
                        source_failure_streak=history_meta.get("source_failure_streak"),
                    )
                if self._log:
                    self._log(f"历史 {stock_code} 全部数据源失败，回退本地缓存: {last_error}")
                _increment_history_diagnostic("fallback_cache_returns")
                return history_df.tail(days).reset_index(drop=True)
            if last_error is not None:
                raise last_error
            return None
        except Exception as e:
            if not isinstance(e, (EastmoneyRateLimitError, HistoryAccessSuspendedError)):
                _increment_history_diagnostic("network_failures")
            if history_df is not None and not history_df.empty:
                if self._log:
                    self._log(f"历史 {stock_code} 刷新失败，回退本地缓存: {e}")
                _increment_history_diagnostic("fallback_cache_returns")
                return history_df.tail(days).reset_index(drop=True)
            if self._log:
                self._log(f"历史 {stock_code} 获取失败: {e}")
            print(f"获取股票 {stock_code} 历史数据失败: {e}")
            return None

    def get_intraday_data(
        self,
        stock_code: str,
        source: Optional[str] = None,
        day_offset: int = 0,
        target_trade_date: str = "",
        include_meta: bool = False,
    ) -> Any:
        import json as _json
        from stock_store import load_intraday_cache, save_intraday_cache

        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None if not include_meta else _empty_intraday_meta_payload()

        today_str = _today_ymd()
        requested_date = str(target_trade_date or "").strip()
        mem_key = (code, requested_date, int(day_offset or 0))

        def _return(payload: Dict[str, Any]) -> Any:
            return payload if include_meta else payload.get("intraday")

        # ---- L1: 进程内 TTL 缓存（60s）盘中反复进入秒开 ----
        mem_hit = _intraday_mem_get(mem_key)
        if mem_hit is not None:
            return _return(mem_hit)

        # ---- L2: SQLite 缓存——过去日期永久；今日数据收盘后（15:30+）也复用 ----
        sqlite_cacheable_today = (requested_date == today_str
                                  and _intraday_market_closed_now())
        if requested_date and (requested_date < today_str or sqlite_cacheable_today):
            cached = load_intraday_cache(code, requested_date)
            if cached and cached.get("data_json"):
                try:
                    rows = _json.loads(cached["data_json"])
                    intraday_df = pd.DataFrame(rows)
                    if not intraday_df.empty and "time" in intraday_df.columns:
                        intraday_df["time"] = pd.to_datetime(intraday_df["time"], errors="coerce")
                        for col in ["open", "close", "high", "low", "volume", "amount", "avg_price"]:
                            if col in intraday_df.columns:
                                intraday_df[col] = pd.to_numeric(intraday_df[col], errors="coerce")
                        auction_snapshot = None
                        auction_raw = cached.get("auction_json", "")
                        if auction_raw:
                            try:
                                auction_snapshot = _json.loads(auction_raw)
                                if auction_snapshot and "time" in auction_snapshot:
                                    auction_snapshot["time"] = pd.to_datetime(auction_snapshot["time"], errors="coerce")
                            except Exception:
                                auction_snapshot = None
                        payload = {
                            "intraday": intraday_df,
                            "selected_trade_date": requested_date,
                            "available_trade_dates": [requested_date],
                            "applied_day_offset": 0,
                            "auction": auction_snapshot,
                        }
                        _intraday_mem_put(mem_key, payload)
                        return _return(payload)
                except Exception:
                    pass  # 缓存损坏，回退网络

        # ---- 网络获取 ----
        raw = None
        auction_snapshot = None
        last_error: Optional[Exception] = None
        plan = self.build_intraday_request_plan(source or self._default_intraday_source)
        for provider in plan.provider_sequence:
            if provider == "eastmoney":
                try:
                    raw = _retry_ak_call(
                        _fetch_eastmoney_intraday_1min,
                        code,
                        ndays=5,
                        logger=self._log,
                    )
                    try:
                        auction_snapshot = _retry_ak_call(
                            _fetch_eastmoney_auction_snapshot,
                            code,
                            logger=self._log,
                        )
                    except Exception as pre_exc:
                        if self._log:
                            self._log(f"分时行情(东财竞价) {code} 获取失败，继续使用常规分时: {pre_exc}")
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"分时行情(东财) {code} 获取失败: {e}")
            elif provider == "sina":
                try:
                    # 容错封装：被限流(HTTP 456)/非 JSONP 时干净返回空表，
                    # 不再抛 akshare 的 "list index out of range"。
                    from src.sources.sina import fetch_intraday_1min as _fetch_sina_intraday_1min
                    raw = _retry_ak_call(_fetch_sina_intraday_1min, code, logger=self._log)
                except Exception as e:
                    last_error = e
                    if self._log:
                        self._log(f"分时行情(新浪) {code} 获取失败: {e}")
            if raw is not None and not getattr(raw, "empty", True):
                break

        if raw is None or getattr(raw, "empty", True):
            if self._log and last_error is not None:
                self._log(f"分时行情 {code} 无可用数据: {last_error}")
            return None if not include_meta else _empty_intraday_meta_payload()

        df = _normalize_intraday_source_frame(raw, code, logger=self._log)
        if df.empty:
            return None if not include_meta else _empty_intraday_meta_payload()

        trade_dates = _resolve_intraday_trade_dates(df)
        if not trade_dates:
            return None if not include_meta else _empty_intraday_meta_payload()

        selected_trade_date, applied_offset = _select_intraday_trade_date(
            trade_dates,
            day_offset=day_offset,
            target_trade_date=target_trade_date,
        )
        df = _slice_intraday_frame_by_trade_date(df, selected_trade_date)
        if df.empty:
            return None if not include_meta else _empty_intraday_meta_payload(
                selected_trade_date=selected_trade_date,
                available_trade_dates=trade_dates,
                applied_day_offset=applied_offset,
            )

        if auction_snapshot is not None and str(auction_snapshot.get("trade_date") or "") != selected_trade_date:
            auction_snapshot = None

        intraday_df = df[["time", "open", "close", "high", "low", "volume", "amount", "avg_price"]].copy()

        # ---- 缓存写入 ----
        # 过去交易日永久缓存；当日数据要等到 15:30+（收盘清算完）才落 SQLite，
        # 避免盘中"半成品"被永久化
        write_sqlite_today = (selected_trade_date == today_str
                              and _intraday_market_closed_now())
        if selected_trade_date and not intraday_df.empty and (
            selected_trade_date < today_str or write_sqlite_today
        ):
            try:
                save_df = intraday_df.copy()
                save_df["time"] = save_df["time"].astype(str)
                data_json = save_df.to_json(orient="records", force_ascii=False)
                auction_json = ""
                if auction_snapshot and isinstance(auction_snapshot, dict):
                    save_auction = dict(auction_snapshot)
                    if "time" in save_auction:
                        save_auction["time"] = str(save_auction["time"])
                    auction_json = _json.dumps(save_auction, ensure_ascii=False, default=str)
                save_intraday_cache(code, selected_trade_date, data_json, auction_json, len(intraday_df))
            except Exception:
                pass  # 缓存写入失败不影响正常流程

        # 构造返回 payload + 写进程内 TTL 缓存（盘中也缓存，下次访问 60s 内秒开）
        payload = _empty_intraday_meta_payload(
            selected_trade_date=selected_trade_date,
            available_trade_dates=trade_dates,
            applied_day_offset=applied_offset,
            auction_snapshot=auction_snapshot,
        )
        payload["intraday"] = intraday_df
        if not intraday_df.empty:
            _intraday_mem_put(mem_key, payload)
        return _return(payload)

    def get_auction_snapshot(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取当日 09:25 集合竞价撮合快照，东财失败时尝试新浪分时兜底。"""
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return None
        try:
            snapshot = _retry_ak_call(
                _fetch_eastmoney_auction_snapshot,
                code,
                logger=self._log,
            )
            if snapshot:
                out = dict(snapshot)
                out.setdefault("source", "eastmoney")
                return out
        except Exception as exc:
            if self._log:
                self._log(f"竞价数据东财源失败 {code}: {exc}")

        try:
            raw = _retry_ak_call(
                _fetch_sina_intraday_1min,
                code,
                logger=self._log,
            )
            snapshot = _snapshot_from_intraday_frame(raw, stock_code=code, source="sina")
            if snapshot:
                if self._log:
                    self._log(f"竞价数据 {code} 使用新浪09:25分时兜底。")
                return snapshot
        except Exception as exc:
            if self._log:
                self._log(f"竞价数据新浪兜底失败 {code}: {exc}")
        return None

    def prewarm_intraday_for_codes(
        self,
        codes: List[str],
        *,
        ndays: int = 5,
        max_workers: int = 4,
        cancel_check: Optional[Callable[[], bool]] = None,
        progress_cb: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, int]:
        """对多只股票预热分时缓存——每只一次网络拉 ndays 天，按日切片写缓存。

        与重复 N 次 get_intraday_data 相比：单次东财调用即获 5 日全部数据，
        切片后批量入 mem cache + SQLite（过去日期 / 收盘后今日），效率高出 5x。

        - codes: 候选代码列表（自动去重 / zfill 6 位）
        - ndays: 拉取最近 N 个交易日，eastmoney trends2 API 支持 1~5
        - cancel_check: 取消令牌检查，返回 True 即终止后续股票
        - progress_cb: 进度回调 (done, total, current_code)
        """
        import json as _json
        from stock_store import save_intraday_cache
        from concurrent.futures import ThreadPoolExecutor, as_completed

        seen: List[str] = []
        seen_set: set = set()
        for c in codes or []:
            c = str(c or "").strip().zfill(6)
            if c and c not in seen_set:
                seen.append(c)
                seen_set.add(c)
        if not seen:
            return {"total": 0, "done": 0, "failed": 0}

        today_str = _today_ymd()
        can_persist_today = _intraday_market_closed_now()

        def _process_one(code: str) -> bool:
            if cancel_check and cancel_check():
                return False
            try:
                raw = _retry_ak_call(
                    _fetch_eastmoney_intraday_1min, code,
                    ndays=ndays, logger=None,
                )
            except Exception as exc:
                logger.debug("分时预热 %s 抓取失败: %s", code, exc)
                return False
            if raw is None or getattr(raw, "empty", True):
                return False
            df = _normalize_intraday_source_frame(raw, code, logger=None)
            if df.empty:
                return False
            trade_dates = _resolve_intraday_trade_dates(df)
            if not trade_dates:
                return False

            # 当日有效的 auction snapshot
            auction_snapshot = None
            if today_str in trade_dates:
                try:
                    auction_snapshot = _retry_ak_call(
                        _fetch_eastmoney_auction_snapshot, code, logger=None,
                    )
                except Exception:
                    auction_snapshot = None
                if auction_snapshot is not None and str(auction_snapshot.get("trade_date") or "") != today_str:
                    auction_snapshot = None

            latest_td = trade_dates[-1]
            for td in trade_dates:
                day_df = _slice_intraday_frame_by_trade_date(df, td)
                if day_df.empty:
                    continue
                cols = ["time", "open", "close", "high", "low", "volume", "amount", "avg_price"]
                cols = [c for c in cols if c in day_df.columns]
                slim = day_df[cols].copy()
                payload = {
                    "intraday": slim,
                    "selected_trade_date": td,
                    "available_trade_dates": list(trade_dates),
                    "applied_day_offset": 0,
                    "auction": auction_snapshot if td == today_str else None,
                }
                # 显式日期入口
                _intraday_mem_put((code, td, 0), payload)
                # GUI 默认入口 target_trade_date="" → 用最新一天覆盖
                if td == latest_td:
                    _intraday_mem_put((code, "", 0), payload)

                # SQLite 持久化：过去日期 / 收盘后今日
                can_save = (td < today_str) or (td == today_str and can_persist_today)
                if can_save:
                    try:
                        save_df = slim.copy()
                        save_df["time"] = save_df["time"].astype(str)
                        data_json = save_df.to_json(orient="records", force_ascii=False)
                        auction_json = ""
                        if td == today_str and auction_snapshot:
                            save_auction = dict(auction_snapshot)
                            if "time" in save_auction:
                                save_auction["time"] = str(save_auction["time"])
                            auction_json = _json.dumps(save_auction, ensure_ascii=False, default=str)
                        save_intraday_cache(code, td, data_json, auction_json, len(slim))
                    except Exception:
                        pass
            return True

        done = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = {pool.submit(_process_one, c): c for c in seen}
            for fut in as_completed(futs):
                if cancel_check and cancel_check():
                    break
                code = futs[fut]
                try:
                    ok = fut.result()
                except Exception as exc:
                    logger.debug("分时预热 worker %s 异常: %s", code, exc)
                    ok = False
                done += 1
                if not ok:
                    failed += 1
                if progress_cb:
                    try:
                        progress_cb(done, len(seen), code)
                    except Exception:
                        pass
        return {"total": len(seen), "done": done, "failed": failed}

    def prewarm_fund_flow_for_codes(
        self,
        codes: List[str],
        *,
        days: int = 30,
        max_workers: int = 4,
        cancel_check: Optional[Callable[[], bool]] = None,
        progress_cb: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, int]:
        """对多只股票预热资金流缓存——走 get_fund_flow_data 让 SQLite 入库。"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        seen: List[str] = []
        seen_set: set = set()
        for c in codes or []:
            c = str(c or "").strip().zfill(6)
            if c and c not in seen_set:
                seen.append(c)
                seen_set.add(c)
        if not seen:
            return {"total": 0, "done": 0, "failed": 0}

        def _process_one(code: str) -> bool:
            if cancel_check and cancel_check():
                return False
            try:
                df = self.get_fund_flow_data(code, days=days, force_refresh=False)
                return df is not None and not df.empty
            except Exception as exc:
                logger.debug("资金流预热 %s 抓取失败: %s", code, exc)
                return False

        done = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = {pool.submit(_process_one, c): c for c in seen}
            for fut in as_completed(futs):
                if cancel_check and cancel_check():
                    break
                code = futs[fut]
                try:
                    ok = fut.result()
                except Exception as exc:
                    logger.debug("资金流预热 worker %s 异常: %s", code, exc)
                    ok = False
                done += 1
                if not ok:
                    failed += 1
                if progress_cb:
                    try:
                        progress_cb(done, len(seen), code)
                    except Exception:
                        pass
        return {"total": len(seen), "done": done, "failed": failed}
