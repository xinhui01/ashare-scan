# stock_data.py 收尾: 抽涨停池服务到 src/sources/limit_up_pool_service.py

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** 把 `stock_data.py` 中涨停池相关方法（StockDataFetcher 类内 ~700 行）+ 2 个模块级 intraday 派生 helper 集体抽到新文件 `src/sources/limit_up_pool_service.py`。stock_data.py 保留 StockDataFetcher 同名 thin delegate。

**Architecture:** 1 commit。模式同 stock_filter P3-P13：模块级函数 + 参数注入。

**模板：** `src/services/scoring/classifiers.py` / `src/services/scoring/predict.py`（前面 stock_filter 拆分工作的成果）

---

## Task 0 — 基线

- pytest 318 passed
- stock_data.py 当前 2488 行

---

## Task 1 — 创建 src/sources/limit_up_pool_service.py

**目标方法（约 700 行）：**

| 旧（位置）| 新（模块级函数）|
|---|---|
| `_derive_seal_time_from_intraday` (module-level 524) | `derive_seal_time_from_intraday` |
| `_count_intraday_breaks` (module-level 557) | `count_intraday_breaks` |
| `StockDataFetcher._fetch_spot_with_fallback` (1476) | `fetch_spot_with_fallback(fetcher, *, log_fn)` |
| `StockDataFetcher._derive_limit_up_pool_from_spot` (1508) | `derive_limit_up_pool_from_spot(fetcher, trade_date, *, log_fn, prev_pool_df=None)` |
| `StockDataFetcher.get_limit_up_pool` (1625) | `get_limit_up_pool(fetcher, trade_date, *, log_fn=None, ...)` |
| `StockDataFetcher.get_previous_limit_up_pool` (1706) | `get_previous_limit_up_pool(fetcher, trade_date, *, log_fn=None, ...)` |
| `StockDataFetcher.get_pool_source` (1770) | `get_pool_source(fetcher, date_key, *, previous=False)` |
| `StockDataFetcher.compare_limit_up_pools` (1808) | `compare_limit_up_pools(fetcher, today_date, yesterday_date, *, log_fn, ...)` |
| `StockDataFetcher.compare_limit_up_pools_window` (1930) | `compare_limit_up_pools_window(fetcher, today_date, compare_days, *, log_fn, ...)` |
| `StockDataFetcher._pool_to_records` (2005) | `pool_to_records(df, tag)` |
| `StockDataFetcher._count_industry` (2042) | `count_industry(df)` (already @staticmethod) |

注意：
- `fetch_spot_with_fallback` 等需要访问 fetcher._log / fetcher._limit_up_pool_cache / fetcher._prev_limit_up_pool_cache / fetcher._last_pool_source / fetcher._last_prev_pool_source / fetcher._recent_trade_dates 等。**仍通过 fetcher 参数访问 self.xxx**（不强求纯函数）
- 对模块级 `_eastmoney_circuit_breaker_open` / `_retry_ak_call` 等保持 import from stock_data

---

## Task 2 — stock_data.py 改 thin delegate

顶部 import：
```python
from src.sources import limit_up_pool_service as _lup_service
```

替换 11 个方法/函数为 thin delegate（保持原签名）：

```python
# 模块级（不在类内）
_derive_seal_time_from_intraday = _lup_service.derive_seal_time_from_intraday
_count_intraday_breaks = _lup_service.count_intraday_breaks

# StockDataFetcher 内
def _fetch_spot_with_fallback(self) -> Optional[pd.DataFrame]:
    return _lup_service.fetch_spot_with_fallback(self, log_fn=self._log)

def _derive_limit_up_pool_from_spot(self, trade_date, *, prev_pool_df=None):
    return _lup_service.derive_limit_up_pool_from_spot(
        self, trade_date, log_fn=self._log, prev_pool_df=prev_pool_df,
    )

def get_limit_up_pool(self, trade_date):
    return _lup_service.get_limit_up_pool(self, trade_date, log_fn=self._log)

# ... 等
```

---

## Task 3 — 验证 + commit

- pytest 318 不下降（特别 test_limit_up_pool_fallback / test_intraday_derive / test_limit_up_prediction）
- import check：`from src.sources.limit_up_pool_service import get_limit_up_pool` + `from stock_data import StockDataFetcher` 同时 OK
- GUI 6s
- 行数：stock_data.py ~2488 → ~1800，limit_up_pool_service.py ~700
- **1 commit**："重构（stock_data 收尾）：抽涨停池服务到 src/sources/limit_up_pool_service.py"

## 风险点

1. **stock_filter / 各 scorer 通过 `self.fetcher.get_limit_up_pool(...)` 等访问**：fetcher.get_limit_up_pool 仍可用（thin delegate），无需改动
2. **fetcher 的 _limit_up_pool_cache / _prev_limit_up_pool_cache / _last_pool_source 字典访问**：在新模块函数内通过 `fetcher._limit_up_pool_cache` 访问
3. **`_eastmoney_circuit_breaker_open` 在 stock_data 模块级**：在新模块内 `from stock_data import _eastmoney_circuit_breaker_open, _retry_ak_call`
4. **`_pool_to_records` 和 `_count_industry` 内部互相调**：迁完直接调本模块同名函数
5. **不要 push**
