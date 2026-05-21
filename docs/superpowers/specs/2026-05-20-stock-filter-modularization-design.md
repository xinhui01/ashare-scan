# stock_filter.py 模块化拆分设计

- 创建日期：2026-05-20
- 状态：待实施
- 范围：把 4539 行 / 217KB 的单体 `stock_filter.py`（含 `StockFilter` god class + ~80 方法）拆成按业务职责组织的 `src/services/scoring/` 子模块 + `src/services/scanning/` 扫描编排，`stock_filter.py` 退化为薄 facade

## 背景

stock_gui.py 5-phase 拆分完成后，剩下最大的债务文件就是 `stock_filter.py`：
- 4539 行 / 217KB
- 1 个 `StockFilter` god class
- ~80 个方法横跨 10 个功能组（设置、扫描、详情、涨停分类、profile 分析、predict 编排、5 个 scorer、共享 helper 等）

这是项目业务逻辑的核心，**所有评分/预测/扫描算法都堆在一起**，导致：
1. 单文件超过 IDE / 主 agent / sub-agent 的舒适阅读上限
2. 修一个 scorer 需要在 4500 行里寻位
3. 单元测试难以聚焦（评分逻辑和 scan/detail 混在一起）
4. 跨 scorer 复用的共享 helper（theme/capital flow/vol baseline）和私有 helper 混淆

类比之前完成的 stock_gui.py（6630 行 god class → 5 个 tabs 类 + 1 个薄 orchestrator），本次给 stock_filter.py 做相同性质的模块化。

## 目标

1. **每个 scorer 一个文件**：`src/services/scoring/{cont,first,fresh,wrap,trend,first_board}.py`，每个 ≤ 1500 行
2. **共享层独立**：`scoring/helpers.py`（模块级 `_count_historical_*`）+ `scoring/shared.py`（theme_bonus / capital_flow_bonus / vol_ratio_with_baseline 等跨 scorer 复用）
3. **predict 主编排独立**：`scoring/predict.py` 含 `predict_limit_up_candidates` + 编排器
4. **scan 编排独立**：`src/services/scanning/orchestrator.py` 含 `scan_all_stocks` + 相关 helper
5. **classifier 独立**：`scoring/classifiers.py` 含 `classify_limit_up_pattern` + `classify_limit_up_pool`
6. **profile 独立**：`scoring/profile.py` 含 `analyze_pre_limit_up_profile` + helper
7. **`stock_filter.py` 退化为 facade**：StockFilter 主类继续存在（外部 import 不变），但方法体 thin delegate 到各 scoring 模块
8. **零行为回归**：pytest 318 全程不下降，GUI 启动 OK

非目标：
- 不重设计评分算法（行为完全一致）
- 不引入新评分维度（这是后续 spec 的事）
- 不动 `src/services/prediction_accuracy_service.py`（accuracy 评估服务）
- 不动 SQLite schema
- 不动 stock_gui.py 内任何代码（前一轮 5-phase 拆分已完成）
- 不引入 mypy / 严格类型（沿用现有风格）
- 不新增单元测试（行为不变靠现有 318 个测试守底；新单测留作后续工程）

## 设计原则

1. **Scorer 类化（可选）**：每个 scorer 既可以是模块级函数（如 `score_cont(rec, ctx) -> Dict`），也可以是类（如 `class ContScorer:`）。**推荐函数式**，因为：
   - 评分逻辑无状态
   - 上下文（fetcher / log / hot_industries / compare_context）以 dict / dataclass 参数传入
   - 单测无需 mock 实例
2. **共享 helper 走模块级函数**：theme_bonus / capital_flow_bonus / vol_ratio_with_baseline 等没有状态依赖
3. **StockFilter 主类只做编排**：保留少量编排方法（`predict_limit_up_candidates` / `scan_all_stocks` / `get_stock_detail`），方法体内调 scoring 模块的函数
4. **保留 `self.fetcher` 入口**：所有需要数据访问的 scoring 函数接收 `fetcher` 作为参数（而非 self.fetcher）
5. **保留 `set_log_callback` / `apply_settings` 等公开 API**：外部调用方（stock_gui / main / 测试）不需要改 import
6. **不开 feature branch**，直接 main 提交

## 目标目录结构

```
src/services/
├── (现有 existing files 不动)
├── prediction_accuracy_service.py   (existing, 不动)
├── concept_hype_service.py          (existing, 不动)
├── ...
├── scoring/
│   ├── __init__.py
│   ├── helpers.py              ← P1: _count_historical_* 模块级 helper
│   ├── shared.py               ← P2: theme_bonus / capital_flow_bonus / vol_ratio_with_baseline
│   ├── classifiers.py          ← P3: classify_limit_up_pattern / classify_limit_up_pool
│   ├── profile.py              ← P4: extract_pre_limit_up_features / analyze_pre_limit_up_profile
│   ├── cont.py                 ← P5: score_continuation
│   ├── first.py                ← P6: score_followthrough_candidate（二波接力）
│   ├── fresh.py                ← P7: score_fresh_first_board
│   ├── wrap.py                 ← P8: score_broken_board_wrap
│   ├── trend.py                ← P9: score_trend_limit_up
│   ├── first_board.py          ← P10: score_first_board_by_profile + lhb/northbound helpers
│   └── predict.py              ← P11: predict_limit_up_candidates 主编排
└── scanning/
    ├── __init__.py
    └── orchestrator.py         ← P12: scan_all_stocks + 相关 helper

stock_filter.py                  ← P13: 退化为 thin facade（StockFilter 类保留，方法 delegate）
```

注：phase 编号 P1-P13，与 stock_gui 5-phase 区分。

## 13 phase 概览

每个 phase = 一个 commit + 独立 spec compliance review + 代码质量 review。

| Phase | 改造 | 难度 | 估算行数 | 输出文件 |
|---|---|---|---|---|
| **P1** | 模块级 helper（`_count_historical_*`）+ scoring 包骨架 | 低 | 模板 + ~150 行迁移 | `scoring/helpers.py` |
| **P2** | 共享 helper（theme/capital flow/vol baseline） | 低 | ~250 行 | `scoring/shared.py` |
| **P3** | classifier（涨停分类） | 中 | ~350 行 | `scoring/classifiers.py` |
| **P4** | profile（pre-limit-up 分析） | 中 | ~300 行 | `scoring/profile.py` |
| **P5** | cont scorer（保留涨停） | 中 | ~290 行 | `scoring/cont.py` |
| **P6** | first scorer（二波接力） | 中 | ~480 行 | `scoring/first.py` |
| **P7** | fresh scorer（首板涨停） | 中 | ~270 行 | `scoring/fresh.py` |
| **P8** | wrap scorer（反包/承接） | 中高 | ~360 行 | `scoring/wrap.py` |
| **P9** | trend scorer（趋势涨停） | 中 | ~300 行 | `scoring/trend.py` |
| **P10** | first_board scorer + lhb/northbound helpers | 中高 | ~430 行 | `scoring/first_board.py` |
| **P11** | predict 主编排 | 高 | ~500 行 | `scoring/predict.py` |
| **P12** | scan 编排 | 高 | ~600 行 | `scanning/orchestrator.py` |
| **P13** | stock_filter.py 退化为 facade | 低（收尾） | ~250 行 | `stock_filter.py` |

总计 ~4280 行迁移，主类剩 ~250 行 facade。

## Phase 依赖

P1 → P2（共享 helper 复用 _count_historical_*）→ 
{P3, P4}（独立可任意顺序）→ 
{P5, P6, P7, P8, P9, P10}（5 个 scorer 各自依赖 P1+P2，可独立完成）→ 
P11（predict 编排，依赖所有 scorer）→ 
P12（scan 编排，独立）→ 
P13（facade 收尾）

## Tab 类与 Scorer 模式对比

stock_gui 用了 **Tab 类持有 app 引用**模式（state-ful）。这里 scorer 应该是 **模块级函数 + 参数注入** 模式（stateless），因为：
- scorer 没有自己的 widget / state
- 输入是 `(rec, hot_industries, compare_context, fetcher, log_fn) → Dict`
- 输出是评分 dict
- 重复运行幂等

签名示例：
```python
# src/services/scoring/cont.py
def score_continuation(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    *,
    fetcher,  # StockDataFetcher
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_fn: Optional[Callable[[str, str], float]] = None,
    local_cache_history_plan: Optional[Any] = None,
) -> Dict[str, Any]:
    """对涨停股进行连板延续评分。满分100。"""
    ...
```

主类 `StockFilter._score_continuation` 退化为 thin delegate：
```python
def _score_continuation(self, rec, hot_industries):
    from src.services.scoring.cont import score_continuation
    return score_continuation(
        rec, hot_industries,
        fetcher=self.fetcher,
        log_fn=self._log,
        limit_up_threshold_fn=self._limit_up_threshold,
        local_cache_history_plan=self._build_local_cache_history_plan("predict-continuation-cache-only"),
    )
```

## 错误处理

- 迁移过程中如果发现某 helper 实际被多 scorer 共用（spec 漏判定为单 scorer 私有）→ 升级为 `shared.py`，在 spec/plan 现场记录
- 评分函数签名变化（参数注入）可能影响测试 mock 路径，需要同步更新
- 跨 scorer 引用（如 cont scorer 调用 fresh scorer 的 helper）→ 后引入者通过 `from .first_board import xxx` 显式 import

## 测试策略

- 每个 phase 完成后跑 `pytest -q` 必须 318 passed 不下降
- 关键测试文件不动：`test_scoring.py` / `test_strong_followthrough.py` / `test_limit_up_prediction.py` / `test_historical_pattern_count.py`
- 现有测试通过 `StockFilter` 接口测，scorer 迁移后接口不变，所以测试应自然通过
- 不新增 scoring 模块的独立单测（留作后续工程）
- 每个 phase 完成后做 import check：`python -c "from stock_filter import StockFilter; ..."` + `python -c "from src.services.scoring.{module} import {fn}"`

## 验收标准

1. **文件大小**：每个 `scoring/*.py` ≤ 1500 行；`scanning/orchestrator.py` ≤ 1000 行；`stock_filter.py` ≤ 300 行（facade）
2. **pytest** 全 phase 后保持 318 passed
3. **import 兼容**：`from stock_filter import StockFilter` / `from stock_filter import _count_historical_continuation` 等老调用方仍能正常 import
4. **GUI 启动** 8s 无 traceback
5. **行为零回归**：5 个 scorer 评分结果完全一致（通过 pytest 守底）
6. **依赖正确性**：grep 全代码库，确认无 `stock_filter` 内部互引用循环（scoring/predict.py → 5 个 scorer，反向不引用）

## 不在范围

- 不动 stock_gui.py 任何代码
- 不动 stock_data.py / stock_store.py（留作后续 phase）
- 不引入新评分维度
- 不新增独立单测
- 不动 src/services 现有其他模块

## 后续工程（不在本 spec）

- stock_data.py / stock_store.py 收尾迁移
- 业务逻辑从 GUI 抽出单测覆盖
- P5.5 拆 predict.py 子文件（stock_gui 那边）
- 性能优化（并发预热 / 缓存策略）
