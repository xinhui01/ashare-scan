# 保留涨停 5 个子类别新增"最优分数段"显示

- 创建日期：2026-05-20
- 状态：待实施
- 范围：仅改造涨停预测 tab → 保留涨停候选 sub-tab 顶部的统计信息区；不动后端服务、不动其他类别

## 背景

涨停预测页 → 保留涨停候选 sub-tab 当前顶部展示 3 行信息：

1. 主行（灰）：`保留涨停 · 昨日命中率 X% · 近20日 X% · 平均次日涨幅 X%`
2. 副行（橙）：`历史最优段: lo-hi 命中率 X%` —— 仅 cont **整体**维度
3. 子类别条（5 列横排）：`1进2: 昨X% | 近20d X% [2进3] ...`

问题：副行的"最优分数段"只反映 cont 整体偏好，而**不同连板数下的最优分数段往往差异很大**（例如 4 进 5 可能 80-90 最稳，1 进 2 反而 50-60 最稳）。用户做 1 进 2 决策时，需要的是"在 1 进 2 这个子类别里，哪个分数段历史最稳"，而不是整体最优段。

## 目标

让每个子类别（1进2 / 2进3 / 3进4 / 4进5 / 5进6+）都显示**自己的"最优分数段"**，给用户的实际操作（按连板数选股 + 看分数段过滤）提供更直接的指引。

非目标：
- 不改 first / fresh / wrap / trend 4 个主类别 sub-tab（它们没有子类别拆分，不存在这个需求）
- 不改 cont 整体的主行 / 副行（"历史最优段"作为 cont 整体的参考保留）
- 不改后端数据模型 / 查询服务

## 设计原则

1. **零后端改动**：复用现有 `prediction_accuracy_service.get_score_bucket_rates(category="cont_1to2")` —— `_load_recent_rows` 已支持子类别字符串作为 category 过滤
2. **worker 线程内取数据**：5 次额外查询放到现有 `_refresh_predict_accuracy_async` 的 worker thread，UI 不卡
3. **视觉降密度**：把原来横向 5 列等宽塞 2 个百分比的"小字带状栏"，改成纵向 5 行 × 3 列的对齐表格，留出"最优分数段"列
4. **样本不足容错**：复用现有 `min_samples=5` 阈值，buyable 不足时显示 `-（样本不足）`，避免噪声

## 设计：UI 改造（仅 cont sub-tab）

### 改造前
```
[行1 灰]   保留涨停 · 昨日命中率 X% · 近20日 X% · 平均次日涨幅 X%
[行2 橙]   历史最优段: 78-90 命中率 58%
[行3 5列]  1进2:昨X%|近X%   2进3:昨X%|近X%   3进4:...   4进5:...   5进6+:...
```

### 改造后
```
[行1 灰]   保留涨停 · 昨日命中率 X% · 近20日 X% · 平均次日涨幅 X%
[行2 橙]   历史最优段: 78-90 命中率 58%   （整体维度，保留）

[行3 表头, 灰]                  昨日           近20d           最优分数段
[行4-8 数据]    1进2:           60% (3/5)     42% (8/19)     65-75: 50%  🟢
                 2进3:           50% (2/4)     38% (5/13)     55-65: 45%  🟡
                 3进4:           -             33% (3/9)      -（样本不足）
                 4进5:           -             -             -
                 5进6+:          -             -             -
```

### 视觉规则

- 子类别表格用 `ttk.Frame` + `grid` 布局，5 行 × 4 列（label / 昨日 / 近20d / 最优段）
- 每个数据 cell 用独立 `ttk.Label`，便于按颜色阈值单独 configure
- **最优段颜色编码**（仅"最优分数段"列着色，主行 / 副行不动）：
  - rate ≥ 40%：绿（前景 `#1b5e20`，加色块 emoji 🟢 或纯文字"高"）
  - 25% ≤ rate < 40%：黄（前景 `#9c7a00`，色块 🟡）
  - rate < 25%：红（前景 `#c62828`，色块 🔴）
  - 样本不足或无数据：灰（前景 `#888`），文案 `-（样本不足）` / `-`
- 字号：表头小字（默认字体 -1），数据行用默认字体
- 列宽：靠 `grid` 自动对齐，必要时用 `sticky="ew"` + 显式 `column.configure(minsize=...)` 避免文字挤一起

## 设计：数据流（worker 线程内取数）

修改 `_refresh_predict_accuracy_async` 的 worker，在已有 5 类（cont/first/fresh/wrap/trend）bucket 查询外，**追加** 5 个子类别（cont_1to2/cont_2to3/cont_3to4/cont_4to5/cont_5plus）查询：

```python
for cat in ("cont", "first", "fresh", "wrap", "trend",
            "cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"):
    try:
        bucket_rates_by_cat[cat] = prediction_accuracy_service.get_score_bucket_rates(
            category=cat, lookback_dates=20, min_samples=5,
        )
    except Exception:
        bucket_rates_by_cat[cat] = {}
```

结果传到 `_apply_predict_accuracy` → `_refresh_predict_best_bucket_labels` 的同一缓存 `self._predict_bucket_rates_cache`。新增 helper：

```python
def _find_best_bucket(self, cat: str) -> Optional[Tuple[Tuple[int, int], Dict[str, Any]]]:
    """从缓存里读 cat 的 bucket rates，取 eligible=True 中 rate 最大的桶。
    无 eligible 桶时返回 None。"""
    rates = self._predict_bucket_rates_cache.get(cat) or {}
    best = None
    for (lo, hi), info in rates.items():
        if not info.get("eligible"):
            continue
        if best is None or info["rate"] > best[1]["rate"] or (
            info["rate"] == best[1]["rate"] and lo > best[0][0]
        ):
            best = ((lo, hi), info)
    return best
```

复用现有 `_refresh_predict_best_bucket_labels` 的逻辑算 5 个主类别的最优段；新增 `_refresh_predict_subcategory_best_buckets`（或合并入上面方法）算 5 个子类别的最优段并刷新对应 Label。

## 文件变更清单

| 文件 | 改动 | 估算行数 |
|---|---|---|
| `stock_gui.py` | `setup_predict_tab` 内 cont sub-tab 的子类别 frame 重排：从 `ttk.Frame + 横向 pack` 改成 `ttk.Frame + grid 5×4`；新增表头 Label；新增 5×1 个"最优段"Label 引用挂到 `self._predict_subcategory_best_labels: Dict[str, ttk.Label]` | ~80 |
| `stock_gui.py` | `_refresh_predict_accuracy_async` worker：bucket_rates 循环里追加 5 个子类别 cat | ~10 |
| `stock_gui.py` | `_refresh_predict_best_bucket_labels`：调用新增 helper 刷新子类别最优段 Label；或新建 `_refresh_subcategory_best_buckets` 方法 | ~50 |
| `stock_gui.py` | `_apply_predict_accuracy` 中渲染子类别那段：`昨日 / 近20d` 文本从原来的拼接字符串改为分别写入对应 grid cell Label | ~30 |
| tests/ | 暂无：子类别"最优段"逻辑复用现有 `get_score_bucket_rates` + `_find_best_bucket` 等同形式，纯 GUI 重排难做单元测试 | 0 |

预计总改动 ~170 行（含适配/旧代码删除）。

## 验收标准

1. **GUI 启动**：5 秒内无 traceback；保留涨停 sub-tab 顶部按设计呈现 8 行布局
2. **数据正确**：5 个子类别每个的"最优分数段"等于直接调用 `get_score_bucket_rates(category="cont_1to2")` 等的最优 eligible 桶（rate 最高，同 rate 取高分段）
3. **颜色编码生效**：随机找 1 个 ≥40% 的子类别看到绿色，1 个 <25% 看到红色（如样本不足以触发，至少手动 mock 数据验证一次）
4. **样本不足显示**：有子类别 buyable < 5 时显示 `-（样本不足）`，颜色为灰
5. **其他 sub-tab 不受影响**：first / fresh / wrap / trend 的顶部布局和数据维持原样
6. **pytest 全绿**：保持 274 passed（本次不新增测试，但已有测试不应被破坏）
7. **性能**：worker 线程多 5 次 DB 查询，整体响应时间增加 < 200ms（用 `time.perf_counter` 简单测一次）

## 错误处理

- `get_score_bucket_rates(category="cont_1to2")` 抛异常：worker 内 try/except 兜底为空 dict，UI 显示 `-`
- 缓存为空（首次启动、`evaluate_all_pending` 还没跑完）：UI 显示 `-`，等 worker 完成自动刷新
- `_predict_bucket_rates_cache` 没有某子类别 key：`_find_best_bucket` 返回 None，对应 Label 显示 `-`

## 不在范围

- 不改 first / fresh / wrap / trend 4 个 sub-tab 的顶部布局
- 不改 cont 主行 / 副行（保留现有显示）
- 不重设计候选表格本身（行高、列宽、行颜色 tag 不动）
- 不新增"最优段"以外的子类别维度（不加"成功分布""平均涨幅"等）
- 不动 `prediction_accuracy_service` 后端任何函数
- 不动 SQLite schema

## 后续可扩展（不在本 spec）

- first / fresh / wrap / trend 4 个 sub-tab 若日后引入子拆分（如按涨幅段、按行业段），可复用同一布局模式
- "最优段"可以扩展显示"次优段"前 2 名（如 `65-75 50% / 75-85 45%`），但当前需求不需要
- 增加 sparkline 显示每个子类别命中率随时间变化
