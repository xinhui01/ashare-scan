# 二波接力 / 反包候选新增"历史同类形态命中"加分设计

- 创建日期：2026-05-20
- 状态：待实施
- 范围：在 `_score_followthrough_candidate`（二波接力）和 `_score_broken_board_wrap`（反包/承接）评分函数里，新增"近 90 日同类形态成功命中次数"加分维度

## 背景

涨停预测当前 5 个主类别（cont/first/fresh/wrap/trend）的评分都基于"今日 K 线特征 + 评分调节因子"。但有一个直觉上很强的信号没纳入：**这只股票过去是否真的做出过相同形态？**

举例：
- A 股近 90 日有 3 次"涨停 → 5 日内再涨停"（成功二波接力），B 股 0 次 —— A 显然更可能再次接力
- C 股近 90 日有 2 次"涨停→被打回→反包再涨停"（成功反包），D 股 0 次 —— C 显然更可能再次反包

这个信号反映股票的"性格"（题材粘性、资金粘性、波动节奏），不依赖我们的预测算法（区别于 `prediction_accuracy_results` 表的"预测命中史"），用真实 K 线扫描即可获得。

## 目标

1. 在二波接力 / 反包 / 强势承接评分内，新增"近 90 日同类形态成功命中次数"加分维度，封顶 +10
2. "预测依据"列自动展示新 reason，让用户一眼看出"是不是老熟客"
3. 0 额外 DB 查询：复用 scoring 函数内已加载的 history DataFrame
4. 行为单调：原有评分维度不变，仅追加新维度；不引入回归

非目标：
- 不改 cont / fresh / trend 三个类别（留作后续迭代评估）
- 不引入新的 DB 表 / migration
- 不写跨服务批量查询（无需，K 线已加载）
- 不重构现有 scoring 函数的整体结构

## 设计

### 新增 2 个 helper（stock_filter.py 模块级或类内 static）

```python
def _count_historical_followthrough(
    history_df: pd.DataFrame,
    code: str,
    lookback_days: int = 90,
    window: int = 5,
    threshold_fn: Callable[[str], float] = None,
) -> Tuple[int, Optional[int]]:
    """扫历史 K 线统计成功二波接力次数。
    
    定义：某日涨停（≥ threshold-0.3%）→ 后续 window 日内出现另一次涨停 → 计为 1 次。
    跳过 today（最后 1 行），避免今日数据自计。
    
    返回：(occurrence_count, days_since_last_hit) 
    - occurrence_count：lookback_days 范围内的成功次数
    - days_since_last_hit：距今最近一次的天数，None 表示无命中
    """
```

```python
def _count_historical_wrap(
    history_df: pd.DataFrame,
    code: str,
    lookback_days: int = 90,
    window: int = 5,
    drop_threshold: float = -3.0,
    threshold_fn: Callable[[str], float] = None,
) -> Tuple[int, Optional[int]]:
    """扫历史 K 线统计成功反包次数。
    
    定义：某日涨停 → window 日内至少一根 ≤ drop_threshold% 阴线 → 之后再次涨停 → 计为 1 次。
    跳过 today。
    
    返回：(occurrence_count, days_since_last_hit)
    """
```

注意：
- `threshold_fn(code)` 用现有 `self._limit_up_threshold_pct` 逻辑（主板 ±10%、创业板/科创板 ±20%）
- `window` 默认 5 与现有 lookback_days=5 一致

### 评分集成（_score_followthrough_candidate）

在现有评分末尾（`return` 前）追加：

```python
occ_count, last_hit_days = _count_historical_followthrough(
    history, code, lookback_days=90, window=5,
    threshold_fn=self._limit_up_threshold_pct,
)
if occ_count >= 3:
    bonus = 8
elif occ_count >= 2:
    bonus = 5
elif occ_count >= 1:
    bonus = 2
else:
    bonus = 0

if bonus > 0:
    if last_hit_days is not None and last_hit_days <= 30:
        bonus = min(bonus + 2, 10)  # 近 30 日命中再 +2，封顶 10
        reasons.append(f"近90日{occ_count}次二波接力成功 (最近{last_hit_days}日内) +{bonus}")
    else:
        reasons.append(f"近90日{occ_count}次二波接力成功 +{bonus}")
    score += bonus
```

### 评分集成（_score_broken_board_wrap）

对**两条路径（wrap 经典反包 / hold_strong 强势承接）**都加同样的加分维度，但调用 `_count_historical_wrap`：

```python
occ_count, last_hit_days = _count_historical_wrap(
    history, code, lookback_days=90, window=5,
    drop_threshold=-3.0,
    threshold_fn=self._limit_up_threshold_pct,
)
# 同样的 ≥3 / ≥2 / ≥1 加分阶梯 + 近 30 日 +2 + 封顶 10
# reason 文案根据 pattern_kind 区分：
#   wrap 路径："近90日X次反包成功 +N"
#   hold_strong 路径："近90日X次承接成功 +N"  ← 即使 helper 是 wrap 也用承接文案，与 predict_type 一致
```

注意：`_count_historical_wrap` 内部找的是"涨停→打回→再涨停"形态。对**强势承接**路径而言，这个 helper 的命中数同样代表"这只股有反包性格"，对承接预测仍有正反馈，文案改成"承接成功"即可。

### history days 调整

现有 `_score_followthrough_candidate` 内：
```python
history = self.fetcher.get_history_data(code, days=65, ...)
```
改成 `days=120`（覆盖 90 日 + 余量 + 涨停后 5 日窗口）。同样改 `_score_broken_board_wrap`。

本地 SQLite history 表通常有 ≥250 日，**不会触发额外网络拉取**。

## 评分加成规则汇总

| 条件 | 加分 | reason 文案示例 |
|---|---|---|
| 近 90 日成功 ≥ 3 次 | +8 | "近90日3次二波接力成功 +8" |
| 近 90 日成功 = 2 次 | +5 | "近90日2次二波接力成功 +5" |
| 近 90 日成功 = 1 次 | +2 | "近90日1次二波接力成功 +2" |
| 近 90 日成功 = 0 次 | 0 | （不追加 reason） |
| **额外修饰**：最近一次发生在 30 日内 | 再 +2 | reason 追加 "(最近X日内)" |
| **封顶** | +10 | 防止某只股票仅靠"老熟客"刷分 |

## 应用范围

| 类别 | 是否加 | 用哪个 helper |
|---|---|---|
| cont（保留涨停） | ❌ 本次不动 | - |
| first（二波接力） | ✅ | `_count_historical_followthrough` |
| fresh（首板涨停） | ❌ 本次不动 | - |
| wrap 路径 A（经典反包） | ✅ | `_count_historical_wrap` |
| wrap 路径 B（强势承接） | ✅ | `_count_historical_wrap`（同一 helper，承接性格代理指标）|
| trend（趋势涨停） | ❌ 本次不动 | - |

## 错误处理

- `history is None or empty`：occ_count=0, last_hit_days=None，不加分（无 reason，不报错）
- history 行数不够 lookback_days：仅在已有数据范围内扫，不报错
- `pd.to_numeric` 转换出错：单行跳过，不影响其他行
- `threshold_fn` 抛异常：兜底 10.0%，记 debug log 不中断流程

## 测试策略

新增 unit test 覆盖 helper 函数（纯计算逻辑，无 DB / 网络依赖）：

`tests/test_historical_pattern_count.py`：
- 测 `_count_historical_followthrough`：
  - 空 DataFrame → (0, None)
  - 1 个涨停无后续 → (0, None)
  - 1 个涨停 + 3 日后再涨停 → (1, last_days)
  - 2 次连续二波接力 → (2, ...)
  - 涨停后 6 日才涨停（超窗口）→ 不计入
  - 创业板 20% 涨停阈值识别正确
- 测 `_count_historical_wrap`：
  - 空 DataFrame → (0, None)
  - 涨停→直接再涨停（无阴线）→ 不计为反包
  - 涨停→阴线 -4% → 再涨停 → (1, ...)
  - 涨停→阴线 -2%（未达阈值）→ 再涨停 → 不计为反包
  - 多次反包 → 正确计数

不新增集成测试（评分函数的回归靠现有 pytest 274 守底）。

## 验收标准

1. `_count_historical_followthrough` / `_count_historical_wrap` 实现正确，**新单测全部通过**
2. 现有 `pytest -q --tb=no` 保持 274 passed + 新增测试数（约 12-15 个）
3. GUI 启动 6 秒无 traceback
4. 手动运行一次预测：看到 first / wrap tab 中至少有候选股的"预测依据"列包含新 reason 文案（如"近90日X次..."），证明集成生效
5. 评分**总分**不会因为新 reason 出现 > 100 的异常（封顶逻辑正确）
6. grep `_count_historical_followthrough\|_count_historical_wrap` 在 stock_filter.py 应命中 2 个 def + 2 个调用 + 单测里若干次

## 不在范围

- 不改其他 3 个类别（cont / fresh / trend）
- 不引入新 DB 表
- 不批量预热历史（worker 线程的事，与本 spec 无关）
- 不重排"预测依据"列内 reason 顺序
- 不改 history 缓存策略，仅把 days 参数从 65 调到 120

## 后续可扩展

- cont / fresh / trend 同样可加历史性格信号（按类别定义不同 pattern）
- 把"性格分"独立成单列展示，方便用户排序
- 加上"近 N 日同类形态失败率"作为反向加分（频繁失败 → 扣分）
