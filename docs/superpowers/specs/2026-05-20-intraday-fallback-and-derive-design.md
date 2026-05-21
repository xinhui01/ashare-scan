# 分时兜底 + 派生封板字段 + 数据源标签 设计

- 创建日期：2026-05-20
- 状态：待实施
- 范围：3 个 phase 串行。Phase 1 给 intraday（分时）数据加新浪/腾讯兜底；Phase 2 用 intraday 数据派生 spot 兜底涨停池缺失的"首次封板时间"和"炸板次数"；Phase 3 在 predict tab 顶部显示当前涨停池数据源标签。

## 背景

上一轮 spot 兜底（commit `fb5450b`）让东财熔断时涨停池退化可用，但派生的涨停池有 3 个字段不完整：
1. **连板数**（已通过昨日 SQLite pool 推断）✅
2. **首次封板时间** —— 留空
3. **炸板次数** —— 默认 0

后两者直接影响 cont（保留涨停）评分的"秒板/早封 +15"加分和"未炸板 +15"加分（默认 0 炸板会给满分，偏高），导致 spot 兜底模式下 cont 评分失真。

修复思路：用每只涨停股的 1min 分时 K 线，逆向算出首次封板时间（first bar where close ≥ 涨停价）和炸板次数（count of "封板→跌破→再封"的次数）。

**前置条件**：项目当前 intraday 抓取只走东财（`src/sources/eastmoney/intraday.py`），东财熔断时 intraday 也失败。所以**必须先做 intraday 的新浪/腾讯兜底**，再用 intraday 派生这两个字段。

同时，spot 兜底场景对用户透明性差。需要 UI 标签明示当前涨停池数据来源（东财 / 缓存 / spot 兜底），让用户一眼看出 cont 评分是不是在退化模式下产生的。

## 目标

1. intraday 分时 K 线在东财失败/熔断时自动切换到新浪/腾讯，行为类似已实现的 spot 三源级联
2. spot 兜底涨停池在派生时调 intraday 拉每只涨停股的 1min 数据，精确计算"首次封板时间"和"炸板次数"
3. predict tab 顶部加一个小标签，根据本次预测使用的涨停池数据源切换显示：
   - 缓存命中（内存/SQLite）：灰色 "数据: 本地缓存"
   - 东财在线：灰色 "数据: 东财"
   - spot 兜底：橙色 "数据: spot 兜底 ⚠️"
   - 完全失败：红色 "数据: 无 ❌"

非目标：
- 不重构 `src/sources/eastmoney/intraday.py` 的内部结构（仅追加兜底链路）
- 不动 SQLite intraday_cache 缓存层
- 不引入新 DB 表
- 不动 stock_gui.py 主类（标签放在 src/gui/app.py 的 predict tab 区域）
- 不动 cont 评分本身的加分公式

## 3 phase 划分

### Phase 1 — intraday 新浪兜底（已存在，跳过）

**调查发现**：项目当前已实现 intraday 的东财→新浪双源级联：
- `DATA_SOURCE_OPTIONS["intraday"]` = `("auto", "eastmoney", "sina")`
- `build_intraday_request_plan("auto")` 在东财熔断时自动切换 `provider_sequence=("sina", "eastmoney")`
- `stock_data.py:2429-2461` 的循环按 plan 顺序遍历 provider

腾讯作为第 3 源 ROI 不高（sina 稳定性已够，3 源都失败的概率极低）。**Phase 1 整体跳过**，无需新代码。

如果未来 sina 也不稳定，再单独 spec 加 tencent 第 3 源。

### Phase 2 — 从 intraday 派生 首次封板时间 + 炸板次数

**目标**：spot 兜底派生涨停池时，对每只涨停股调 intraday（已有 Phase 1 兜底），计算精确的封板时间字段。

**新增 helper**（`stock_data.py` 或 `src/sources/intraday_derive.py`）：
```python
def _derive_seal_time_from_intraday(
    intraday_df: pd.DataFrame,
    limit_up_price: float,
    tolerance_pct: float = 0.1,  # 价格 ≥ limit_up * (1 - 0.1%) 即视为封板
) -> Optional[str]:
    """从 1min 分时找出首次封板时间（HH:MM:SS 字符串），无封板返回 None。"""

def _count_intraday_breaks(
    intraday_df: pd.DataFrame,
    limit_up_price: float,
    tolerance_pct: float = 0.1,
) -> int:
    """数封板后又跌破涨停价的次数（炸板次数）。"""
```

**集成**：
- 修改 `_derive_limit_up_pool_from_spot`：派生每只股票时，调 `fetcher.fetch_intraday(code, date)` 拿 1min 数据，再调上面两个 helper
- 注意性能：50+ 候选股 × 拉 intraday，可能慢。需要：
  - 并发拉取（用 `_DaemonThreadPoolExecutor`，limit 4-8 worker）
  - 或惰性派生：只在 cont 评分确实需要这两个字段时才拉（cont 评分函数内 lazy fetch）
  - 优先方案：并发拉取（更简单，缓存层兜底）

**单测**：
- `_derive_seal_time`：无封板 / 早盘秒板 / 中午封板 / 尾盘封板
- `_count_intraday_breaks`：0 炸板 / 1 炸板 / 多次炸板
- 容差：价格 = limit_up × 0.999 应算封板（在 0.1% 容差内）

**估算**：1-2 commit

### Phase 3 — predict tab 数据源指示标签

**目标**：UI 告知用户当前预测用的涨停池来源。

**新增**：
- `StockDataFetcher` 内 `self._last_pool_source: Dict[str, str] = {}` 字典，key=date_key, value 枚举：
  - `"cache_memory"` / `"cache_db"` / `"eastmoney"` / `"spot_fallback"` / `"empty"`
- 在 `get_limit_up_pool` 每个分支末尾记录 source
- 公开方法 `get_pool_source(date_key: str, *, previous: bool = False) -> str`

**UI 改动**（src/gui/app.py predict tab）：
- 在 `sent_bar`（情绪条）下面新加一行 thin bar，含一个 `self._predict_data_source_label`
- 默认隐藏（pack_forget）；预测完成后调 `get_pool_source(today_date)` 拿到来源，updateLabel + 决定显示样式
- 显示规则：
  - 缓存命中：灰色 `数据: 本地缓存`
  - 东财在线：灰色 `数据: 东财`
  - spot 兜底：橙色 `数据: spot 兜底 ⚠️ cont 评分字段已尽量派生但精度可能下降`
  - 完全失败：红色 `数据: 无 ❌`

**集成**：
- 在 `_apply_predict_result` 末尾追加 `_refresh_data_source_label(today_date)` 调用
- 在 `_refresh_data_source_label` 中查 `self.stock_filter.fetcher.get_pool_source(date_key)` 并更新 label

**估算**：1 commit

## 设计原则

1. **0 行为回归**：东财正常时所有 3 phase 都不改变现有行为
2. **错误兜底完整**：任何一级 fallback 失败都不抛异常，只是退化到下一级或留空
3. **缓存层不变**：内存 LRU + SQLite intraday_cache 仍是首选，fallback 是 cache miss 后的网络层级联
4. **UI 标签不主动弹**：默认安静，仅在 spot 兜底/无数据时用颜色提示

## 性能预估

- Phase 1：东财正常时 0 额外开销；东财熔断时多 1 次 fallback 请求（新浪约 5s 单股，腾讯 3s）
- Phase 2：spot 兜底场景下，对 50 只涨停股各拉 1min 分时，并发 4 worker → 约 30s（首次）。之后命中 SQLite intraday_cache 秒回
- Phase 3：0 额外开销（label update 是同步内存操作）

## 验收

每个 phase 独立验收，串行执行：

### Phase 1
- `tests/test_intraday_fallback.py` 全绿（约 5-8 case）
- 全量 pytest ≥ 298 passed（+ 新单测）
- GUI 启动正常
- 模拟东财熔断时，intraday 接口仍能返回非空 DataFrame（手动测试）

### Phase 2
- `tests/test_intraday_derive.py` 全绿（约 6-8 case）
- 全量 pytest 不下降
- 模拟 spot 兜底场景，派生涨停池的"首次封板时间"和"炸板次数"字段被填充
- cont 评分在 spot 兜底模式下不再固定 "未炸板 +15"

### Phase 3
- 启动 GUI → 预测一次 → 顶部出现数据源标签
- 手动触发东财熔断 → 重新预测 → 标签变橙色显示 "spot 兜底"
- pytest 不下降

## 不在范围

- 不重写 intraday 列名格式（保留现有"时间/开/收/高/低/量"中文列名）
- 不补"封板资金"字段（spot/intraday 都没有）
- 不加历史回填脚本（spot 兜底涨停池存进 SQLite 时，封板时间/炸板次数字段会自动用 derive 后的值）
- 不引入 mypy / 类型注解全覆盖

## 后续可扩展

- 把 Phase 1 的 fallback 模式抽成通用模板（spot fallback 也可以复用同一套级联骨架）
- 把"数据源指示标签"扩展到 detail / intraday tab（让用户知道每个 tab 数据从哪来）
- 把 source 信息写入 SQLite 缓存元数据（便于历史回溯：哪天的涨停池是真东财、哪天是兜底）
