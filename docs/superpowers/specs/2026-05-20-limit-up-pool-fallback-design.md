# 涨停池 spot 兜底设计

- 创建日期：2026-05-20
- 状态：待实施
- 范围：给 `EODData.get_limit_up_pool` / `get_previous_limit_up_pool` 加 spot 快照兜底，解决东财熔断时预测完全失能的问题

## 背景

当前 `get_limit_up_pool` 仅依赖东财 `ak.stock_zt_pool_em`。东财熔断时直接返回空 DataFrame，导致：
1. 涨停预测拿不到今日涨停池数据
2. 涨停对比窗口里"今日"行全是 0
3. "晋级率: 0.0%" 等数字被误算
4. 整个预测流程实质失能

而 `stock_filter._fetch_spot_snapshot` 已实现东财→新浪的全市场快照兜底，但只在预测内部使用，涨停池没复用这个兜底链。

之前 commit `66cc750` 给上证指数加了三源级联（东财→新浪→腾讯），同样思路套用到涨停池。

## 目标

1. 东财涨停池失败时，自动用全市场 spot 兜底，过滤出今日涨停股
2. 用本地 SQLite 昨日涨停池递推今日连板数
3. 退化输出兼容现有涨停池 DataFrame schema（让 predict 链路无感切换）
4. 0 行为回归（东财正常时维持现有流程）

非目标：
- 不重写涨停池存储格式 / SQLite schema
- 不为涨停池对比窗口加去重缓存（B 选项，用户暂不要）
- 不补 首封时间 / 涨停原因（spot 没有，留空即可）
- 不动 `compare_limit_up_pools` / `compare_limit_up_pools_window` 调用方
- 不动 `_eastmoney_circuit_breaker_open` 熔断逻辑

## 设计

### 新增 helper：`_derive_limit_up_pool_from_spot`

在 `stock_data.py` 的 `EODData` 类内新增：

```python
def _derive_limit_up_pool_from_spot(
    self,
    trade_date: str,
    prev_pool_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """从全市场 spot 快照派生今日涨停池（东财涨停池失败时的兜底）。

    步骤：
    1. 调 _fetch_spot_with_fallback 拿全市场 spot（东财→新浪自动兜底）
    2. 计算每只股票"涨停阈值"（主板 10%、创业/科创 20%、北交所 30%）
    3. 过滤出 today 涨幅 ≥ threshold-0.3 的股票
    4. 用 prev_pool_df 递推连板数：
       - 在 prev_pool 中找到 (连板数=N) → today_count = N+1
       - 未在 prev_pool 中 → today_count = 1（首板）
    5. 合成 DataFrame，列名与东财涨停池一致：
       代码 / 名称 / 最新价 / 涨跌幅 / 换手率 / 流通市值 / 总市值 /
       连板数 / 首次封板时间(留空) / 最后封板时间(留空) / 炸板次数(留 0) /
       所属行业 / 涨停统计 / 涨停原因(留空)
    返回空 DataFrame 表示彻底失败。
    """
```

### 新增 helper：`_fetch_spot_with_fallback`

把 `stock_filter._fetch_spot_snapshot` 的兜底逻辑提炼到 `stock_data.py`（避免跨文件依赖）：

```python
def _fetch_spot_with_fallback(self) -> Optional[pd.DataFrame]:
    """全市场实时行情快照，东财→新浪兜底。"""
    import akshare as ak
    if not _eastmoney_circuit_breaker_open():
        try:
            if self._log:
                self._log("全市场 spot 快照：东财...")
            return _retry_ak_call(ak.stock_zh_a_spot_em)
        except Exception as exc:
            if self._log:
                self._log(f"全市场 spot 东财失败: {exc}，尝试新浪兜底")
    try:
        if self._log:
            self._log("全市场 spot 快照：新浪兜底（约 30s）...")
        df = _retry_ak_call(ak.stock_zh_a_spot)
        if df is not None and not df.empty:
            # 新浪代码带 sh/sz/bj 前缀，需归一化
            if "代码" in df.columns:
                df["代码"] = (
                    df["代码"].astype(str)
                    .str.replace(r"^(sh|sz|bj)", "", regex=True)
                    .str.strip()
                    .str.zfill(6)
                )
            return df
    except Exception as exc:
        if self._log:
            self._log(f"全市场 spot 新浪兜底也失败: {exc}")
    return None
```

注意：
- `stock_filter._fetch_spot_snapshot` 暂不删除（避免回归），后续可改为调本 helper
- 列名"代码"在新浪/东财都用中文，无需翻译

### 修改 `get_limit_up_pool` 网络分支

原有：
```python
# 3. 网络请求（涨停池目前仅东财有接口）
if _eastmoney_circuit_breaker_open():
    self._log(f"涨停池 {date_key}：东财熔断中，暂无替代数据源。可尝试换 IP 或等待冷却结束。")
    return pd.DataFrame()
try:
    df = _retry_ak_call(ak.stock_zt_pool_em, date=date_key)
    if df is not None and not df.empty:
        # ... 入库 ...
        return df
except Exception as e:
    self._log(f"涨停池 {date_key} 获取失败: {e}")
empty = pd.DataFrame()
self._limit_up_pool_cache[date_key] = empty
return empty
```

改造为：
```python
# 3. 东财涨停池接口
em_ok = not _eastmoney_circuit_breaker_open()
if em_ok:
    try:
        df = _retry_ak_call(ak.stock_zt_pool_em, date=date_key)
        if df is not None and not df.empty:
            raw_count = len(df)
            df = self._sanitize_limit_up_pool(df)
            dropped = raw_count - len(df)
            self._limit_up_pool_cache[date_key] = df
            save_limit_up_pool(date_key, df)
            if self._log:
                drop_note = f"，过滤 {dropped} 条脏数据" if dropped > 0 else ""
                self._log(f"涨停池 {date_key} 东财 {len(df)} 只{drop_note}，已保存")
            return df
    except Exception as exc:
        if self._log:
            self._log(f"涨停池 {date_key} 东财失败: {exc}，尝试 spot 兜底")
else:
    if self._log:
        self._log(f"涨停池 {date_key} 东财熔断中，尝试 spot 兜底")

# 4. spot 兜底：从全市场 spot 派生
prev_date = _previous_trading_day(date_key) or ""
prev_pool = None
if prev_date:
    prev_pool = load_limit_up_pool(prev_date)
derived = self._derive_limit_up_pool_from_spot(date_key, prev_pool_df=prev_pool)
if derived is not None and not derived.empty:
    self._limit_up_pool_cache[date_key] = derived
    save_limit_up_pool(date_key, derived)
    if self._log:
        self._log(f"涨停池 {date_key} spot 兜底 {len(derived)} 只（连板数由昨日 pool 推断），已保存")
    return derived

# 5. 彻底失败
empty = pd.DataFrame()
self._limit_up_pool_cache[date_key] = empty
if self._log:
    self._log(f"涨停池 {date_key} 所有源均失败，返回空")
return empty
```

`get_previous_limit_up_pool` 做类似改造（spot 兜底过滤更弱，因为"昨日涨停池"本身在 SQLite 大概率有缓存）。

### 列名兼容性

东财 涨停池 DataFrame 关键列：
```
序号 代码 名称 最新价 涨跌幅 成交额 流通市值 总市值
换手率 封板资金 首次封板时间 最后封板时间 炸板次数 涨停统计 连板数 所属行业
```

派生 DataFrame **必须**保留：`代码`、`名称`、`最新价`、`涨跌幅`、`换手率`、`连板数`、`所属行业`（这些被 predict 评分函数读取）。其余可留空或 NaN。

`_sanitize_limit_up_pool` 现有逻辑会过滤 `涨跌幅 == -100 / 最新价 == 0` 等脏数据，派生数据天然符合（spot 数据本就是有效行情），但仍然过一遍 sanitize 以防 spot 数据偶有异常。

### 性能考量

- 东财正常时：0 额外开销（沿用原路径）
- 东财失败时：新浪 spot 单次请求约 30s（一次性，因为 SQLite 缓存会接住后续请求）
- 派生过程：纯 CPU 内存操作，过滤+合成 5000 行 spot → 几百行涨停池，<200ms
- 连板数推断：查一次 SQLite 昨日 pool，单次 SQL <10ms

## 错误处理

- spot 拉取失败 → derived 为 None → 走最终空返回
- 昨日 pool 不存在 → 所有股票连板数 = 1（合理退化）
- 派生过程任何异常 → try/except 兜底，记 log，返回空
- prev_date 计算失败 → 用空字符串，pool 查询会返回 None，连板数全设 1

## 测试

### 新增单测 `tests/test_limit_up_pool_fallback.py`

测 `_derive_limit_up_pool_from_spot`（纯函数路径）：

1. spot 为 None → 空 DataFrame
2. spot 为空 DataFrame → 空 DataFrame
3. spot 含 5 只股票，1 只 +10%、1 只 +9.6%（达阈值）、3 只 +5% → 派生出 2 只涨停股
4. 创业板 +11% 不算涨停（创业板阈值 20%）
5. 北交所 +30% 算涨停
6. 昨日 pool 有股票 A（连板数=2）+ 股票 B（连板数=1），今日 A、B、C 都涨停 → A=3 / B=2 / C=1
7. 昨日 pool 为 None → 所有今日涨停股连板数 = 1
8. 列名完整性：派生 DataFrame 必有 [代码,名称,最新价,涨跌幅,换手率,连板数,所属行业]

### 不动现有 274+16=290 个 pytest

`get_limit_up_pool` 改造保持东财正常路径 100% 不变（只新增 fallback 分支），现有 pytest 全绿不破。

## 验收标准

1. 新增单测全部通过（约 8 个 case）
2. 现有 pytest 290 passed 不下降
3. GUI 启动 6 秒无 traceback
4. 模拟东财熔断（手动设置 circuit_breaker 状态）→ `get_limit_up_pool` 仍能返回非空 DataFrame
5. 派生 DataFrame 的列名兼容 `compare_limit_up_pools` / `predict_limit_up_candidates` 流程（手动跑一次预测验证）
6. 连板数递推正确（昨日 pool 在 SQLite 时）

## 不在范围

- 不实施 B（涨停池对比窗口去重）
- 不补 首封时间 / 涨停原因 字段
- 不重写 `compare_limit_up_pools_window`
- 不改 `stock_filter._fetch_spot_snapshot`（保留兼容）
- 不动 stock_gui.py
- 不引入新 DB 表 / migration

## 后续可扩展

- B 选项（窗口去重缓存）单独 spec
- 把 `stock_filter._fetch_spot_snapshot` 改为转发 `_fetch_spot_with_fallback`，消除重复代码
- 派生 pool 加入 industry meta 回填（如果 spot 没带行业）
