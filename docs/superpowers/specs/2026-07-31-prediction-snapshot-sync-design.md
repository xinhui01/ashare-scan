# 预测快照 JSON 跨电脑同步 — 设计

日期:2026-07-31
状态:已与用户确认方案 A(导出 + 自动导入闭环),数据无隐私顾虑,自动 commit+push。

## 目标

每次预测完成后,把最新一次预测(候选名单、预测依据、情绪上下文、派生模拟买入)导出为一份
git 跟踪的 JSON 文件并自动提交推送;另一台电脑 `git pull` 后启动 GUI 自动导入本地 SQLite,
竞价确认、模拟买入等既有功能零改动可用。

## 非目标

- 不同步整个 SQLite 库、不同步历史多日预测(只保留最新一份,覆盖式)。
- 不自动执行 `git pull`(用户在另一台电脑手动 pull)。
- 不改动竞价确认 / 模拟买入 / 准确率对账的任何逻辑。

## 文件与数据

- 快照路径:`snapshots/latest_prediction.json`(仓库根,git 跟踪;`data/` 被 ignore 所以不放那里)。
- 结构:

```json
{
  "schema_version": 1,
  "exported_at": "2026-07-31 18:30:00",
  "trade_date": "20260731",
  "prediction": { "...完整预测 payload(与 limit_up_predictions.payload_json 相同)..." },
  "simulated_buys": [ "...由 build_simulated_buy_picks(limit=2) 派生的当日模拟买入快照,只读参考..." ]
}
```

- 情绪、预测依据(reasons)、summary 本来就在 payload 里,无需额外拼装。
- `simulated_buys` 仅供人工查看/其他工具消费;导入端不写模拟买入台账——
  GUI 既有的 `_start_simulated_buy_history_sync` 会从预测记录自动重建,保持单一真源。

## 组件

新增 `src/services/prediction_snapshot_service.py`:

1. `build_snapshot(payload) -> dict`:拼装上述结构。
2. `export_snapshot(payload, auto_push=True) -> bool`:写文件(UTF-8、ensure_ascii=False、
   indent=2 便于 git diff),然后 `_git_publish()`。任何失败只记日志、绝不抛出、不影响预测流程。
3. `_git_publish(path)`:`git add <file>` → `commit`(信息 `chore: 更新预测快照 <trade_date>`)→
   `push`;push 失败则 `pull --rebase` 后重推;rebase 失败则 `rebase --abort` 并告警,
   绝不 stash / 不碰用户其他改动。子进程带超时(本地操作 15s、网络操作 60s)。
4. `import_snapshot_if_newer() -> str`:读快照文件,校验 schema;
   与本地 `limit_up_predictions.saved_at` 比较(`exported_at` 更新才导入);
   导入 = `save_limit_up_prediction_record(payload)` + `save_last_limit_up_prediction(payload)`
   (后者是竞价确认 fallback 与启动加载读的 key)。返回状态描述供日志。

`stock_store.py` 新增 `get_limit_up_prediction_saved_at(trade_date) -> Optional[str]`。

## 接入点

- 导出:`src/services/scoring/predict.py` 主结果保存处(save_limit_up_prediction_record 之后),
  覆盖 GUI 与 CLI(predict_today.bat)两条链路;`src/gui/tabs/predict.py` 题材回填重存处同样导出
  (每次预测最多 2 次 commit,可接受)。涨停池为空的早退 degraded 结果**不导出**,避免空结果
  覆盖同日正常快照。
- 导入:`src/gui/app.py` 启动流程中、`self.predict._load_last_prediction()` 之前同步调用
  (文件读取 + 单条 SQLite 写,毫秒级),结果写日志。

## 使用时序

T 日盘后电脑 A 跑预测 → 自动写快照 + commit + push;
T+1 竞价期电脑 B `git pull` → 打开 GUI(启动自动导入)→ 点「竞价确认」。

## 错误处理

- git 不可用 / 无网络 / 冲突:快照文件已写,push 降级为日志告警,用户可手动提交。
- 快照损坏 / schema 不符 / 缺文件:导入静默跳过并记日志。
- 本地已有同日更新记录:跳过导入(以 saved_at vs exported_at 判定)。

## 测试

`tests/test_prediction_snapshot_service.py`(unittest 风格,临时目录 + mock subprocess):
快照结构、导出写文件、git 失败降级不抛、导入的新/旧/缺失/损坏四分支、saved_at getter。
回归:`tests/test_stock_store.py`。
手工验证:对真实库最新预测跑一次 `export_snapshot(auto_push=False)`,肉眼检查 JSON。
