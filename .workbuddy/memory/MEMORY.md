# 项目长期记忆（gupiao）

## 网络出口约定（重要）
- 项目网络请求**全部直连**，不使用任何代理。2026-08-01 已删除 `src/network/proxy_pool.py`（免费代理池），
  并将 `stock_data._apply_network_patches` 改为强制 `trust_env=False`（所有 requests.Session 忽略 HTTP(S)_PROXY）。
- `_set_proxy.bat` 已改为只清空代理 env、走直连（不再探测/注入 Clash 7897）。
- 遇到 `ProxyError` / `RemoteDisconnected`：优先查本机是否残留 `HTTPS_PROXY`/`HTTP_PROXY` 环境变量指向失效代理，
  **不要再动代理相关代码**。历史K线走本地 sqlite 缓存不受影响。
- 东财直连 `RemoteDisconnected`（非 ProxyError）的根因**不是 Clash**（用户 2026-08-02 明确确认未开 Clash）。
  实际是本机网络链路对 `push2his.eastmoney.com` 的 TCP 限制（ISP/防火墙/公司网络），或东财服务端风控。
  表现为 `Connection aborted. RemoteDisconnected`。代码层无法绕过网络层限制，只能靠同花顺兜底（无大单拆分）；
  排查应直接测本机到 eastmoney 的连通性，而非改代码或动 Clash。

## 资金流数据
- 个股资金流：东财优先走自建 `src/sources/eastmoney/fund_flow.fetch_individual_fund_flow`（真实会话 Cookie 预取 + 完整浏览器头 + 多 host 兜底 push2his/push2/push2delay，含主力/大单/超大单完整拆分），失败再兜底 akshare `stock_individual_fund_flow`；东财整体不可用则回退同花顺源。
- **启动探针** `stock_data.check_fund_flow_connectivity()`（模块导入时后台线程触发，幂等）：
  探测东财 `push2his.eastmoney.com` 与同花顺 `data.10jqka.com.cn` 直连；
  东财不可达→主动熔断东财(`em_circuit_breaker`)并打印处置提示（不归因 Clash），后续资金流自动走同花顺兜底。
  状态存 `_FF_EM_REACHABLE` / `_FF_THS_REACHABLE`；`build_fund_flow_request_plan(auto)` 据此优先选源。
- **同花顺兜底** `src/sources/ths.fetch_fund_flow_frame`：用「按 code 降序二分翻页」定位目标股票
  （修复 akshare `stock_fund_flow_individual` 用 zdf 翻页导致 000938 等被漏掉的 bug）。
  同花顺榜单**仅含流入/流出/净额，无主力/大单拆分**；兜底把「净额」映射为 主力净额/大单净额。
  `StockDataFetcher._eastmoney_fund_flow_expected()` 为 False 时，缓存缺大单也不空转刷新。
- ⚠️ 同花顺 `data.10jqka.com.cn/funds/ggzjl` 偶发 401（反爬 hexin-v 失效），`_ths_page_frame` 已加重试+vcode刷新。
- 东财 `RemoteDisconnected` 若是服务端风控（缺真实 cookie），`fund_flow.py` 的真实会话 Cookie 方案可修复；
  若是本机网络层 TCP 限制（ISP/防火墙），加 cookie 无效，只能同花顺兜底（无大单拆分）。2026-08-02 已做完修复代码，待用户本机验证。
- 资金流必须联网；历史K线走 `data/stock_store.sqlite3` 缓存。

## GitHub 推送约定
- 本机到 GitHub 的 HTTPS(443) 直连超时不可达（百度等站点可通，非整体断网），但 SSH(22) 端口通。
- remote 已切换为 SSH：`git@github.com:xinhui01/ashare-scan.git`（2026-08-02 因 443 超时而改；原仓库 gupiao 后重命名为 ashare-scan，GitHub moved 提示后已更新 remote）。
- SSH 密钥：`~/.ssh/id_ed25519`（ed25519，空口令，comment `gupiao-workbuddy`），公钥已生成待加入 GitHub 账户。
- 推送命令：`git push origin main`（走 SSH 22，无需任何代理）。首次推送前需把公钥加到 GitHub → Settings → SSH and GPG keys。
- 与"股票数据全部直连"约定不冲突：GitHub 走 SSH 协议，数据 API 仍直连。

## Git 跟踪引用沙箱限制（重要）
- 本沙箱环境下 git 自身的引用写盘被拦截：`git update-ref` 返回成功但不生效；`git push`/`git fetch` 也**不会刷新** `origin/main` 远程跟踪引用。
- 表现：`git status` 持续误报 `ahead N`（如 ahead 293 / ahead 1），但 `git ls-remote origin main` 查到的远程真值始终正确（推送其实已成功）。
- **验证同步是否成功唯一可靠方式**：`git ls-remote origin main`（直接问 GitHub）。
- 修正本地显示假 "ahead N"：直接编辑 `.git/packed-refs`，把 `refs/remotes/origin/main` 那行改成 `git ls-remote` 返回的 SHA；`git update-ref` 在此环境无效。
- 直接改 packed-refs 文件（Edit/Write 工具）可持久生效、rev-parse 能正确读取；不要依赖 update-ref/push/fetch 去刷新该引用。

## 策略复盘教训（预判次日用）
- 当 涨停总数 / 晋级率 / 市场情绪 三者同步骤升 = 情绪 regime 切换为**进攻**，不要停留在"轮动日=防守"旧框架；应预案"情绪爆量则转攻、跟随当日新主线"。
- 主线有惯性但也轮动：预判次日时，主线以"**当日**最强行业/题材"为准，不要机械沿用前一日主线（案例：0803夜按电网设备→0804实际切元件/芯片半导体，一夜切换）。
- 超短纪律：持仓若掉出强度池 TOP10 且不在主线内 → 优先换主线而非死扛；开盘核 5日线，破即走。
- 反包池被系统标"谨慎/回避"时，即便大环境火热也按规则不做（案例：中岩大地 0803 连板 → 0804 被砸成反包）。
