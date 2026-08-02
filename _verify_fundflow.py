"""资金流修复验证（临时脚本，可复跑）

覆盖：
  场景1 启动连通性探针（东财/同花顺直连探测）
  场景2 同花顺按 code 二分翻页防漏股（原 bug: 000938 被漏）
  场景3 主流程 get_fund_flow_data —— 东财不可达应走同花顺兜底，不报错
  场景4 二次调用不空转（不因缺大单反复刷新/弹窗）
  场景5 显式 eastmoney/ths 模式 intact
"""
import logging, time
logging.basicConfig(level=logging.ERROR, format="%(message)s")

import stock_data
from src.sources import ths

print("=" * 64)
print("【场景1】启动连通性探针")
print("=" * 64)
stock_data.check_fund_flow_connectivity(log=print)
em_ok = stock_data._FF_EM_REACHABLE
ths_ok = stock_data._FF_THS_REACHABLE
print(f"  东财直连可达   = {em_ok}")
print(f"  同花顺直连可达 = {ths_ok}")

print("\n" + "=" * 64)
print("【场景2】同花顺按 code 二分翻页防漏股（原 bug 票 000938）")
print("=" * 64)
for code in ["000938", "600036", "300750", "000001", "688981", "002594"]:
    t0 = time.time()
    r = ths.fetch_fund_flow_frame(code)
    hit = not r.empty
    name = r.iloc[0]["股票简称"] if hit else "未命中"
    net = r.iloc[0]["净额(元)"] if hit else "-"
    print(f"  {code}: 命中={'Y' if hit else 'N'} 名称={name} 净额={net} ({time.time()-t0:.1f}s)")

print("\n" + "=" * 64)
print("【场景3】主流程 get_fund_flow_data —— 东财不可达走同花顺兜底")
print("=" * 64)
# 确保走兜底路径：沙箱里东财本就连不通；显式置 False 更稳
stock_data._FF_EM_REACHABLE = False
f = stock_data.StockDataFetcher()
print("  auto plan provider_sequence =", f.build_fund_flow_request_plan("auto").provider_sequence)
print("  _eastmoney_fund_flow_expected =", f._eastmoney_fund_flow_expected())
t0 = time.time()
df = f.get_fund_flow_data("000938", days=5, force_refresh=True)
print(f"  结果: {'None(失败)' if df is None else str(len(df))+' 行'}  耗时 {time.time()-t0:.1f}s")
if df is not None:
    print(f"  date={df.iloc[0]['date']}  main_force_amount={df.iloc[0]['main_force_amount']}  big_order_amount={df.iloc[0]['big_order_amount']}")
    print(f"  来源列: {list(df.columns)}")

print("\n" + "=" * 64)
print("【场景4】二次调用（验证不空转 / 不报错）")
print("=" * 64)
t0 = time.time()
df2 = f.get_fund_flow_data("000938", days=5)
ok = df2 is not None and len(df2) > 0
print(f"  结果: {'None(失败)' if df2 is None else str(len(df2))+' 行'}  耗时 {time.time()-t0:.2f}s")
print(f"  不空转/不报错: {'Y' if ok else 'N'}")

print("\n" + "=" * 64)
print("【场景5】显式 eastmoney / ths 模式配置 intact")
print("=" * 64)
print("  plan eastmoney:", f.build_fund_flow_request_plan("eastmoney").provider_sequence)
print("  plan ths      :", f.build_fund_flow_request_plan("ths").provider_sequence)

print("\n✅ 验证脚本执行完毕")
