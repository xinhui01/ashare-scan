"""scoring 包：涨停预测评分模块化拆分。

按 stock_filter.py 模块化 spec 拆出的子模块：
- helpers.py — 模块级 K 线历史形态统计 helper
- shared.py — 跨 scorer 复用的评分调节因子（theme/capital flow/vol baseline）
- classifiers.py — 涨停形态分类
- profile.py — pre-limit-up 特征提取与 profile 聚合
- cont.py / first.py / fresh.py / wrap.py / trend.py / first_board.py — 5 个主类别 scorer
- predict.py — predict_limit_up_candidates 主编排
"""
