"""scanning 包：stock_filter scan 编排模块化拆分。

按 stock_filter.py 模块化 spec 拆出的子模块：
- orchestrator.py — scan_all_stocks 主编排 + 18 个 scan helper（线程池调度 / 取消信号 /
  历史镜像分区 / 进度上报）
"""
