"""涨停预测候选质量门（优化模块）。

问题背景（详见 docs/涨停预测优化方案.md）：
- 现状：predict_limit_up_candidates 一次性吐出全部候选（热门日 50~100+ 只），
  全部候选命中率仅 14~16%，且平均收益为负。
- 实证（近 20 / 60 / 全部历史三窗口反事实，见 data/counterfactual_out.txt）：
  1) 截断候选数：top-5 → 命中率 21~31%，平均收益转正；
  2) 按类别筛：反包(wrap) 命中 20~33%（最佳），二波(first) 18~24%，
     保留(cont) ~20%，趋势(trend) 8~13%、首板(fresh) 8~13%（最差）；
  3) 组合：反包+二波+保留 且 top-5 → 命中率 ~30~40%。

本模块在预测结果落盘前，对 5 个类别子列表做"类别筛选 + top-N 截断"。
函数级默认 preset="off"（库函数无副作用）；预测流程 predict.py 调用处
显式传 wrap_first_cont，即 2026-08-05 起默认启用、跑预测即生效。
环境变量 LIMITUP_QUALITY_GATE 可覆盖调用方预设（设 off 临时恢复全量对比）。
"""
from typing import Any, Dict, List, Optional, Tuple

# 预测结果 dict 中各类别对应的子列表 key
CATEGORY_RESULT_KEY = {
    "wrap": "broken_board_wrap_candidates",   # 反包
    "first": "first_board_candidates",         # 二波接力
    "cont": "continuation_candidates",         # 保留涨停
    "fresh": "fresh_first_board_candidates",   # 首板
    "trend": "trend_limit_up_candidates",      # 趋势涨停
}

# 类别优先级（按实证命中率排序，越小越优先）：
# 反包 > 二波 ≈ 保留 > 首板 > 趋势
# 用类别优先级作主排序，可缓解"跨类别分数标定错位"（诊断F：趋势分最高但命中最低）。
CATEGORY_PRIORITY = {
    "wrap": 0,
    "first": 1,
    "cont": 2,
    "fresh": 3,
    "trend": 4,
}

# 预设：启用类别集合 + 全局截断数（按类别优先级合并后截断）
QUALITY_GATE_PRESETS = {
    "off": None,
    "wrap_only": {"categories": ["wrap"], "top_n": 5},
    "wrap_first": {"categories": ["wrap", "first"], "top_n": 5},
    "wrap_first_cont": {"categories": ["wrap", "first", "cont"], "top_n": 5},
}


def _resolve_config(
    preset: str,
    top_n: Optional[int],
    categories: Optional[List[str]],
) -> Optional[Dict[str, Any]]:
    """解析配置：显式 categories 优先，否则查预设。"""
    if categories is not None:
        return {"categories": list(categories), "top_n": int(top_n or 5)}
    cfg = QUALITY_GATE_PRESETS.get(preset or "off")
    if cfg is None:
        return None
    return dict(cfg)


def _candidate_score(candidate: Dict[str, Any]) -> float:
    try:
        return float(candidate.get("score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def apply_prediction_quality_gate(
    result: Dict[str, Any],
    preset: str = "off",
    top_n: Optional[int] = None,
    categories: Optional[List[str]] = None,
    data_quality: Optional[Dict[str, Any]] = None,
    log_fn: Optional[Any] = None,
) -> Dict[str, Any]:
    """对预测结果应用"类别筛选 + top-N 截断"。

    返回（可能修改后的）result。预设为 off 或 categories 为空 / top_n<=0 时原样返回（无副作用）。

    启用类别的子列表保留、其余清空；启用类别内按"类别优先级 + 分数降序"合并后
    截断 top_n，再按原类别放回各自子列表 —— 保证 GUI / 竞价确认 读取的字段结构不变。
    """
    import os

    # 环境变量可覆盖预设（不改代码即可启用，便于 Codex 审阅后一键切换）
    env_preset = os.environ.get("LIMITUP_QUALITY_GATE")
    if env_preset:
        preset = env_preset

    cfg = _resolve_config(preset, top_n, categories)
    if not cfg:
        # 预设拼写错误时静默关闭会让人误以为已启用，这里显式提示一次
        if preset and preset != "off" and preset not in QUALITY_GATE_PRESETS:
            valid = ", ".join(k for k in QUALITY_GATE_PRESETS if k != "off")
            msg = f"候选质量门：未知预设 '{preset}'（可选: {valid}），本次不启用"
            if log_fn:
                log_fn(msg)
            else:
                import logging

                logging.getLogger(__name__).warning(msg)
        return result

    enabled = [c for c in cfg["categories"] if c in CATEGORY_RESULT_KEY]
    if not enabled:
        return result
    top_n = int(cfg.get("top_n") or 0)
    if top_n <= 0:
        return result

    # 收集启用类别的候选，记录原始类别
    tagged: List[Tuple[str, Dict[str, Any]]] = []
    for cat in enabled:
        key = CATEGORY_RESULT_KEY[cat]
        for item in (result.get(key) or []):
            if isinstance(item, dict):
                tagged.append((cat, item))

    before_total = sum(
        len(result.get(k) or []) for k in CATEGORY_RESULT_KEY.values()
    )

    # 主序=类别优先级（缓解跨类别分数错位），次序=分数降序
    tagged.sort(key=lambda t: (CATEGORY_PRIORITY[t[0]], -_candidate_score(t[1])))
    tagged = tagged[:top_n]

    # 重建子列表：仅保留启用类别，其余清空；保持原字段结构
    new_lists = {k: [] for k in CATEGORY_RESULT_KEY.values()}
    for cat, item in tagged:
        new_lists[CATEGORY_RESULT_KEY[cat]].append(item)
    for k in CATEGORY_RESULT_KEY.values():
        result[k] = new_lists[k]

    after_total = len(tagged)

    gate_info = {
        "enabled": True,
        "preset": preset,
        "categories": enabled,
        "top_n": top_n,
        "before_total": before_total,
        "after_total": after_total,
        "dropped": before_total - after_total,
    }
    if data_quality is not None and isinstance(data_quality, dict):
        data_quality["quality_gate"] = gate_info
    if log_fn:
        log_fn(
            f"候选质量门：{preset} 已启用，候选 {before_total} → {after_total}"
            f"（类别={','.join(enabled)}，top_n={top_n}）"
        )
    return result


if __name__ == "__main__":
    # 冒烟测试：构造假结果，验证截断与类别过滤、字段结构不变
    fake = {
        "broken_board_wrap_candidates": [
            {"code": f"w{i}", "score": 90 - i} for i in range(5)
        ],
        "first_board_candidates": [
            {"code": f"f{i}", "score": 80 - i} for i in range(5)
        ],
        "continuation_candidates": [
            {"code": f"c{i}", "score": 70 - i} for i in range(5)
        ],
        "fresh_first_board_candidates": [
            {"code": f"r{i}", "score": 60 - i} for i in range(5)
        ],
        "trend_limit_up_candidates": [
            {"code": f"t{i}", "score": 100 - i} for i in range(5)
        ],
    }
    out = apply_prediction_quality_gate(fake, preset="wrap_first_cont", top_n=5)
    kept = (
        [(c["code"], c["score"]) for c in out["broken_board_wrap_candidates"]]
        + [(c["code"], c["score"]) for c in out["first_board_candidates"]]
        + [(c["code"], c["score"]) for c in out["continuation_candidates"]]
    )
    print("kept (wrap,first,cont top5):", kept)
    print("fresh:", out["fresh_first_board_candidates"])
    print("trend:", out["trend_limit_up_candidates"])
    assert len(kept) == 5, f"期望截断到 5，实际 {len(kept)}"
    assert out["fresh_first_board_candidates"] == [], "非启用类别应清空"
    assert out["trend_limit_up_candidates"] == [], "非启用类别应清空"
    print("smoke OK")
