from src.utils.cache_freshness import history_meta_requires_repair


def test_history_meta_requires_repair_for_legacy_tencent_cache():
    assert history_meta_requires_repair(
        {"source": "tencent", "partial_fields": "", "needs_repair": 0}
    ) is True


def test_history_meta_requires_repair_respects_explicit_partial_flags():
    assert history_meta_requires_repair(
        {"source": "sina", "partial_fields": "amount", "needs_repair": 0}
    ) is True
    assert history_meta_requires_repair(
        {"source": "sina", "partial_fields": "", "needs_repair": 0}
    ) is False
