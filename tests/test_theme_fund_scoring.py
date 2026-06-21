from src.services.scoring.shared import theme_fund_bonus


def test_theme_fund_bonus_uses_code_theme_fund_context():
    bonus, reasons = theme_fund_bonus(
        "300001",
        "通用设备",
        {
            "code_theme_map": {"300001": "机器人"},
            "code_theme_fund_score": {"300001": 68},
            "theme_fund_accumulation_map": {"机器人": 18},
            "theme_breakout_map": {"机器人": 34},
        },
    )

    assert bonus == 6
    assert reasons == ["题材资金潜18/爆34+6"]
