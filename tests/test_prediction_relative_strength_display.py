import inspect

from src.gui.tabs.predict import PredictTab
from src.services import prediction_excel_export_service as export_service


def test_prediction_candidate_tables_show_relative_strength_column():
    source = inspect.getsource(PredictTab)

    assert '"relative_strength"' in source
    assert '"强弱分"' in source
    assert "relative_strength_score" in source


def test_prediction_excel_exports_relative_strength_column():
    specs = export_service.CANDIDATE_SPECS

    for _sheet, _key, columns in specs:
        headers = [label for _field, label in columns]
        fields = [field for field, _label in columns]
        assert "强弱分" in headers
        assert "relative_strength_score" in fields
