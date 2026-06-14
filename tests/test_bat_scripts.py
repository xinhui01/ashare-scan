from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _script_text(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def test_update_cache_bat_runs_update_cache_command():
    text = _script_text("update_cache.bat")

    assert 'cd /d "%~dp0"' in text
    assert 'if exist "%~dp0.venv\\Scripts\\python.exe"' in text
    assert 'set "CACHE_ARGS=--max-stocks 0 --days 60 --workers 3 --source auto"' in text
    assert '"%PYTHON_EXE%" main.py update-cache %CACHE_ARGS% %*' in text
    assert 'if "%~1"=="" pause' in text


def test_start_app_bat_runs_gui_entrypoint():
    text = _script_text("start_app.bat")

    assert 'cd /d "%~dp0"' in text
    assert 'if exist "%~dp0.venv\\Scripts\\python.exe"' in text
    assert '"%PYTHON_EXE%" main.py %*' in text
    assert 'if "%~1"=="" pause' in text


def test_predict_today_bat_runs_predict_today_command():
    text = _script_text("predict_today.bat")

    assert 'cd /d "%~dp0"' in text
    assert 'if exist "%~dp0.venv\\Scripts\\python.exe"' in text
    assert 'set "PREDICT_ARGS=--lookback 5"' in text
    assert '"%PYTHON_EXE%" main.py predict-today %PREDICT_ARGS% %*' in text
    assert 'if "%~1"=="" pause' in text


def test_update_and_predict_bat_runs_combined_command():
    text = _script_text("update_and_predict.bat")

    assert 'cd /d "%~dp0"' in text
    assert 'if exist "%~dp0.venv\\Scripts\\python.exe"' in text
    assert 'set "CACHE_ARGS=--max-stocks 0 --days 60 --workers 3 --source auto"' in text
    assert 'set "PREDICT_ARGS=--lookback 5"' in text
    assert '"%PYTHON_EXE%" main.py update-and-predict %CACHE_ARGS% %PREDICT_ARGS% %*' in text
    assert 'if "%~1"=="" pause' in text


def test_run_tasks_bat_offers_a_menu_without_remembering_commands():
    text = _script_text("run_tasks.bat")

    assert "choice /c 12340" in text
    assert "call start_app.bat" in text
    assert "call update_cache.bat" in text
    assert "call predict_today.bat" in text
    assert "call update_and_predict.bat" in text
