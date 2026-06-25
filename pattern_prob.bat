@echo off
setlocal
cd /d "%~dp0"
chcp 65001 >nul

set "PYTHON_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"
set "OUT=%~dp0data\run_logs\pattern_limit_up_prob_report.txt"

echo [pattern-prob] Computing P(next-day limit-up ^| today's K-line shape)...
echo [pattern-prob] This scans the full history table, takes ~1-2 min.
echo.

"%PYTHON_EXE%" "%~dp0scripts\pattern_limit_up_prob.py" > "%OUT%" 2>&1
set "RC=%ERRORLEVEL%"

type "%OUT%"
echo.
echo ----------------------------------------------------------------
echo [pattern-prob] exit code: %RC%
echo [pattern-prob] report saved (UTF-8): %OUT%
echo [pattern-prob] If Chinese looks garbled in this console, open the file above in VSCode/editor.

if "%~1"=="" pause
exit /b %RC%
