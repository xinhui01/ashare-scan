@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

set "CACHE_ARGS=--max-stocks 0 --days 60 --workers 3 --source auto"
set "PREDICT_ARGS=--lookback 5"

"%PYTHON_EXE%" main.py update-and-predict %CACHE_ARGS% %PREDICT_ARGS% %*
set "RC=%ERRORLEVEL%"
if "%~1"=="" pause
exit /b %RC%
