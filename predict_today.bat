@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

set "PREDICT_ARGS=--lookback 5"

"%PYTHON_EXE%" main.py predict-today %PREDICT_ARGS% %*
set "RC=%ERRORLEVEL%"
if "%~1"=="" pause
exit /b %RC%
