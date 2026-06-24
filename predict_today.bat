@echo off
setlocal
cd /d "%~dp0"

if exist "%~dp0_set_proxy.bat" call "%~dp0_set_proxy.bat"

set "PYTHON_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

set "PREDICT_ARGS=--lookback 25"

"%PYTHON_EXE%" main.py predict-today %PREDICT_ARGS% %*
set "RC=%ERRORLEVEL%"
if "%~1"=="" pause
exit /b %RC%
