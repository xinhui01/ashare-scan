@echo off
setlocal
cd /d "%~dp0"

REM 可选：旧代理兼容（项目现已全直连，无 _set_proxy.bat 也不影响）
if exist "%~dp0_set_proxy.bat" call "%~dp0_set_proxy.bat"

set "PYTHON_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

echo [1/2] 拉取最新预测快照 (git pull)...
git pull --ff-only
if errorlevel 1 (
    echo [提示] git pull 失败（可能无网络/未配 SSH key），将使用本地已有快照继续。
)

echo [2/2] 执行命令行竞价确认...
"%PYTHON_EXE%" "%~dp0scripts\cli_opening_confirm.py" %*
set "RC=%ERRORLEVEL%"

if "%~1"=="" pause
exit /b %RC%
