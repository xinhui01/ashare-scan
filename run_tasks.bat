@echo off
setlocal
cd /d "%~dp0"

:menu
cls
echo A-share task menu
echo.
echo 1. Update history cache
echo 2. Predict today
echo 3. Update cache, then predict
echo 4. Start app GUI
echo 0. Exit
echo.
choice /c 12340 /n /m "Choose a task: "

if errorlevel 5 goto end
if errorlevel 4 goto start_app
if errorlevel 3 goto update_and_predict
if errorlevel 2 goto predict_today
if errorlevel 1 goto update_cache

:update_cache
call update_cache.bat
goto again

:predict_today
call predict_today.bat
goto again

:update_and_predict
call update_and_predict.bat
goto again

:start_app
call start_app.bat
goto again

:again
echo.
choice /c yn /n /m "Run another task? [Y/N]: "
if errorlevel 2 goto end
goto menu

:end
exit /b 0
