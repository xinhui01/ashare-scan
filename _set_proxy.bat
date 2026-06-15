@echo off
rem ---------------------------------------------------------------------
rem  Adaptive network egress for the stock app.
rem  If local Clash (127.0.0.1:7897) is up  -> route traffic through Clash.
rem  If it is not running                    -> clear proxy and go direct.
rem
rem  Scope: only affects the calling bat's process (it already did setlocal),
rem  so the global environment is untouched and other apps keep their proxy.
rem  Called via `call` from each launcher; intentionally NO setlocal here,
rem  so that `set` propagates back to the caller.
rem ---------------------------------------------------------------------
set "CLASH_PROXY=http://127.0.0.1:7897"

powershell -NoProfile -Command "try{$c=New-Object Net.Sockets.TcpClient;$c.Connect('127.0.0.1',7897);$c.Close()}catch{exit 1}" >nul 2>&1
if errorlevel 1 (
    set "HTTP_PROXY="
    set "HTTPS_PROXY="
    set "http_proxy="
    set "https_proxy="
    echo [proxy] Clash 7897 not running - direct connection
) else (
    set "HTTP_PROXY=%CLASH_PROXY%"
    set "HTTPS_PROXY=%CLASH_PROXY%"
    set "http_proxy=%CLASH_PROXY%"
    set "https_proxy=%CLASH_PROXY%"
    echo [proxy] Clash 7897 detected - routing via Clash
)
