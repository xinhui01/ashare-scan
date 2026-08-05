@echo off
chcp 65001 >nul 2>&1
rem ---------------------------------------------------------------------
rem  Network egress policy: all direct connections (no proxy).
rem
rem  Clears HTTP(S)_PROXY inherited from parent process / system, so
rem  requests are not routed to a dead local proxy (e.g. Clash port 7897
rem  open but not serving -> ProxyError). This file only clears proxy
rem  vars, nothing else. Called by launcher bats via `call _set_proxy.bat`;
rem  Clash probing and proxy injection have been removed.
rem
rem  NOTE: keep this file ASCII-only. Long multibyte rem lines break the
rem  cmd.exe batch parser under codepage 65001 (fragments get executed).
rem ---------------------------------------------------------------------
set "HTTP_PROXY="
set "HTTPS_PROXY="
set "http_proxy="
set "https_proxy="
set "ALL_PROXY="
echo [proxy] direct mode: HTTP(S)_PROXY cleared, all requests go direct
