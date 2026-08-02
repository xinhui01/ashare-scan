@echo off
chcp 65001 >nul 2>&1
rem ---------------------------------------------------------------------
rem  网络出口策略：全部直连（不再走任何代理）。
rem
rem  清掉可能从父进程 / 系统继承来的 HTTP(S)_PROXY，避免请求被导向失效的
rem  代理（例如本地 Clash 7897 端口开着却未提供代理服务，导致 ProxyError）。
rem  本文件只做清代理动作，不改变其它环境变量；被各启动 bat 以
rem  `call _set_proxy.bat` 调用，但已不再做 Clash 探测与代理注入。
rem ---------------------------------------------------------------------
set "HTTP_PROXY="
set "HTTPS_PROXY="
set "http_proxy="
set "https_proxy="
set "ALL_PROXY="
echo [proxy] 直连模式：已清空 HTTP(S)_PROXY，所有请求走本地直连
