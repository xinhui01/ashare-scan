"""
网络相关：SSL 校验见 USE_INSECURE_SSL / ASHARE_SCAN_INSECURE_SSL；
代理报错见 USE_BYPASS_PROXY / ASHARE_SCAN_BYPASS_PROXY（见 README）。
"""
import os
import platform
import sys
import tkinter as tk

# 单开关：True=不走系统代理（推荐，避免代理导致东财接口断连）；False=沿用系统代理
BYPASS_PROXY = True


def _check_runtime() -> None:
    is_macos = platform.system() == "Darwin"
    is_pyenv = ".pyenv" in (sys.executable or "")
    is_py313 = sys.version_info[:2] >= (3, 13)
    if is_macos and is_pyenv and is_py313:
        raise SystemExit(
            "当前运行环境是 macOS + pyenv Python 3.13，这个组合在 Tk GUI 下容易直接 bus error/abort。\n"
            "建议改用 Python 3.11/3.12 重新创建虚拟环境后再启动。"
        )


def _drop_dead_http_proxies() -> None:
    """清除指向死代理的 HTTP(S)_PROXY 环境变量。

    历史遗留：曾在 env 里手动设过 http://118.89.136.118:31283，
    但该代理早已下线，导致 requests/akshare 默认调用全部卡死。
    BYPASS_PROXY=True 已经能让项目内 Session 走 trust_env=False，
    但 pip / 其它子进程仍会读到这条 env，所以从根上清掉。
    """
    DEAD_HOSTS = ("118.89.136.118:31283",)
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"):
        val = os.environ.get(key, "")
        if any(h in val for h in DEAD_HOSTS):
            os.environ.pop(key, None)


def main():
    _check_runtime()
    _drop_dead_http_proxies()
    if BYPASS_PROXY:
        os.environ["ASHARE_SCAN_BYPASS_PROXY"] = "1"
    else:
        os.environ.pop("ASHARE_SCAN_BYPASS_PROXY", None)

    from stock_logger import get_logger
    logger = get_logger(__name__)
    logger.info("应用启动")

    from stock_gui import StockMonitorApp
    from stock_store import ensure_store_ready

    ensure_store_ready()
    root = tk.Tk()
    app = StockMonitorApp(root)
    logger.info("主窗口已初始化，进入主循环")
    root.mainloop()
    logger.info("应用退出")


if __name__ == "__main__":
    main()
