"""本机东财资金流连通性诊断（无需代理，纯直连）。

用法（在本机命令行执行）：
    cd D:\code\gupiao
    .venv\Scripts\python.exe _diag_em.py

它会逐层排查 eastmoney 资金流接口为什么连不上：
  L1 DNS 解析   L2 TCP 443 连接   L3 HTTPS 请求(裸UA/完整浏览器头/带cookie)
并与同花顺、腾讯对照，最后给出"卡在哪一层"的判定。
"""
import os
import socket
import ssl
import sys

# 清掉任何残留代理，确保是真实直连
for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"):
    os.environ.pop(k, None)

import requests


def line(t=""):
    print(t)


def dns_resolve(host):
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        ips = sorted({i[4][0] for i in infos})
        return True, ips
    except Exception as e:
        return False, str(e)


def tcp_connect(host, port=443, timeout=6):
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True, None
    except Exception as e:
        return False, type(e).__name__ + ": " + str(e)


def https_get(label, url, headers, timeout=(5, 12)):
    try:
        r = requests.get(url, headers=headers, timeout=timeout, verify=True)
        return True, f"HTTP {r.status_code} len={len(r.text)}"
    except Exception as e:
        return False, type(e).__name__ + ": " + str(e)[:160]


def main():
    line("=" * 60)
    line("东财资金流接口 本机连通性诊断")
    line("=" * 60)

    em_hosts = [
        "push2his.eastmoney.com",
        "push2.eastmoney.com",
        "quote.eastmoney.com",
    ]
    browser_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://quote.eastmoney.com/",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }
    fflow_url = (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
        "?lmt=1&klt=101&secid=1.600036"
        "&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
        "&ut=b2884a393a59ad64002292a3e90d46a5"
    )

    line()
    line("[L1] DNS 解析")
    dns_ok_all = True
    for h in em_hosts:
        ok, info = dns_resolve(h)
        line(f"  {h:28s} {'OK ' + str(info) if ok else 'FAIL ' + info}")
        dns_ok_all = dns_ok_all and ok

    line()
    line("[L2] TCP 443 连接")
    tcp_ok_any = False
    for h in em_hosts:
        ok, info = tcp_connect(h)
        line(f"  {h:28s} {'OPEN' if ok else 'CLOSED ' + info}")
        tcp_ok_any = tcp_ok_any or ok

    line()
    line("[L3] HTTPS 请求 东财资金流接口")
    # 裸 UA
    ok, info = https_get("裸UA", fflow_url, {"User-Agent": "Mozilla/5.0"})
    line(f"  裸UA        : {'OK ' + info if ok else 'FAIL ' + info}")
    # 完整浏览器头
    ok, info = https_get("浏览器头", fflow_url, browser_headers)
    line(f"  浏览器头    : {'OK ' + info if ok else 'FAIL ' + info}")
    # 先拿 cookie 再带 cookie 请求
    try:
        s = requests.Session()
        s.get("https://quote.eastmoney.com/", headers=browser_headers, timeout=(5, 12))
        ok, info = https_get("带cookie", fflow_url, browser_headers, timeout=(5, 12))
        line(f"  带cookie    : {'OK ' + info if ok else 'FAIL ' + info}")
    except Exception as e:
        line(f"  带cookie    : SKIP 取cookie失败 {type(e).__name__}: {e}")
    # 新逻辑（真实会话 Cookie + 完整头 + 多 host 兜底）权威判定——即程序实际使用的请求方式
    try:
        from src.sources.eastmoney.fund_flow import _em_fund_flow_reachable

        ok = _em_fund_flow_reachable()
        line(f"  新逻辑(真实cookie+多host): {'OK 东财可达' if ok else 'FAIL 仍不可达（大概率网络层限制）'}")
    except Exception as e:
        line(f"  新逻辑    : SKIP {type(e).__name__}: {e}")

    line()
    line("[对照] 其它数据源是否通（排除本机整体断网）")
    for label, url, hdr in [
        ("同花顺", "http://data.10jqka.com.cn/funds/ggzjl/", browser_headers),
        ("腾讯行情", "https://qt.gtimg.cn/q=sz000938", {"User-Agent": "Mozilla/5.0"}),
        ("东财首页", "https://www.eastmoney.com/", browser_headers),
    ]:
        ok, info = https_get(label, url, hdr, timeout=(5, 12))
        line(f"  {label:10s}: {'OK ' + info if ok else 'FAIL ' + info}")

    line()
    line("=" * 60)
    line("判定")
    line("=" * 60)
    if not dns_ok_all:
        line("• DNS 解析失败：本机 DNS 无法解析 eastmoney，检查 /etc/hosts、DNS 设置或网络。")
    elif not tcp_ok_any:
        line("• TCP 层被掐：能解析但连不上 443。典型是 ISP/防火墙/公司网络对 eastmoney 的")
        line("  TCP 限制（或 GFW 对该 IP 的 reset）。代码层无法绕过，只能依赖同花顺兜底。")
    else:
        line("• TCP 通但 HTTPS 失败：能连上 443，请求被服务端 reset/拒绝。可能是东财风控")
        line("  （缺 cookie/token 或 UA 校验）。可尝试在代码里为东财请求补充 cookie/UA 重试。")
    line("• 若同花顺/腾讯通而东财不通：排除整体断网，问题集中在 eastmoney 这一条链路。")
    line("• 若三者都不通：本机整体无外网出口（公司内网/网关限制），与代码无关。")
    line("=" * 60)


if __name__ == "__main__":
    main()
