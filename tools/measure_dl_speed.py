"""测量 /dl 下载链接经 CF 隧道的实际吞吐（直连，绕过系统代理/Clash）。

用法：
    python tools/measure_dl_speed.py <完整下载链接> [每次取样MB] [样本数]

例：
    python tools/measure_dl_speed.py "https://your-domain.example/dl/<token>" 8 3

说明：
- 强制直连（ProxyHandler({})），不受系统代理/Clash 影响；
- 需要浏览器 UA（CF Bot 防护会 403 掉 Python 默认 UA）；
- 服务端 /dl 支持 Range，样本只拉前 N MB；
- 用于对比 cloudflared --protocol http2 / quic 的实际差异。
"""
import sys
import time
import urllib.request

URL = sys.argv[1] if len(sys.argv) > 1 else ""
MB = float(sys.argv[2]) if len(sys.argv) > 2 else 8
N = int(sys.argv[3]) if len(sys.argv) > 3 else 3
SIZE = int(MB * 2**20)

HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "*/*",
}


def once(i: int) -> float:
    t0 = time.time()
    req = urllib.request.Request(URL, headers={**HDRS, "Range": f"bytes=0-{SIZE - 1}"})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=300) as r:
        n = len(r.read(SIZE))
    dt = time.time() - t0
    print(f"  样本{i}: {n / 2**20:.0f}MB / {dt:.2f}s = {n / 2**20 / dt:.2f} MB/s")
    return n / 2**20 / dt


if not URL:
    print(__doc__)
    raise SystemExit(1)

print(f"测量 {N} 样本 × {MB:.0f}MB（直连）：")
rates = [once(i + 1) for i in range(N)]
print(f"均值: {sum(rates) / len(rates):.2f} MB/s")
