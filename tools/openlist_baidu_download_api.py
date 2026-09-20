"""把百度网盘驱动的「下载接口」切到 crack（非限速通道），解决跨平台转存时百度侧下载慢。

背景（2026-09-18 实测）：百度 → 夸克 转存端到端只有 36 KB/s，而用户本机用百度客户端
下载很快。根因：OpenList 百度驱动默认 `download_api='official'`，走的是**被百度限速**的
官方开放接口；`'crack'` 会用 `custom_crack_ua` 伪装成百度 PC 客户端去取下载直链，
从而拿到不限速的链接。

合法取值（驱动模板实测）：`official | crack | crack_video`，默认 `official`。
`custom_crack_ua`（默认 'netdisk'）本脚本**原样保留不改**。

用法（在 pan-transfer 容器内，已带 OPENLIST_BASE/OPENLIST_TOKEN 环境变量）：
    docker cp openlist_baidu_download_api.py pan-transfer:/tmp/_d.py
    docker exec pan-transfer python /tmp/_d.py            # 默认切 crack
    docker exec pan-transfer python /tmp/_d.py official   # 切回官方限速接口

切完需重启 openlist 容器让驱动加载新参数（本脚本只写库 + reload）。
"""
import os
import sys
import json
import httpx

BASE = os.environ.get("OPENLIST_BASE", "http://openlist:5244").rstrip("/")
TOKEN = os.environ.get("OPENLIST_TOKEN", "")
if not TOKEN:
    print("NO_TOKEN")
    sys.exit(2)

HEADERS = {"Authorization": TOKEN, "Content-Type": "application/json"}
VALID = ("official", "crack", "crack_video")
TARGET = (sys.argv[1] if len(sys.argv) > 1 else "crack").strip()
if TARGET not in VALID:
    print("BAD_VALUE", TARGET, "| valid:", VALID)
    sys.exit(4)


def get(path, **kw):
    return httpx.get(f"{BASE}{path}", headers=HEADERS, timeout=20, **kw)


def post(path, body):
    return httpx.post(f"{BASE}{path}", json=body, headers=HEADERS, timeout=20)


def find_baidu():
    r = get("/api/admin/storage/list", params={"page": 1, "per_page": 100})
    for s in ((r.json().get("data") or {}).get("content") or []):
        if s.get("driver") == "BaiduNetdisk" or s.get("mount_path") == "/baidu":
            return s
    return None


def main():
    cur = find_baidu()
    if not cur:
        print("NO_BAIDU_STORAGE")
        sys.exit(3)
    sid = cur["id"]
    add = json.loads(cur.get("addition") or "{}")
    print(f"[baidu] id={sid} mount={cur.get('mount_path')}")
    print(f"download_api : {add.get('download_api')!r} -> {TARGET!r}")
    print(f"custom_crack_ua (保留不动) = {add.get('custom_crack_ua')!r}")

    add["download_api"] = TARGET
    obj = dict(cur)
    obj["addition"] = json.dumps(add, ensure_ascii=False)
    r = post("/api/admin/storage/update", obj)
    j = r.json()
    print("update code=", j.get("code"), "msg=", j.get("message"))
    if j.get("code") != 200:
        print("UPDATE_FAILED", r.text[:400])
        sys.exit(5)
    try:
        rr = post("/api/admin/storage/reload", {"id": sid})
        print("reload code=", rr.json().get("code"))
    except Exception as e:
        print("reload skipped:", e)

    after = json.loads(find_baidu().get("addition") or "{}")
    got = after.get("download_api")
    print(f"回读 download_api = {got!r}", "OK" if got == TARGET else "MISMATCH")
    print("RESULT:", "APPLIED" if got == TARGET else "FAILED")


if __name__ == "__main__":
    main()
