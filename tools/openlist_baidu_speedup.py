"""经 OpenList 管理 API 把百度网盘驱动提速参数真正写进库（#1 → #3 → #2 顺序）。

目标值：
  #1  low_bandwith_upload_mode  = False           （关闭低带宽模式 → 高速上传）
  #3  use_dynamic_upload_api    = True            （启用动态上传 API，单大文件更稳更快）
  #2  custom_upload_part_size   = 67108864 (64MB) （64MB 分片）

顺序要求：#1 → #3 → #2，每步「读当前 → 改一个字段 → update → reload」。
其余字段（含 upload_thread='1' 单线程，规避 31299）原样保留，绝不误改。
写完后回读确认三个目标值都已落库。容器重启由编排层负责（本脚本只负责写库+reload）。

运行（在 pan-transfer 容器内，已带 OPENLIST_BASE/OPENLIST_TOKEN 环境变量）：
  docker cp openlist_baidu_speedup.py pan-transfer:/tmp/_s.py
  docker exec pan-transfer python /tmp/_s.py
"""
import os
import sys
import json
import httpx

BASE = os.environ.get("OPENLIST_BASE", "http://openlist:5244").rstrip("/")
TOKEN = os.environ.get("OPENLIST_TOKEN", "")
if not TOKEN:
    print("NO_TOKEN"); sys.exit(2)

HEADERS = {"Authorization": TOKEN, "Content-Type": "application/json"}

# 顺序：#1 → #3 → #2
STEPS = [
    ("#1 low_bandwith_upload_mode=False", "low_bandwith_upload_mode", False),
    ("#3 use_dynamic_upload_api=True",    "use_dynamic_upload_api", True),
    ("#2 custom_upload_part_size=64MB",   "custom_upload_part_size", 67108864),
]


def get(path, params=None):
    return httpx.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=15)


def post(path, body):
    return httpx.post(f"{BASE}{path}", json=body, headers=HEADERS, timeout=15)


def find_baidu():
    r = get("/api/admin/storage/list", params={"page": 1, "per_page": 100})
    data = (r.json().get("data") or {}).get("content") or []
    for s in data:
        if s.get("driver") == "BaiduNetdisk" or s.get("mount_path") == "/baidu":
            return s
    return None


def main():
    baidu = find_baidu()
    if not baidu:
        print("NO_BAIDU_STORAGE"); sys.exit(3)
    sid = baidu["id"]
    print(f"[baidu] id={sid} mount={baidu.get('mount_path')} status={baidu.get('status')}")

    for label, key, val in STEPS:
        cur = find_baidu()
        add = json.loads(cur.get("addition") or "{}")
        before = add.get(key)
        add[key] = val
        obj = dict(cur)
        obj["addition"] = json.dumps(add, ensure_ascii=False)
        r = post("/api/admin/storage/update", obj)
        j = r.json()
        print(f"{label}: before={before!r} -> set={val!r} | update code={j.get('code')} msg={j.get('message')}")
        if j.get("code") != 200:
            print("  UPDATE_FAILED", r.text[:400])
        try:
            rr = post("/api/admin/storage/reload", {"id": sid})
            print(f"    reload code={rr.json().get('code')}")
        except Exception as e:
            print("    reload skipped:", e)

    after = find_baidu()
    add_after = json.loads(after.get("addition") or "{}")
    print("\n=== 回读确认 ===")
    ok = True
    for label, key, val in STEPS:
        got = add_after.get(key)
        flag = "OK" if got == val else "MISMATCH"
        if got != val:
            ok = False
        print(f"  {key:30} = {got!r:16} expect {val!r:16} [{flag}]")
    print("  upload_thread (应保持 '1') =", repr(add_after.get("upload_thread")))
    print("RESULT:", "ALL_APPLIED" if ok else "FAILED")


if __name__ == "__main__":
    main()
