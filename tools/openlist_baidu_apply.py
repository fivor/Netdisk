"""经 OpenList 管理 API 把百度网盘驱动的「大文件友好」参数真正写进库。

目标值（之前的热改想设但曾被误报「未落库」，本次以 API 方式正式写入并验证）：
  - upload_thread             = '1'   （单线程上传，规避 31299 / 闪退）
  - low_bandwith_upload_mode  = True  （低带宽模式，更稳）
  - custom_upload_part_size   = 33554432  （32 MB 分片，降低分片数，规避百度分片上限）

流程：GET 当前存储完整对象 → 仅改 addition 三个字段（其余原样保留）→
POST /api/admin/storage/update 写回 → reload 让运行中驱动加载 → 再 GET 回读确认。
幂等：若已是目标值，重写不产生副作用，但会触发 reload 保证生效。
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

TARGET = {
    "upload_thread": "1",
    "low_bandwith_upload_mode": True,
    "custom_upload_part_size": 33554432,
}


def get(path, params=None):
    return httpx.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=15)


def post(path, json_body):
    return httpx.post(f"{BASE}{path}", json=json_body, headers=HEADERS, timeout=15)


def find_baidu():
    r = get("/api/admin/storage/list", params={"page": 1, "per_page": 100})
    j = r.json()
    for s in (j.get("data") or {}).get("content") or []:
        if s.get("driver") == "BaiduNetdisk" or s.get("mount_path") == "/baidu":
            return s
    return None


def main():
    baidu = find_baidu()
    if not baidu:
        print("NO_BAIDU_STORAGE"); sys.exit(3)
    sid = baidu["id"]
    print(f"[before] id={sid} mount={baidu.get('mount_path')} status={baidu.get('status')}")
    add_before = json.loads(baidu.get("addition") or "{}")
    print("  before upload_thread        =", repr(add_before.get("upload_thread")))
    print("  before low_bandwith_upload =", repr(add_before.get("low_bandwith_upload_mode")))
    print("  before custom_part_size    =", repr(add_before.get("custom_upload_part_size")))

    # 仅改 addition 三个字段，其余原样保留
    add = dict(add_before)
    add.update(TARGET)
    obj = dict(baidu)
    obj["addition"] = json.dumps(add, ensure_ascii=False)

    r = post("/api/admin/storage/update", obj)
    print(f"[update] status={r.status_code} code={r.json().get('code')} msg={r.json().get('message')}")
    if r.json().get("code") != 200:
        print("UPDATE_FAILED", r.text[:500]); sys.exit(4)

    # 强制 reload 让运行中的驱动加载新 addition
    try:
        rr = post("/api/admin/storage/reload", {"id": sid})
        print(f"[reload] status={rr.status_code} code={rr.json().get('code')} msg={rr.json().get('message')}")
    except Exception as e:
        print("[reload] skipped:", e)

    # 回读确认
    after = find_baidu()
    add_after = json.loads(after.get("addition") or "{}")
    print(f"[after ] id={after['id']} status={after.get('status')}")
    ok = True
    for k, v in TARGET.items():
        got = add_after.get(k)
        flag = "OK" if got == v else "MISMATCH"
        if got != v:
            ok = False
        print(f"  {k:28} = {got!r:14} expect {v!r:14} [{flag}]")
    print("RESULT:", "ALL_TARGET_VALUES_PRESENT" if ok else "FAILED")


if __name__ == "__main__":
    main()
