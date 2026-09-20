"""只读探查：百度网盘存储在 OpenList 里的真实 addition 字段，以及驱动声明的可用参数名。

用于确认「之前热改没落库」的真相，并为后续真正写入正确的参数名做准备。
绝不修改任何数据。
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


def get(path, params=None):
    r = httpx.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=15)
    return r


def main():
    # 1) 列出所有存储，找到百度那个
    r = get("/api/admin/storage/list", params={"page": 1, "per_page": 100})
    print("LIST status", r.status_code)
    j = r.json()
    print("LIST code/message:", j.get("code"), j.get("message"))
    content = (j.get("data") or {}).get("content") or []
    baidu = None
    for s in content:
        if s.get("driver") == "BaiduNetdisk" or s.get("mount_path") == "/baidu":
            baidu = s
            break
    if not baidu:
        print("NO_BAIDU_STORAGE_FOUND. drivers seen:",
              sorted({s.get("driver") for s in content}))
        return
    print("\n=== BAIDU STORAGE (current) ===")
    print("id        :", baidu.get("id"))
    print("mount_path:", baidu.get("mount_path"))
    print("driver    :", baidu.get("driver"))
    print("status    :", baidu.get("status"))
    print("disabled  :", baidu.get("disabled"))
    print("order     :", baidu.get("order"))
    addition_raw = baidu.get("addition") or "{}"
    try:
        add = json.loads(addition_raw)
    except Exception as e:
        add = {"__parse_error__": str(e), "__raw__": addition_raw[:500]}
    print("addition keys:", sorted(add.keys()))
    # 重点关注的上传相关字段
    for k in ("upload_thread", "low_bandwith_upload_mode", "custom_upload_part_size",
              "thread", "part_size", "upload_part_size"):
        if k in add:
            print(f"  addition[{k!r}] = {add[k]!r}")

    # 2) 驱动声明的可用参数（权威字段名）
    r2 = get("/api/admin/driver/info", params={"driver": "BaiduNetdisk"})
    print("\nDRIVER_INFO status", r2.status_code)
    j2 = r2.json()
    print("DRIVER_INFO code/message:", j2.get("code"), j2.get("message"))
    data = j2.get("data") or {}
    items = data.get("additional") or data.get("config", {}).get("additional") or []
    print(f"\n=== DRIVER DECLARED CONFIG ITEMS ({len(items)}) ===")
    for it in items:
        name = it.get("name")
        if name and any(w in name.lower() for w in
                        ("upload", "part", "band", "thread", "chunk", "size")):
            print(f"  name={name!r:40} type={it.get('type'):8} default={it.get('default')!r}")
            if it.get("help"):
                print(f"      help: {it.get('help')}")


if __name__ == "__main__":
    main()
