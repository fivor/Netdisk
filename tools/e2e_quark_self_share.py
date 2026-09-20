"""端到端校验：夸克「自己的分享」能否定位到盘内文件（只读，不转存不上传）。

用途：回归 2026-09-18 的故障 —— 有效链接却被报
「自己的分享：盘内未定位到 N 个文件（可能已被删除）」。
根因是旧实现用整盘 BFS 找 fid，撞上 3000 条扫描上限；
现改为 GET /file/info?fid= 拿 pdir_fid 自底向上拼路径。

运行（需容器内真实 Cookie 环境变量）：
    docker cp tools/e2e_quark_self_share.py pan-transfer:/tmp/e2e.py
    docker exec pan-transfer python /tmp/e2e.py [share_id]
"""
import asyncio
import sys
import time

sys.path.insert(0, "/app")

from adapters.quark import QuarkAdapter  # noqa: E402
from config import settings  # noqa: E402

DEFAULT_SHARE_ID = "fa9cec21b0dd"


async def main() -> int:
    share_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SHARE_ID
    a = QuarkAdapter(settings)
    if not a.configured():
        print("✗ 夸克 Cookie 未配置，无法校验")
        return 2
    c = a.client()
    h = a._headers()

    r = await c.post(f"{a.BASE}/share/sharepage/token", params=a.PR,
                     json={"pwd_id": share_id, "passcode": ""}, headers=h)
    d = await a._check(r)
    stoken = d.get("stoken") or ""
    print(f"[token] stoken={'有' if stoken else '无'}")

    r = await c.get(f"{a.BASE}/share/sharepage/detail",
                    params={**a.PR, "pwd_id": share_id, "passcode": "",
                            "stoken": stoken, "pdir_fid": "0", "_page": 1,
                            "_size": 50, "_fetch_banner": 1, "_fetch_share": 1,
                            "_fetch_total": 1,
                            "_sort": "file_type:asc,updated_at:desc"},
                    headers=h)
    data = await a._check(r)
    lst = data.get("list") or []
    print(f"[detail] 条目 {len(lst)} 个，is_owner={data.get('is_owner')}")
    for f in lst[:10]:
        print(f"    - {f.get('file_name')} (fid={f.get('fid')}, dir={f.get('dir')})")

    fids = [f["fid"] for f in lst if f.get("fid")]
    names = [f.get("file_name") or "" for f in lst if f.get("fid")]
    if not fids:
        print("✗ 分享内没有可定位的文件")
        return 1

    print("[locate] 自底向上定位中 …")
    t0 = time.time()
    ref = await a._self_share_ref(
        c, lst, fids, names,
        lambda p, m: print(f"    [{p}%] {m}"))
    cost = time.time() - t0
    print(f"✓ 定位成功：目录={ref.mount_dir} 文件数={len(ref.refs)} 耗时={cost:.2f}s")
    for n in names[:10]:
        print(f"    · {n}")
    return 0


sys.exit(asyncio.run(main()))
