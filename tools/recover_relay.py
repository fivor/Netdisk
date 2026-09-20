"""补齐「复制已完成、但任务记录没终结」的中转任务：校验目标 → 生成分享 → 写回 success。

为什么需要它：跨平台中转期间如果应用被重启/被冻结，我们的调度线程会消失，而
**OpenList 侧的复制其实可能已经完成**（文件已在目标盘「转存」目录下）。这时任务
记录会停在 running/failed，用户拿不到分享链接。本脚本只做三件事：
1. 用与线上同一套 `RelayMeter` **只读校验**目标目录（文件数 + 大小逐项比对）；
2. 通过目标平台适配器 `locate + share` **生成一次分享链接**；
3. 把任务记录改成 success（可用 --dry-run 跳过第 3 步）。
**不重传、不删除任何数据。**

用法（在 pan-transfer 容器内执行）：
    docker cp tools/recover_relay.py pan-transfer:/tmp/recover_relay.py
    docker exec pan-transfer python /tmp/recover_relay.py \
        --task-id ba025a31211d --item "【巫师3】Mod整合" \
        --src-dir "/quark/微信分享/游戏"            # 可选，给了才做逐文件校验
    # 想先看结果不写库：加 --dry-run

`--item` 取「源分享里的顶层项名」（OpenList 任务名 `copy [/quark](/…/A/B) to
[/baidu](/转存/A)` 里的 `A`）；`--dst-dir` 默认 = 目标挂载 + transfer_dir。
"""
from __future__ import annotations

import argparse
import asyncio
import sys

sys.path.insert(0, "/app")

import httpx
from adapters.base import build_registry
from config import settings
from db import TaskStore
from http_client import new_client
from openlist import OpenListClient
from relay_meter import RelayMeter


def _gb(n: float) -> str:
    n = float(n or 0)
    for unit, div in (("TB", 2 ** 40), ("GB", 2 ** 30), ("MB", 2 ** 20)):
        if n >= div:
            return f"{n / div:.2f} {unit}"
    return f"{n / 1024:.0f} KB"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--task-id", required=True)
    ap.add_argument("--item", action="append", required=True,
                    help="源分享里的顶层项名，可重复")
    ap.add_argument("--src-dir", default="",
                    help="源侧完整路径（如 /quark/微信分享/游戏），给了才做逐文件校验")
    ap.add_argument("--dst-dir", default="")
    ap.add_argument("--dry-run", action="store_true", help="只校验+生成链接，不写库")
    a = ap.parse_args()

    store = TaskStore(settings.db_path)
    task = store.get(a.task_id)
    if not task:
        print(f"任务 {a.task_id} 不存在", file=sys.stderr)
        return 2
    target = task.get("target") or ""
    mounts = settings.openlist_mounts
    dst_dir = a.dst_dir or (mounts.get(target, "").rstrip("/") + "/"
                            + settings.transfer_dir)
    print(f"任务 {a.task_id} 源={task.get('source')} 目标={target} 状态={task.get('status')}")
    print(f"目标目录 {dst_dir}；待校验项 {a.item}")

    ok = True
    if a.src_dir:
        ol = OpenListClient(settings.openlist_base, settings.openlist_token)
        with httpx.Client(trust_env=False, timeout=60) as c:
            m = RelayMeter(ol, c, a.src_dir, dst_dir, names=a.item,
                           src_mount=mounts.get(task.get("source") or "", ""),
                           dst_mount=mounts.get(target, ""))
            m.scan_source()
            print(f"[校验] 源侧 {len(m.files)} 个文件 / {_gb(m.total)}")
            ok, why = m.verify_destination()
            print("[校验]", "目标内容完整 ✓" if ok else f"目标不完整 ✗ {why}")
    else:
        print("[校验] 未提供 --src-dir：跳过逐文件校验（仅确认目标目录可访问）")
        with httpx.Client(trust_env=False, timeout=30) as c:
            ol = OpenListClient(settings.openlist_base, settings.openlist_token)
            rows = ol.list_dir(c, dst_dir)
            print(f"[校验] {dst_dir} 下有 {len(rows)} 个条目")
            ok = bool(rows)
    if not ok:
        print("校验未通过，不生成链接。", file=sys.stderr)
        return 3

    async def _link():
        async with new_client() as c:
            reg = build_registry(settings, client=c)
            ad = reg.get(target)
            if ad is None:
                raise SystemExit(f"未知目标平台 {target}")
            ref = await ad.locate(a.item, settings.transfer_dir, lambda p, m: None)
            return await ad.share(ref, lambda p, m: None)

    res = asyncio.run(_link())
    print(f"[分享] {res.new_url}  提取码：{res.password}")
    if a.dry_run:
        print("[db] --dry-run：未写库")
        return 0
    store.update(a.task_id, status="success", progress=100,
                 message="复制已完成（目的地校验通过），分享链接已补生成",
                 result_url=res.new_url or "", result_pwd=res.password)
    t = store.get(a.task_id)
    print(f"[db] {t['status']} | {t['result_url']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
