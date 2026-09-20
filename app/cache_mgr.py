"""本机缓存管理：下载统计 + TTL/容量双闸清理。

- /dl 每次下载调用 record_download()：只更新内存计数（零磁盘 I/O），落盘由 flush() 周期完成；
- cleanup_once()：
  1) TTL 闸：最近下载时间（无记录时用文件 mtime）超过 CACHE_TTL_DAYS 的文件删除；
  2) 容量闸：目录总大小超过 CACHE_MAX_GB 时，按「最久未下载」逐个删除直到达标；
  3) 顺手清理状态文件里已无对应文件的孤儿记录；
  4) 落盘内存计数（flush）。
- 后台线程由 main.lifespan 启动，每 10 分钟跑一轮；进程退出时再 flush 一次。
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

from config import settings

_lock = threading.Lock()
# 内存计数（运行期权威值）：record_download 只写内存，零磁盘 I/O；
# 周期 flush() 落盘，避免高并发每次下载都「读+写磁盘 + 全局锁」成为瓶颈。
# 启动时从磁盘快照初始化（init_cache），保证进程重启后下载次数不丢。
_mem: dict = {}
# (客户端, 文件名) -> 最近一次计数时间，用于短时去重（防刷新/多线程刷爆计数）
_recent: dict = {}


def _state_file() -> Path:
    db = Path(settings.db_path or "/data/app/tasks.db")
    return db.parent / "dl_state.json"


def _load_state() -> dict:
    try:
        with open(_state_file(), encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_state(state: dict) -> None:
    p = _state_file()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    os.replace(tmp, p)


def init_cache() -> None:
    """启动时把磁盘计数载入内存（进程重启保留下载次数）。"""
    global _mem
    with _lock:
        _mem = _load_state()


def record_download(filename: str, client: str = "", dedup_seconds: float = 60.0) -> None:
    """/dl 下载一次记一次：只更新内存计数，零磁盘 I/O；落盘由 flush() 周期完成。

    计数噪声治理：浏览器/播放器下大文件是分片续传（Range 请求），一个电影能刷
    几百次「下载」。调用方只对「无 Range 的完整请求」调用本函数；此外这里再做
    一层 (客户端, 文件名) 短时去重，避免同一人反复刷新把计数刷爆。
    """
    now = time.time()
    with _lock:
        if client and dedup_seconds > 0:
            k = (client, filename)
            if now - _recent.get(k, 0.0) < dedup_seconds:
                return
            _recent[k] = now
            if len(_recent) > 4096:  # 防内存无界增长：定期清过期项
                cutoff = now - dedup_seconds
                for kk in [x for x, ts in _recent.items() if ts < cutoff]:
                    _recent.pop(kk, None)
        entry = _mem.get(filename) or {"last": 0, "count": 0}
        entry["last"] = now
        entry["count"] = int(entry.get("count") or 0) + 1
        _mem[filename] = entry


def _iter_files() -> list[tuple[str, Path]]:
    """递归枚举「转存」目录下全部文件，返回 [(相对 posix 路径, Path)]（按路径排序）。

    源分享整体含文件夹时，OpenList 会把目录结构原样复制进来（转存/风暴MOD/game.apk），
    顶层 iterdir 的平铺假设会把这些文件全部漏掉——清单、统计、TTL/容量清理、
    手动删除都必须走这里，且 key 用相对路径（与 /dl 计数 _mem 的 key 同源）。
    """
    work = _work_dir()
    if not work.is_dir():
        return []
    rows: list[tuple[str, Path]] = []
    for p in work.rglob("*"):
        try:
            if p.is_file():
                rows.append((p.relative_to(work).as_posix(), p))
        except OSError:
            continue          # 枚举瞬间被清理线程删掉：跳过
    rows.sort(key=lambda x: x[0])
    return rows


def list_files() -> list[dict]:
    """本机「转存」目录的文件清单（相对路径/大小/落盘时间/下载次数/最近下载）。

    供「本机文件管理页」展示；下载次数取自内存计数 _mem（与 /dl 记录同源，
    key 为相对路径——含子目录的文件形如「风暴MOD/game.apk」）。
    """
    rows: list[dict] = []
    for rel, p in _iter_files():
        try:
            st = p.stat()
        except OSError:
            continue
        mem = _mem.get(rel) or {}
        rows.append({
            "name": rel,
            "size": st.st_size,
            "mtime": st.st_mtime,
            "count": int(mem.get("count") or 0),
            "last": float(mem.get("last") or 0.0),
        })
    return rows


def delete_file(name: str) -> bool:
    """删除本机缓存中的单个文件（支持子目录相对路径，如「风暴MOD/game.apk」）。

    防穿越：拒绝绝对路径与「..」段，且解析后的真实路径必须仍在转存目录内
    （符号链接等意外形态在 resolve 一并校验）。空目录壳留给容量清理兜底。
    """
    if not name or name in (".", ".."):
        return False
    rel = Path(name)
    if rel.is_absolute() or ".." in rel.parts:
        return False
    work = _work_dir()
    p = work / rel
    try:
        inside = os.path.normcase(str(p.resolve())).startswith(
            os.path.normcase(str(work.resolve())) + os.sep)
    except (OSError, ValueError):
        return False
    if not inside or not p.is_file():
        return False
    p.unlink(missing_ok=True)
    with _lock:
        _mem.pop(rel.as_posix(), None)
    return True


def flush() -> None:
    """把内存计数落盘（进程退出或每轮清理时调用）。"""
    with _lock:
        _flush_nolock()


def _flush_nolock() -> None:
    """flush 的内部实现：不加锁。仅由已持有 _lock 的调用方（cleanup_once）使用，
    避免 threading.Lock 不可重入导致的死锁。

    注意：**即使 _mem 为空也要写盘**。否则「清理完孤儿后内存变空」时，磁盘上的旧
    快照不会被覆盖，下次启动 init_cache 会把孤儿计数又读回来（与真实文件不符）。
    """
    _save_state(dict(_mem))


def _work_dir() -> Path:
    return Path(settings.local_dir) / settings.transfer_dir


def cleanup_once() -> dict:
    """跑一轮清理，返回统计（供日志/状态接口）。"""
    result = {"deleted": [], "total_bytes": 0}
    with _lock:
        # 递归枚举（含子目录）——平铺假设会让嵌套文件永久逃过 TTL/容量清理
        meta: dict[Path, tuple[str, float, int]] = {}   # Path -> (rel, mtime, size)
        for rel, p in _iter_files():
            try:
                st = p.stat()
                meta[p] = (rel, st.st_mtime, st.st_size)
            except OSError:
                pass
        files = list(meta)
        now = time.time()

        def last_used(p: Path) -> float:
            # 内存计数优先（更准、更新），否则退回文件 mtime
            mem = _mem.get(meta[p][0])
            if mem and mem.get("last"):
                return float(mem["last"])
            return float(meta[p][1])

        # ① TTL 闸
        ttl = max(settings.cache_ttl_days, 0) * 86400
        for p in list(files):
            if ttl and now - last_used(p) > ttl:
                p.unlink(missing_ok=True)
                result["deleted"].append(meta[p][0])
                files.remove(p)

        # ② 容量闸：按最久未下载淘汰
        max_bytes = max(settings.cache_max_gb, 0) * 2**30
        total = sum(meta[p][2] for p in files)
        if max_bytes:
            for p in sorted(files, key=last_used):
                if total <= max_bytes:
                    break
                total -= meta[p][2]
                p.unlink(missing_ok=True)
                result["deleted"].append(meta[p][0])
                files.remove(p)

        # ③ 状态孤儿清理（子目录空壳目录一并删掉，别留空文件夹尸体）
        alive = {meta[p][0] for p in files}
        for name in list(_mem):
            if name not in alive:
                _mem.pop(name, None)
        _flush_nolock()  # 已持有 _lock，调用无锁版本，避免重复加锁死锁
        _prune_empty_dirs()

        result["total_bytes"] = sum(meta[p][2] for p in files)
        result["files"] = len(files)
    return result


def _prune_empty_dirs() -> None:
    """删掉转存目录下因文件被清理而空掉的子目录（顶层转存目录本身保留）。"""
    work = _work_dir()
    if not work.is_dir():
        return
    for d in sorted((p for p in work.rglob("*") if p.is_dir()),
                    key=lambda x: len(x.parts), reverse=True):
        try:
            d.rmdir()      # 仅当目录为空才成功（rmdir 语义即所需）
        except OSError:
            pass


def stats() -> dict:
    """当前缓存概况（管理员状态接口用）。

    注意：listdir 与 stat 之间文件可能正被缓存线程删掉 → 必须容忍 OSError，
    否则一次竞态就会把 /api/status 打成 500。
    """
    n = 0
    total = 0
    for _, p in _iter_files():
        try:
            total += p.stat().st_size
            n += 1
        except OSError:
            continue          # 刚好被清理线程删除：跳过即可
    return {"files": n, "total_bytes": total,
            "ttl_days": settings.cache_ttl_days,
            "max_gb": settings.cache_max_gb}


def start_cache_thread(stop_event: threading.Event, interval: float = 600.0):
    """后台清理线程：启动即跑一轮，之后按 interval 循环。daemon=True 随主进程退出。"""
    init_cache()

    def _loop():
        while not stop_event.is_set():
            try:
                r = cleanup_once()
                if r["deleted"]:
                    print(f"[cache] 已清理 {len(r['deleted'])} 个过期文件: {r['deleted'][:5]}")
            except Exception as e:
                print(f"[cache] 清理失败: {e}")
            stop_event.wait(interval)
        # 进程退出前再落盘一次，避免内存计数丢失
        try:
            flush()
        except Exception:
            pass

    th = threading.Thread(target=_loop, name="cache-mgr", daemon=True)
    th.start()
    return th
