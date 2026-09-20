"""cache_mgr 目录支持回归测试（2026-09-20 实锤场景）。

源分享整体含文件夹时，OpenList 会把目录结构原样复制进「转存」目录
（转存/风暴MOD/game.apk）。旧的平铺假设（顶层 iterdir + 非 file 即跳过）
导致：清单里看不见、统计不算数、TTL/容量清理永久漏掉、手动删除 404。
四个用例分别锁住这四种能力，key 一律为相对 posix 路径（与 /dl 计数同源）。
"""
import time
from pathlib import Path

import cache_mgr
from config import settings


def _isolated_work(tmp_path, monkeypatch) -> Path:
    """把 cache_mgr 的工作目录指到独立 tmp，避免与其它用例共享状态。"""
    monkeypatch.setattr(settings, "local_dir", str(tmp_path))
    monkeypatch.setattr(cache_mgr, "_mem", {})
    work = Path(settings.local_dir) / settings.transfer_dir
    work.mkdir(parents=True, exist_ok=True)
    return work


def test_list_and_stats_include_nested(tmp_path, monkeypatch):
    """清单与统计必须覆盖子目录文件，key 为相对路径。"""
    work = _isolated_work(tmp_path, monkeypatch)
    sub = work / "风暴MOD"
    sub.mkdir()
    (sub / "game.apk").write_bytes(b"X" * 2048)
    (work / "top.txt").write_bytes(b"Y")

    names = [r["name"] for r in cache_mgr.list_files()]
    assert "风暴MOD/game.apk" in names
    assert "top.txt" in names

    st = cache_mgr.stats()
    assert st["files"] == 2
    assert st["total_bytes"] >= 2048 + 1


def test_cleanup_ttl_covers_nested(tmp_path, monkeypatch):
    """TTL 闸必须能判死子目录文件，且顺带清掉空目录壳与孤儿计数。"""
    work = _isolated_work(tmp_path, monkeypatch)
    sub = work / "老MOD"
    sub.mkdir()
    (sub / "old.apk").write_bytes(b"Z")
    monkeypatch.setattr(cache_mgr, "_mem", {
        "老MOD/old.apk": {"count": 1, "last": time.time() - 400 * 86400}})

    r = cache_mgr.cleanup_once()
    assert "老MOD/old.apk" in r["deleted"]
    assert not (sub / "old.apk").exists()
    assert not sub.exists()                      # 空目录壳一并清掉
    assert cache_mgr._mem == {}                  # 孤儿计数清空
    assert r["files"] == 0 and r["total_bytes"] == 0


def test_delete_file_relative_and_traversal(tmp_path, monkeypatch):
    """删除支持相对路径；绝对路径与 .. 段一律拒绝。"""
    work = _isolated_work(tmp_path, monkeypatch)
    sub = work / "风暴MOD"
    sub.mkdir()
    (sub / "game.apk").write_bytes(b"G")
    monkeypatch.setattr(cache_mgr, "_mem",
                        {"风暴MOD/game.apk": {"count": 3, "last": 1.0}})

    assert cache_mgr.delete_file("风暴MOD/game.apk") is True
    assert not (sub / "game.apk").exists()
    assert "风暴MOD/game.apk" not in cache_mgr._mem
    # 穿越与绝对路径拒绝（Windows 盘符形态也在内）
    assert cache_mgr.delete_file("../outside.txt") is False
    assert cache_mgr.delete_file("C:/Windows/system.ini") is False
    assert cache_mgr.delete_file("") is False
    assert cache_mgr.delete_file("不存在.txt") is False
