import threading

import pytest

from db import TaskStore


@pytest.fixture()
def store(tmp_path):
    return TaskStore(str(tmp_path / "tasks.db"))


def test_create_and_get(store):
    tid = store.create(source="baidu", target="baidu",
                       share_url="https://pan.baidu.com/s/1abc", password="abcd")
    t = store.get(tid)
    assert t["id"] == tid
    assert t["status"] == "pending"
    assert t["progress"] == 0
    assert t["password"] == "abcd"


def test_update_status_and_result(store):
    tid = store.create(source="quark", target="quark", share_url="https://x", password=None)
    store.update(tid, status="running", progress=40, message="转存中")
    t = store.get(tid)
    assert (t["status"], t["progress"], t["message"]) == ("running", 40, "转存中")
    store.update(tid, status="success", result_url="https://pan.quark.cn/s/new",
                 result_pwd=None, progress=100)
    t = store.get(tid)
    assert t["result_url"] == "https://pan.quark.cn/s/new"
    assert t["status"] == "success"


def test_update_rejects_bad_status(store):
    tid = store.create(source="ali", target="ali", share_url="https://x", password=None)
    with pytest.raises(ValueError):
        store.update(tid, status="doing")


def test_progress_detail_roundtrip(store):
    """progress_detail（下载/上传明细）字典落库再读回应保持原样。"""
    tid = store.create(source="quark", target="baidu", share_url="https://x", password=None)
    detail = {"relay": True, "dl_done": 23_000_000_000,
              "ul_done": 23_000_000_000, "total": 23_080_000_000, "total_files": 1}
    store.update(tid, status="running", progress_detail=detail)
    t = store.get(tid)
    assert t["progress_detail"] == detail
    # 不带 detail 的普通更新不应抹掉它
    store.update(tid, status="success", progress=100)
    assert store.get(tid)["progress_detail"] == detail
    # 缺列的老库读回应为空字典（_migrate 兜底）
    assert isinstance(store.get(tid)["progress_detail"], dict)


def test_progress_detail_default_empty(store):
    """未写入 progress_detail 的任务，读回应为空字典而非 None/字符串。"""
    tid = store.create(source="baidu", target="baidu", share_url="https://x", password=None)
    assert store.get(tid)["progress_detail"] == {}


def test_update_missing_task_raises(store):
    with pytest.raises(KeyError):
        store.update("nope", status="failed")


def test_list_order_desc(store):
    for i in range(3):
        store.create(source="baidu", target="baidu", share_url=f"https://x/{i}", password=None)
    tasks = store.list()
    assert len(tasks) == 3
    assert tasks[0]["created_at"] >= tasks[-1]["created_at"]


def test_counts_by_status(store):
    a = store.create(source="baidu", target="baidu", share_url="u", password=None)
    store.create(source="baidu", target="baidu", share_url="u", password=None)
    store.update(a, status="success")
    counts = store.counts_by_status()
    assert counts == {"pending": 1, "success": 1}


def test_thread_safety(tmp_path):
    """多线程并发写不应报错或丢任务（模拟 worker 并发更新）。"""
    store = TaskStore(str(tmp_path / "conc.db"))
    ids = [store.create(source="baidu", target="baidu", share_url="u", password=None)
           for _ in range(10)]

    def worker(tid):
        for p in range(0, 101, 20):
            store.update(tid, progress=p, message=f"{p}%")
        store.update(tid, status="success")

    threads = [threading.Thread(target=worker, args=(t,)) for t in ids]
    [t.start() for t in threads]
    [t.join() for t in threads]
    tasks = store.list(limit=100)
    assert all(t["status"] == "success" and t["progress"] == 100 for t in tasks)


# ---- 双角色：owner 归属与旧库迁移 ----

def test_owner_scoped_create_and_list(store):
    a = store.create(source="quark", target="quark", share_url="https://x/a",
                     password=None, owner_id="user-aaa")
    store.create(source="quark", target="quark", share_url="https://x/b",
                 password=None, owner_id="user-bbb")
    mine = store.list(owner_id="user-aaa")
    assert [t["id"] for t in mine] == [a]
    everything = store.list()
    assert len(everything) == 2


def test_owner_default_empty(store):
    tid = store.create(source="quark", target="quark", share_url="u", password=None)
    assert store.get(tid)["owner_id"] == ""


def test_reap_stuck_recovers_orphaned_tasks(store):
    """重启后：上轮 pending/running 任务（旧 worker 已消失）应被回收为 failed，
    终态任务（success/failed）不受影响，cancelled 等其它状态也不动。"""
    pending = store.create(source="baidu", target="baidu", share_url="u", password=None)
    running = store.create(source="quark", target="quark", share_url="u", password=None)
    store.update(running, status="running", progress=30)
    done = store.create(source="ali", target="ali", share_url="u", password=None)
    store.update(done, status="success", result_url="https://x")

    n = store.reap_stuck()
    assert n == 2

    assert store.get(pending)["status"] == "failed"
    assert store.get(running)["status"] == "failed"
    assert store.get(done)["status"] == "success"  # 终态不动
    # 回收后再次回收应为 0（幂等）
    assert store.reap_stuck() == 0


def test_dl_token_unique_and_lookup(store):
    """每个任务有独立 dl_token（32 位），可按 token 反查任务。"""
    a = store.create(source="quark", target="local", share_url="u", password=None)
    b = store.create(source="quark", target="local", share_url="u2", password=None)
    ta, tb = store.get(a), store.get(b)
    assert ta["dl_token"] and tb["dl_token"]
    assert ta["dl_token"] != tb["dl_token"]
    assert len(ta["dl_token"]) == 32
    assert store.get_by_dl_token(ta["dl_token"])["id"] == a
    assert store.get_by_dl_token("nope") is None
    assert store.get_by_dl_token("") is None


def test_find_recent(store):
    """同链接去重查询：命中窗口内相同 (源,目标,文本)，不匹配则返回 None。"""
    a = store.create(source="quark", target="baidu", share_url="same", password=None)
    hit = store.find_recent(source="quark", target="baidu", share_url="same",
                            within_seconds=600)
    assert hit and hit["id"] == a
    assert store.find_recent(source="quark", target="baidu", share_url="other",
                             within_seconds=600) is None
    assert store.find_recent(source="quark", target="uc", share_url="same",
                             within_seconds=600) is None


def test_migrate_legacy_db_without_owner(tmp_path):
    """旧版任务库（无 owner_id 列）打开后自动补列，旧数据不丢。"""
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, created_at REAL, updated_at REAL,
            source TEXT, target TEXT, share_url TEXT, password TEXT,
            status TEXT DEFAULT 'pending', progress INTEGER DEFAULT 0,
            message TEXT DEFAULT '', result_url TEXT, result_pwd TEXT, error TEXT
        );
        INSERT INTO tasks (id, created_at, updated_at, source, target, share_url,
                           status) VALUES ('old1', 1.0, 1.0, 'baidu', 'baidu', 'u', 'failed');
    """)
    conn.commit()
    conn.close()

    store = TaskStore(db_path)
    t = store.get("old1")
    assert t is not None and t["owner_id"] == ""
    assert t["dl_token"]  # 旧库迁移行自动回填下载凭证
    # 新任务可正常带 owner 写入
    tid = store.create(source="quark", target="quark", share_url="u",
                       password=None, owner_id="u1")
    assert store.get(tid)["owner_id"] == "u1"


def test_group_fields_roundtrip_and_legacy_default(tmp_path):
    """组合转存分组字段：写读往返、按 seq 取组、未分组任务与空组查询不受影响。"""
    store = TaskStore(str(tmp_path / "g.db"))
    a = store.create(source="quark", target="local", share_url="https://x",
                     password=None, owner_id="u", group_id="g1", group_seq=1,
                     created_at=100.0)
    b = store.create(source="quark", target="uc", share_url="https://x",
                     password=None, owner_id="u", group_id="g1", group_seq=2,
                     created_at=99.999)
    kids = store.list_group("g1")
    assert [k["id"] for k in kids] == [a, b]        # 按 group_seq 升序
    assert [k["group_seq"] for k in kids] == [1, 2]
    assert all(k["group_id"] == "g1" for k in kids)
    assert [k["target"] for k in kids] == ["local", "uc"]
    assert kids[1]["reuse_src"] is False

    store.update(b, reuse_src=True)
    assert store.get(b)["reuse_src"] is True
    store.update(b, reuse_src=False)
    assert store.get(b)["reuse_src"] is False       # bool 正确落库为 0/1

    # 列表按 created_at DESC：seq=1 时间最新 → 组内按 1..N 升序出现
    ids = [t["id"] for t in store.list()]
    assert ids.index(a) < ids.index(b)

    # 未分组任务照旧；空/不存在的 group_id 返回空列表
    c = store.create(source="quark", target="local", share_url="https://y",
                     password=None)
    assert store.get(c)["group_id"] == "" and store.get(c)["group_seq"] == 0
    assert store.list_group("") == [] and store.list_group("ghost") == []
