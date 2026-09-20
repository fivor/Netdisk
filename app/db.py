"""SQLite 任务库：线程安全的最小任务存储（WAL 模式）。

表结构对应方案 7.2「SQLite 任务表：状态、进度、失败原因」。
低并发场景（几十人、线程池 2~4 worker），单文件 + WAL 足够，
不引入 Celery/Redis（方案定版决策）。
"""
from __future__ import annotations

import secrets
import sqlite3
import json
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

_SCHEMA_TABLES = """
CREATE TABLE IF NOT EXISTS tasks (
    id            TEXT PRIMARY KEY,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    source        TEXT NOT NULL,
    target        TEXT NOT NULL,
    share_url     TEXT NOT NULL,
    password      TEXT,
    status        TEXT NOT NULL DEFAULT 'pending',
    progress      INTEGER NOT NULL DEFAULT 0,
    message       TEXT NOT NULL DEFAULT '',
    result_url    TEXT,
    result_pwd    TEXT,
    error         TEXT,
    owner_id      TEXT NOT NULL DEFAULT '',
    result_files  TEXT,
    dl_token      TEXT,
    progress_detail TEXT,
    over_files    TEXT,
    src_ref       TEXT,
    confirmed_skip INTEGER NOT NULL DEFAULT 0,
    group_id      TEXT NOT NULL DEFAULT '',
    group_seq     INTEGER NOT NULL DEFAULT 0,
    reuse_src     INTEGER NOT NULL DEFAULT 0,
    cap_gb        REAL
);
"""

# 索引必须在迁移（补 owner_id 列）之后创建，否则旧库会因缺列报错
_SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_owner ON tasks (owner_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks (group_id, group_seq);
"""

# 合法状态机：pending -> running -> success / failed / cancelled
# needs_confirm：跨平台中转时部分文件超过目标盘单文件上限，等待用户「跳过超限继续」或「取消」
STATUSES = ("pending", "running", "success", "failed", "cancelled", "needs_confirm")


def _migrate(conn: sqlite3.Connection) -> None:
    """旧库迁移：缺 owner_id / result_files / dl_token 列时补齐。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    if "owner_id" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN owner_id TEXT NOT NULL DEFAULT ''")
    if "result_files" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN result_files TEXT")
    if "dl_token" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN dl_token TEXT")
    if "progress_detail" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN progress_detail TEXT")
    if "over_files" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN over_files TEXT")
    if "src_ref" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN src_ref TEXT")
    if "confirmed_skip" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN confirmed_skip INTEGER NOT NULL DEFAULT 0")
    if "group_id" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN group_id TEXT NOT NULL DEFAULT ''")
    if "group_seq" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN group_seq INTEGER NOT NULL DEFAULT 0")
    if "reuse_src" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN reuse_src INTEGER NOT NULL DEFAULT 0")
    if "cap_gb" not in cols:
        # needs_confirm 时的目标盘单文件上限（结构化字段，前端不再从 message 正则反解）
        conn.execute("ALTER TABLE tasks ADD COLUMN cap_gb REAL")


def _new_dl_token() -> str:
    """下载凭证：32 位随机十六进制。

    与 task_id 解耦——下载链接只携带 dl_token，分享出去不会泄露任务 ID
    （任务 ID 可被用于 /api/tasks/{id} 探测，凭证外泄面因此收窄）。
    """
    return secrets.token_hex(16)


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._local = threading.local()
        with self._conn() as conn:
            conn.executescript(_SCHEMA_TABLES)
            _migrate(conn)
            conn.executescript(_SCHEMA_INDEXES)
            # 回填缺失的 dl_token（旧库迁移行），保证每个任务都有独立下载凭证
            rows = conn.execute(
                "SELECT id FROM tasks WHERE dl_token IS NULL OR dl_token=''").fetchall()
            for r in rows:
                conn.execute("UPDATE tasks SET dl_token=? WHERE id=?",
                             (_new_dl_token(), r["id"]))

    def _conn(self) -> sqlite3.Connection:
        """每线程一个连接（sqlite3 对象禁止跨线程复用）。"""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=15)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=15000")
            self._local.conn = conn
        return conn

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._conn()
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _json_loads(raw: str | None, fallback):
        """JSON 文本 → 对象；损坏数据降级为 fallback，绝不让一条脏记录打挂整个列表接口。"""
        if not raw:
            return fallback
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return fallback

    def _row_to_task(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        # result_files 以 JSON 文本存储，读回时还原为列表
        d["result_files"] = self._json_loads(d.get("result_files"), [])
        # progress_detail 以 JSON 文本存储，读回时还原为字典（跨平台中转的下载/上传明细）
        d["progress_detail"] = self._json_loads(d.get("progress_detail"), {})
        # over_files / src_ref 以 JSON 文本存储（超限确认流程的跨阶段数据）
        d["over_files"] = self._json_loads(d.get("over_files"), [])
        d["src_ref"] = self._json_loads(d.get("src_ref"), None)
        d["confirmed_skip"] = bool(d.get("confirmed_skip"))
        # 组合转存（一条链接 → 多个目标盘）：组标识 / 组内序号（1..N）/ 复用已保存源文件
        d["group_id"] = d.get("group_id") or ""
        d["group_seq"] = int(d.get("group_seq") or 0)
        d["reuse_src"] = bool(d.get("reuse_src"))
        # 目标盘单文件上限（GB，needs_confirm 时落库；0/None = 无）
        try:
            d["cap_gb"] = float(d.get("cap_gb") or 0)
        except (TypeError, ValueError):
            d["cap_gb"] = 0.0
        return d

    # ---- 写操作 ----

    def create(self, *, source: str, target: str, share_url: str, password: str | None,
               owner_id: str = "", group_id: str = "", group_seq: int = 0,
               created_at: float | None = None) -> str:
        """建任务；组合转存额外传 group_id + group_seq（组内序号从 1 开始）。

        created_at 可显式指定：列表按 created_at DESC 排序，组合转存的子任务若各自
        取 time.time() 会把组内顺序倒过来。调用方按组统一基准时间倒推即可
        （见 main._create_group），保证同组子任务相邻且按 seq 升序出现在列表里。
        """
        task_id = uuid.uuid4().hex[:12]
        now = created_at if created_at is not None else time.time()
        with self._write() as conn:
            conn.execute(
                "INSERT INTO tasks (id, created_at, updated_at, source, target, share_url,"
                " password, owner_id, dl_token, group_id, group_seq)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, now, now, source, target, share_url, password, owner_id,
                 _new_dl_token(), group_id, group_seq),
            )
        return task_id

    def update(self, task_id: str, **fields: Any) -> None:
        allowed = {k: v for k, v in fields.items() if k in {
            "status", "progress", "message", "result_url", "result_pwd",
            "error", "result_files", "progress_detail", "over_files",
            "src_ref", "confirmed_skip", "reuse_src", "cap_gb"}}
        for status in ("status",):
            if status in allowed and allowed[status] not in STATUSES:
                raise ValueError(f"非法任务状态: {allowed[status]}")
        if not allowed:
            return
        # result_files 是列表，落库时序列化为 JSON 文本（TEXT 列）
        if isinstance(allowed.get("result_files"), list):
            allowed["result_files"] = json.dumps(allowed["result_files"], ensure_ascii=False)
        # progress_detail 是字典（下载/上传明细），落库时序列化为 JSON 文本
        if isinstance(allowed.get("progress_detail"), dict):
            allowed["progress_detail"] = json.dumps(allowed["progress_detail"], ensure_ascii=False)
        # over_files（超限清单）/ src_ref（resume 用源盘引用）为列表/字典，统一 JSON 序列化
        for k in ("over_files", "src_ref"):
            if isinstance(allowed.get(k), (list, dict)):
                allowed[k] = json.dumps(allowed[k], ensure_ascii=False)
        # 布尔字段落库为 0/1（SQLite 无原生 bool）
        for bk in ("confirmed_skip", "reuse_src"):
            if isinstance(allowed.get(bk), bool):
                allowed[bk] = 1 if allowed[bk] else 0
        allowed["updated_at"] = time.time()
        cols = ", ".join(f"{k}=?" for k in allowed)
        with self._write() as conn:
            cur = conn.execute(f"UPDATE tasks SET {cols} WHERE id=?", (*allowed.values(), task_id))
            if cur.rowcount == 0:
                raise KeyError(f"任务不存在: {task_id}")

    def delete(self, task_id: str) -> int:
        """删除任务，返回删除行数（0 = 不存在）。"""
        with self._write() as conn:
            cur = conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
            return cur.rowcount

    def reap_stuck(self) -> int:
        """启动回收：把上轮遗留的 pending/running 任务标为 failed。

        容器/进程重启后，旧 Orchestrator 的线程池不复存在，这些任务
        永远不会被 worker 认领，若不回收会永久卡在「转存中」（前端进度条
        原地转坟）。返回受影响行数。

        注：标记为 failed 而非 cancelled——cancelled 语义是「用户主动取消」，
        重启中断并非用户所为，误标会让用户困惑（「我没取消啊」）。
        cancelled 状态留给后续的「取消进行中任务」功能。
        """
        with self._write() as conn:
            cur = conn.execute(
                "UPDATE tasks SET status='failed', progress=0, "
                "message='服务重启，任务中断（请重新提交）', "
                "error='服务重启中断', updated_at=? "
                "WHERE status IN ('pending','running')",
                (time.time(),))
            return cur.rowcount

    # ---- 读操作 ----

    def get(self, task_id: str) -> dict[str, Any] | None:
        row = self._conn().execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return self._row_to_task(row) if row else None

    def get_by_dl_token(self, token: str) -> dict[str, Any] | None:
        """/dl 专用：按下载凭证查任务（分享链接只携带 dl_token，不泄露 task_id）。"""
        if not token:
            return None
        row = self._conn().execute(
            "SELECT * FROM tasks WHERE dl_token=?", (token,)).fetchone()
        return self._row_to_task(row) if row else None

    def find_recent(self, *, source: str, target: str, share_url: str,
                    within_seconds: float = 600.0,
                    owner_id: str | None = None) -> dict[str, Any] | None:
        """同链接短时间去重：返回窗口内相同 (源,目标,分享文本) 的最近一条**单条任务**。

        `group_id=''` 是刻意的：组合转存的子任务属于「一条组合记录」，不该被单目标
        提交当成普通任务复用（否则前端会把组合里的某个子任务显示成一条独立记录）。
        `owner_id` 非 None 时只复用**同一提交者**的任务——别人的同链接任务与己无关，
        把自己的提交挂到别人的 task_id 上既是信息面也是困惑源。
        """
        since = time.time() - within_seconds
        sql = ("SELECT * FROM tasks WHERE source=? AND target=? AND share_url=? "
               "AND group_id='' AND created_at>=?")
        params: list = [source, target, share_url, since]
        if owner_id is not None:
            sql += " AND owner_id=?"
            params.append(owner_id)
        sql += " ORDER BY created_at DESC LIMIT 1"
        row = self._conn().execute(sql, params).fetchone()
        return self._row_to_task(row) if row else None

    def find_recent_group(self, *, share_url: str, targets: list[str],
                          within_seconds: float = 600.0,
                          owner_id: str | None = None) -> list[dict[str, Any]]:
        """组合转存去重：找窗口内「同一分享 + 同一目标集合」的已存在组。

        命中则返回该组全部子任务（按 group_seq 升序），供 API 直接复用，避免
        双击/回车连发建出重复的组。目标集合按**去重排序后**比较（与提交侧同口径）。
        `owner_id` 非 None 时只复用同一提交者的组（与单条 dedup 同语义）。
        """
        if within_seconds <= 0:
            return []
        want = tuple(sorted({t for t in targets if t}))
        if not want:
            return []
        since = time.time() - within_seconds
        sql = "SELECT * FROM tasks WHERE group_id<>'' AND share_url=? AND created_at>=?"
        params: list = [share_url, since]
        if owner_id is not None:
            sql += " AND owner_id=?"
            params.append(owner_id)
        rows = self._conn().execute(sql, params).fetchall()
        buckets: dict[str, list] = {}
        for r in rows:
            buckets.setdefault(r["group_id"], []).append(r)
        for _, items in buckets.items():
            kids = [self._row_to_task(r)
                    for r in sorted(items, key=lambda r: r["group_seq"] or 0)]
            if tuple(sorted(k["target"] for k in kids)) != want:
                continue
            if any(k["status"] in ("pending", "running", "success") for k in kids):
                return kids
        return []

    def list(self, limit: int = 50, owner_id: str | None = None) -> list[dict[str, Any]]:
        """按时间倒序列出任务；owner_id 非 None 时只返回该用户的任务。"""
        if owner_id is None:
            rows = self._conn().execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = self._conn().execute(
                "SELECT * FROM tasks WHERE owner_id=? ORDER BY created_at DESC LIMIT ?",
                (owner_id, limit),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def list_group(self, group_id: str) -> list[dict[str, Any]]:
        """取某「组合转存」组内的全部子任务，按组内序号升序（group_seq: 1..N）。

        编排器据此按序串行执行，并在「超限暂停 → 继续」时重建剩余目标序列。
        """
        if not group_id:
            return []
        rows = self._conn().execute(
            "SELECT * FROM tasks WHERE group_id=? ORDER BY group_seq ASC",
            (group_id,)).fetchall()
        return [self._row_to_task(r) for r in rows]

    def counts_by_status(self) -> dict[str, int]:
        rows = self._conn().execute(
            "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}
