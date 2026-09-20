"""API 层测试：FastAPI TestClient（lifespan 会创建 store/orch）。"""
import time

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture()
def client(monkeypatch):
    """启动应用；把编排器 submit 换成 no-op，保证测试确定性。"""
    with TestClient(main.app) as c:
        if main.orch is not None:
            monkeypatch.setattr(main.orch, "submit", lambda *a, **k: None)
            # 组合转存同理：真正的串行执行/源盘只存一次由 test_orchestrator 覆盖，
            # 这里只验证 API 层的建组、校验与组级取消/删除。
            monkeypatch.setattr(main.orch, "submit_group", lambda *a, **k: None)
        yield c


def test_healthz(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_index_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "网盘转存站" in r.text


def test_parse_ok(client):
    r = client.post("/api/parse", json={"text": "https://pan.quark.cn/s/abc12345 提取码：k9k9"})
    assert r.status_code == 200
    data = r.json()
    assert data["platform"] == "quark"
    assert data["password"] == "k9k9"
    assert data["platform_label"] == "夸克网盘"


def test_parse_bad_link_422(client):
    r = client.post("/api/parse", json={"text": "不是链接"})
    assert r.status_code == 422


def test_transfer_creates_task(client):
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/abc12345", "password": None,
                          "owner_id": "u1"})
    assert r.status_code == 200
    tid = r.json()["task_id"]
    # submit 被 no-op，任务停留在 running（编排器已置位）
    r2 = client.get(f"/api/tasks/{tid}?owner=u1")
    assert r2.status_code == 200
    assert r2.json()["id"] == tid


def test_transfer_baidu_requires_password(client):
    r = client.post("/api/transfer", json={"text": "https://pan.baidu.com/s/1abc123"})
    assert r.status_code == 422
    assert "提取码" in r.json()["detail"]


def test_transfer_with_password_ok(client):
    r = client.post("/api/transfer",
                    json={"text": "https://pan.baidu.com/s/1abc123", "password": "x9x9"})
    assert r.status_code == 200


def test_transfer_target_validation(client):
    """目标平台四选一：baidu/quark/ali/uc 合法，乱值拒绝。"""
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/tgcheck1", "target": "uc"})
    assert r.status_code == 200
    assert r.json()["target"] == "uc"
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/tgcheck1", "target": "nonsense"})
    assert r.status_code == 422
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/tgcheck1", "target": "ali"})
    assert r.status_code == 200
    assert r.json()["target"] == "ali"


def test_transfer_quark_to_baidu_needs_no_password(client):
    """源是夸克（无提取码）→ 目标百度：不应要求提取码（提取码按源平台判断）。"""
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/qk2bd12345", "target": "baidu"})
    assert r.status_code == 200
    assert r.json()["target"] == "baidu"


def test_tasks_list_contains_created(client):
    client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/listcheck1"})
    tasks = client.get("/api/tasks").json()["tasks"]
    assert any(t["share_url"].startswith("https://pan.quark.cn/s/listcheck1") for t in tasks)


def test_status_shape(client):
    """本机来源（TestClient 无转发头）= 管理员，可见就绪状态。"""
    r = client.get("/api/status")
    assert r.status_code == 200
    data = r.json()
    assert data["admin"] is True
    keys = {p["key"] for p in data["platforms"]}
    assert {"baidu", "quark", "ali", "uc"} <= keys
    assert "adapters" in data["ready"]
    assert set(data["targets"]) == {"baidu", "quark", "ali", "uc", "local"}
    assert data["openlist_ok"] is False  # 测试环境指向不可达端口


def test_status_hides_ready_from_user(client):
    """经隧道来的普通用户：只给平台列表与目标选项，不给就绪状态。"""
    r = client.get("/api/status", headers={"X-Forwarded-For": "1.2.3.4"})
    assert r.status_code == 200
    data = r.json()
    assert data["admin"] is False
    assert "ready" not in data
    assert "openlist_ok" not in data


def test_tasks_require_owner_for_user(client):
    """普通用户必须带 owner 标识；只能看到自己的任务。"""
    client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/ownertest1",
                                       "owner_id": "user-aaa"})
    client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/ownertest2",
                                       "owner_id": "user-bbb"})
    # 缺 owner → 400
    r = client.get("/api/tasks", headers={"X-Forwarded-For": "1.2.3.4"})
    assert r.status_code == 400
    # user-aaa 只见自己的
    r = client.get("/api/tasks?owner=user-aaa", headers={"X-Forwarded-For": "1.2.3.4"})
    tasks = r.json()["tasks"]
    assert tasks and all(t["owner_id"] == "user-aaa" for t in tasks)
    assert not any("ownertest2" in t["share_url"] for t in tasks)
    # 用户任务详情带 owner_id 字段
    assert tasks[0]["owner_id"] == "user-aaa"


def test_task_detail_and_delete_scoped(client):
    """非本人任务按不存在处理（详情与删除均隔离）。"""
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/scopedel1",
                                           "owner_id": "user-aaa"})
    tid = r.json()["task_id"]
    hdr = {"X-Forwarded-For": "1.2.3.4"}
    # user-bbb 访问 → 404
    assert client.get(f"/api/tasks/{tid}?owner=user-bbb", headers=hdr).status_code == 404
    assert client.delete(f"/api/tasks/{tid}?owner=user-bbb", headers=hdr).status_code == 404
    # 本人可删（先落终态）
    main.store.update(tid, status="failed", error="x")
    assert client.delete(f"/api/tasks/{tid}?owner=user-aaa",
                         headers=hdr).json()["ok"] is True


def test_force_delete_running_task(client):
    """卡死的进行中任务：默认 409 拒绝；确认后 force=1 可直接删除。"""
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/stuck12345",
                                           "owner_id": "uid-x"})
    tid = r.json()["task_id"]
    # 不带 force → 409
    assert client.delete(f"/api/tasks/{tid}?owner=uid-x").status_code == 409
    # 带 force → 删除成功，且标注了强制
    rr = client.delete(f"/api/tasks/{tid}?owner=uid-x&force=1")
    assert rr.status_code == 200
    assert rr.json()["forced"] is True
    assert client.get(f"/api/tasks/{tid}?owner=uid-x").status_code == 404


def test_admin_login_and_powers(client, monkeypatch):
    """管理员口令登录 → token 解锁全量任务与就绪状态；外网来源空口令拒绝。"""
    monkeypatch.setattr(main.settings, "admin_password", "adm123")
    try:
        # 口令错误 → 401（未授权；不再是「仅管理员口令我认识」的 403）
        r = client.post("/api/auth/login", json={"password": "wrong"})
        assert r.status_code == 401
        # 口令正确 → token + role=admin
        r = client.post("/api/auth/login", json={"password": "adm123"})
        assert r.status_code == 200
        body = r.json()
        assert body["role"] == "admin" and body["admin"] is True
        token = body["token"]
        hdr = {"X-Admin-Token": token}
        # token 复用：外网来源也可用 token
        ext = dict(hdr, **{"X-Forwarded-For": "1.2.3.4"})
        s = client.get("/api/status", headers=ext).json()
        assert s["admin"] is True and "ready" in s
        # 管理员看全部任务（不需要 owner 参数）
        tasks = client.get("/api/tasks", headers=ext).json()["tasks"]
        assert isinstance(tasks, list)
        # 错误 token 不放行
        bad = dict(hdr, **{"X-Admin-Token": "nope", "X-Forwarded-For": "1.2.3.4"})
        assert "ready" not in client.get("/api/status", headers=bad).json()
    finally:
        monkeypatch.setattr(main.settings, "admin_password", "")


def test_admin_login_requires_password_for_remote(client, monkeypatch):
    """未配置管理员口令时，经隧道来的请求不能成为管理员。"""
    monkeypatch.setattr(main.settings, "admin_password", "")
    r = client.post("/api/auth/login", json={"password": "anything"},
                    headers={"X-Forwarded-For": "1.2.3.4"})
    assert r.status_code == 403
    assert "本机" in r.json()["detail"]


def test_unified_login_resolves_role(client, monkeypatch):
    """统一登录：同一个输入框按口令判定身份。

    访问口令 → 普通界面（token 过共享鉴权但非管理员）；
    管理员密码 → 管理员界面（同一个 token 同时满足共享鉴权与管理员鉴权，
    于是管理员不必再额外知道访问口令）。
    """
    monkeypatch.setattr(main.settings, "app_password", "userpw")
    monkeypatch.setattr(main.settings, "admin_password", "adminpw")
    try:
        # ① 访问口令 → 普通用户
        r = client.post("/api/auth/login", json={"password": "userpw"})
        assert r.status_code == 200
        assert r.json()["role"] == "user" and r.json()["admin"] is False
        utok = r.json()["token"]
        s = client.get("/api/status", headers={"X-Auth-Token": utok}).json()
        assert s["admin"] is False and "ready" not in s
        assert "queue" in s                      # 并发概况对普通用户也可见

        # ② 管理员密码 → 管理员；同一 token 两用
        r = client.post("/api/auth/login", json={"password": "adminpw"})
        assert r.status_code == 200
        assert r.json()["role"] == "admin" and r.json()["admin"] is True
        atok = r.json()["token"]
        s = client.get("/api/status", headers={"X-Auth-Token": atok}).json()
        assert s["admin"] is True and "ready" in s
        # 换成 X-Admin-Token 单独携带也必须通过（同一个 token 不该因「放错头」被拒）
        r = client.get("/api/status", headers={"X-Admin-Token": atok})
        assert r.status_code == 200 and r.json()["admin"] is True

        # ③ 错口令 401；不带 token 401
        assert client.post("/api/auth/login", json={"password": "nope"}).status_code == 401
        assert client.get("/api/status").status_code == 401
    finally:
        monkeypatch.setattr(main.settings, "app_password", "")
        monkeypatch.setattr(main.settings, "admin_password", "")


def test_legacy_ownerless_tasks_admin_only(client):
    """无主旧任务（owner_id 为空）仅管理员可见，普通用户按 id 探测一律 404。"""
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/legacy001"})
    tid = r.json()["task_id"]
    ext = {"X-Forwarded-For": "1.2.3.4"}
    assert client.get(f"/api/tasks/{tid}", headers=ext).status_code == 404
    assert client.delete(f"/api/tasks/{tid}", headers=ext).status_code == 404
    assert client.get(f"/api/tasks/{tid}").status_code == 200  # 管理员可见


def test_dl_link_lifecycle_tied_to_task(client):
    """/dl 下载链接与任务共存：任务删除即失效；仅本机目标的成功任务可下载。

    链接凭证为 dl_token（与 task_id 解耦）——task_id 不再能直接访问下载。
    """
    from pathlib import Path
    from config import settings
    from main import store

    work = Path(settings.local_dir) / "转存"
    work.mkdir(parents=True, exist_ok=True)
    (work / "e2e下载.txt").write_text("本机高速内容", encoding="utf-8")

    # 造一个本机目标的成功任务
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/dllink0001", "target": "local"})
    tid = r.json()["task_id"]
    tk = store.get(tid)["dl_token"]
    assert tk and tk != tid  # 凭证与 task_id 不同
    store.update(tid, status="success", result_url=f"/dl/{tk}/e2e下载.txt",
                 message="本机高速下载链接已生成", progress=100,
                 result_files=["e2e下载.txt"])

    # 单文件任务：/dl/<token> 短链直接下载（不再回清单页）
    r = client.get(f"/dl/{tk}")
    assert r.status_code == 200
    assert "本机高速内容" in r.text
    # 真实文件名在 Content-Disposition 的 filename* 里按 RFC 5987 URL 编码
    assert "e2e%E4%B8%8B%E8%BD%BD.txt" in r.headers.get("content-disposition", "")
    # 带文件名的兼容路由（旧链接）仍可用
    r = client.get(f"/dl/{tk}/e2e下载.txt")
    assert r.status_code == 200
    assert "本机高速内容" in r.text
    # 不存在的文件
    assert client.get(f"/dl/{tk}/ghost.txt").status_code == 404
    # 解耦：task_id 不能直接当下载凭证
    assert client.get(f"/dl/{tid}/e2e下载.txt").status_code == 404
    # 任务删除 → 链接失效
    assert client.delete(f"/api/tasks/{tid}").status_code == 200
    assert client.get(f"/dl/{tk}/e2e下载.txt").status_code == 404
    assert client.get(f"/dl/{tk}").status_code == 404


def test_dl_rejects_non_local_or_unfinished(client):
    """非本机目标 / 未成功任务：/dl 一律 404。"""
    from main import store
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/dlguard001"})
    tid = r.json()["task_id"]      # target=quark（非 local）
    store.update(tid, status="success", result_url=f"/dl/{tid}/x.txt")
    tk = store.get(tid)["dl_token"]
    assert client.get(f"/dl/{tk}/x.txt").status_code == 404

    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/dlguard002",
                                           "target": "local"})
    tid2 = r.json()["task_id"]     # 本机目标但仍是 running
    tk2 = store.get(tid2)["dl_token"]
    assert client.get(f"/dl/{tk2}/x.txt").status_code == 404


def test_task_404(client):
    assert client.get("/api/tasks/doesnotexist").status_code == 404


def test_task_delete(client):
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/deletetest1"})
    tid = r.json()["task_id"]
    # submit 被 no-op，任务停在 pending；先落终态再删
    main.store.update(tid, status="failed", error="x")
    assert client.delete(f"/api/tasks/{tid}").json()["ok"] is True
    assert client.get(f"/api/tasks/{tid}").status_code == 404
    assert client.delete(f"/api/tasks/{tid}").status_code == 404


def test_delete_running_returns_409(client):
    """进行中的任务不允许删除（避免与 worker 写回竞态）。"""
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/runningdel1"})
    tid = r.json()["task_id"]
    # submit 被 no-op，任务停留在 pending（非终态）→ 409
    assert client.delete(f"/api/tasks/{tid}").status_code == 409
    # 标记为终态后可删
    main.store.update(tid, status="failed", error="x")
    assert client.delete(f"/api/tasks/{tid}").json()["ok"] is True


def test_auth_required_when_configured(client, monkeypatch):
    """APP_PASSWORD 非空时 /api/* 需要 X-Auth-Token；/healthz 与静态页豁免。"""
    monkeypatch.setattr(main.settings, "app_password", "secret123")
    try:
        assert client.get("/api/tasks").status_code == 401
        assert client.get("/api/tasks", headers={"X-Auth-Token": "wrong"}).status_code == 401
        assert client.get("/healthz").status_code == 200
        assert client.get("/").status_code == 200
        ok = client.get("/api/tasks", headers={"X-Auth-Token": "secret123"})
        assert ok.status_code == 200
        wrong_pw = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/auth1"})
        assert wrong_pw.status_code == 401
    finally:
        monkeypatch.setattr(main.settings, "app_password", "")


def test_dl_isolated_per_task(client):
    """P1 回归：所有 local 任务共享 transfer_dir，/dl 必须按 result_files 隔离，
    禁止用 A 任务链接读取 B 任务文件（跨任务串档）。"""
    from pathlib import Path
    from urllib.parse import quote as urlquote

    from config import settings
    from db import TaskStore

    work = Path(settings.local_dir) / settings.transfer_dir
    work.mkdir(parents=True, exist_ok=True)
    (work / "a_movie.mp4").write_bytes(b"AAAA")
    (work / "b_movie.mp4").write_bytes(b"BBBB")
    store = TaskStore(settings.db_path)
    t1 = store.create(source="quark", target="local", share_url="x", password=None, owner_id="u1")
    store.update(t1, status="success", result_files=["a_movie.mp4"])
    t2 = store.create(source="quark", target="local", share_url="y", password=None, owner_id="u2")
    store.update(t2, status="success", result_files=["b_movie.mp4"])
    k1, k2 = store.get(t1)["dl_token"], store.get(t2)["dl_token"]
    try:
        # t1 只能下 a，跨任务读 b 必须 404
        assert client.get(f"/dl/{k1}/a_movie.mp4").content == b"AAAA"
        assert client.get(f"/dl/{k1}/b_movie.mp4").status_code == 404
        # t2 只能下 b
        assert client.get(f"/dl/{k2}/b_movie.mp4").content == b"BBBB"
        assert client.get(f"/dl/{k2}/a_movie.mp4").status_code == 404
        # t1 单文件：/dl/<token> 短链直接下载（隔离校验核心是内容只能是自己任务里的）
        r1 = client.get(f"/dl/{k1}")
        assert r1.content == b"AAAA"
        assert "a_movie.mp4" in r1.headers.get("content-disposition", "")
        assert "b_movie.mp4" not in r1.headers.get("content-disposition", "")
        # 任务删除后链接失效
        store.delete(t1)
        assert client.get(f"/dl/{k1}/a_movie.mp4").status_code == 404
    finally:
        for f in ("a_movie.mp4", "b_movie.mp4"):
            (work / f).unlink(missing_ok=True)
        store.delete(t2)


def test_dl_chinese_filename_disposition(client):
    """P2 回归：中文文件名下载必须带 filename*=UTF-8''，否则 Firefox/Safari 乱码。"""
    from pathlib import Path
    from urllib.parse import quote as urlquote

    from config import settings
    from db import TaskStore

    work = Path(settings.local_dir) / settings.transfer_dir
    work.mkdir(parents=True, exist_ok=True)
    name = "电影.1080p.mkv"
    (work / name).write_bytes(b"X")
    store = TaskStore(settings.db_path)
    t = store.create(source="quark", target="local", share_url="z", password=None, owner_id="u")
    store.update(t, status="success", result_files=[name])
    tk = store.get(t)["dl_token"]
    try:
        r = client.get(f"/dl/{tk}/{urlquote(name)}")
        assert r.status_code == 200
        cd = r.headers.get("content-disposition", "")
        assert "filename*=UTF-8" in cd, cd
        # 中文名以 RFC5987 百分号编码写入（Firefox/Safari 据此正确显示）
        assert urlquote(name) in cd, cd
    finally:
        (work / name).unlink(missing_ok=True)
        store.delete(t)


# ---- 新增：fail-fast / 去重 / 批量 / 取消 / 本机文件管理 ----

def test_transfer_fail_fast_unconfigured_target(client, monkeypatch):
    """目标平台未配置凭据 → 提交即 422（不再等 worker 跑完才报「平台未配置」）。"""
    monkeypatch.setattr(main.settings, "baidu_cookie", "")
    try:
        r = client.post("/api/transfer",
                        json={"text": "https://pan.quark.cn/s/ff00000001", "target": "baidu"})
        assert r.status_code == 422
        assert "未配置凭据" in r.json()["detail"]
    finally:
        monkeypatch.setattr(main.settings, "baidu_cookie", "BDUSS=test-bduss")


def test_transfer_fail_fast_cross_platform_needs_openlist(client, monkeypatch):
    """跨平台但未配 OPENLIST_TOKEN → 提交即 422。"""
    monkeypatch.setattr(main.settings, "openlist_token", "")
    try:
        r = client.post("/api/transfer",
                        json={"text": "https://pan.quark.cn/s/ff00000002", "target": "baidu"})
        assert r.status_code == 422
        assert "OPENLIST_TOKEN" in r.json()["detail"]
    finally:
        monkeypatch.setattr(main.settings, "openlist_token", "test-openlist-token")


def test_transfer_dedup_recent(client):
    """同链接短时间重复提交 → 复用旧任务（deduped=True，task_id 相同）。"""
    body = {"text": "https://pan.quark.cn/s/dedup000001", "target": "local"}
    r1 = client.post("/api/transfer", json=body)
    assert r1.status_code == 200 and r1.json()["deduped"] is False
    r2 = client.post("/api/transfer", json=body)
    assert r2.status_code == 200
    assert r2.json()["deduped"] is True
    assert r2.json()["task_id"] == r1.json()["task_id"]


def test_transfer_batch(client):
    """批量提交：按行拆分，逐条建任务；坏链接单独失败不影响其余。"""
    text = ("https://pan.quark.cn/s/batch000001\n"
            "https://pan.quark.cn/s/batch000002\n"
            "这不是链接")
    r = client.post("/api/transfer/batch", json={"text": text, "target": "local"})
    assert r.status_code == 200
    d = r.json()
    assert d["total"] == 3
    assert d["ok"] == 2 and d["failed"] == 1
    oks = [x for x in d["results"] if x["ok"]]
    assert all(x["task_id"] for x in oks)
    bad = [x for x in d["results"] if not x["ok"]][0]
    assert "error" in bad


def test_transfer_batch_empty_422(client):
    r = client.post("/api/transfer/batch", json={"text": "   \n  \n"})
    assert r.status_code == 422


def test_split_link_blocks_groups_password_line():
    """跨行提取码应并入上一链接块；多条链接各自成块。"""
    text = ("链接：https://pan.baidu.com/s/aaa\n"
            "提取码：w226\n"
            "https://pan.quark.cn/s/bbb")
    blocks = main._split_link_blocks(text)
    assert len(blocks) == 2
    assert "w226" in blocks[0] and "pan.baidu.com/s/aaa" in blocks[0]
    assert "pan.quark.cn/s/bbb" in blocks[1]


def test_batch_keeps_cross_line_password(client, monkeypatch):
    """回归：百度链接与「提取码」分两行粘贴 → 并作一条并正确取到码，不再 422。

    复现 #issue：粘贴「链接：…\n提取码：w226」点批量 → 之前拆成两行各自解析，
    链接行因本行无码被 422「百度分享需要提取码」，提取码行因无链接报错，故失败 2 条。
    """
    monkeypatch.setattr(main, "_check_target_ready", lambda *a, **k: None)
    text = ("链接：https://pan.baidu.com/s/1NNR4rNxfwO2ImD2x95DNzw\n"
            "提取码：w226")
    r = client.post("/api/transfer/batch", json={"text": text, "target": "local"})
    assert r.status_code == 200
    d = r.json()
    assert d["total"] == 1 and d["ok"] == 1, d
    tid = d["results"][0]["task_id"]
    assert client.get(f"/api/tasks/{tid}?owner=admin").json()["password"] == "w226"


def test_batch_baidu_still_requires_password(client, monkeypatch):
    """反向护栏：缺提取码的百度链接仍应 422「百度分享需要提取码」（未放宽约束）。"""
    monkeypatch.setattr(main, "_check_target_ready", lambda *a, **k: None)
    text = "https://pan.baidu.com/s/1NNR4rNxfwO2ImD2x95DNzw"
    r = client.post("/api/transfer/batch", json={"text": text, "target": "local"})
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] == 0 and d["failed"] == 1
    assert "百度分享需要提取码" in d["results"][0]["error"]



def test_task_cancel(client):
    """取消进行中任务：置 cancelled；已终态再取消 → 409。"""
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/cancel00001",
                                           "owner_id": "u-cxl"})
    tid = r.json()["task_id"]
    # submit 被 no-op → 任务停在 pending（可取消）
    rr = client.post(f"/api/tasks/{tid}/cancel?owner=u-cxl")
    assert rr.status_code == 200 and rr.json()["cancelled"] is True
    assert client.get(f"/api/tasks/{tid}?owner=u-cxl").json()["status"] == "cancelled"
    assert client.post(f"/api/tasks/{tid}/cancel?owner=u-cxl").status_code == 409


def test_cancel_from_needs_confirm(client):
    """回归：待确认（needs_confirm）任务可被取消，不会卡在 409「任务已结束」。

    复现 #issue：超限弹确认框后点「取消」，状态应翻为 cancelled 而非一直 待确认。
    """
    tid = main.store.create(source="quark", target="baidu",
                            share_url="https://pan.quark.cn/s/capcheck1", password=None)
    main.store.update(tid, status="needs_confirm", progress=30, message="部分文件超限")
    rr = client.post(f"/api/tasks/{tid}/cancel?owner=admin")
    assert rr.status_code == 200 and rr.json()["cancelled"] is True
    assert client.get(f"/api/tasks/{tid}?owner=admin").json()["status"] == "cancelled"


def test_confirm_continue_endpoint(client, monkeypatch):
    """confirm-continue 仅对 needs_confirm 任务生效：其余状态 409；正确状态 200 并翻转为 running。"""
    r = client.post("/api/transfer", json={"text": "https://pan.quark.cn/s/ccendp0001",
                                           "owner_id": "ucc"})
    tid = r.json()["task_id"]
    # 非 needs_confirm（running/pending）→ 409
    assert client.post(f"/api/tasks/{tid}/confirm-continue?owner=ucc").status_code == 409
    # 置为 needs_confirm（含超限清单 + 源盘引用，供 resume 复用）
    main.store.update(tid, status="needs_confirm",
                      over_files=[{"name": "big.iso", "size": 9e9}],
                      src_ref={"names": ["big.iso"], "mount_dir": "/x"})
    calls = []
    monkeypatch.setattr(main.orch._pool, "submit", lambda *a, **k: calls.append(a))
    rr = client.post(f"/api/tasks/{tid}/confirm-continue?owner=ucc")
    assert rr.status_code == 200 and rr.json()["resumed"] is True
    t = main.store.get(tid)
    assert t["status"] == "running"
    assert t["confirmed_skip"] is True
    assert calls, "应当重新提交 worker"


def test_local_files_admin_list_and_delete(client):
    """本机文件管理：管理员可见清单，可删除缓存文件。"""
    from pathlib import Path
    from config import settings
    work = Path(settings.local_dir) / settings.transfer_dir
    work.mkdir(parents=True, exist_ok=True)
    f = work / "cache_probe.bin"
    f.write_bytes(b"12345")
    try:
        r = client.get("/api/local/files")
        assert r.status_code == 200
        names = [x["name"] for x in r.json()["files"]]
        assert "cache_probe.bin" in names
        assert client.delete("/api/local/files/cache_probe.bin").status_code == 200
        assert not f.exists()
        assert client.delete("/api/local/files/ghost.bin").status_code == 404
    finally:
        f.unlink(missing_ok=True)


def test_local_files_requires_admin(client):
    """本机文件管理仅管理员：经隧道来的普通用户 403。"""
    ext = {"X-Forwarded-For": "1.2.3.4"}
    assert client.get("/api/local/files", headers=ext).status_code == 403
    assert client.delete("/api/local/files/x.bin", headers=ext).status_code == 403


def test_transfer_ali_target_requires_admin(client):
    """阿里作为目标仅管理员可选（服务端硬约束，不只靠前端置灰）。"""
    ext = {"X-Forwarded-For": "1.2.3.4"}   # 经隧道 → 普通用户
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/alilock0001", "target": "ali"},
                    headers=ext)
    assert r.status_code == 403
    assert "阿里" in r.json()["detail"]
    # 批量提交里同样被拒（该行失败，整体仍返回 200）
    r = client.post("/api/transfer/batch",
                    json={"text": "https://pan.quark.cn/s/alilock0002", "target": "ali"},
                    headers=ext)
    assert r.status_code == 200
    assert r.json()["results"][0]["ok"] is False
    # 管理员（本机 TestClient）不受限
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/alilock0003", "target": "ali"})
    assert r.status_code == 200


def test_reap_safety_valve_helpers():
    """启动回收的安全阀：两轮快照有差异（progress 推进/任务增删）→ 判定仍在传输。

    回归 2026-09-18 事故：重启时 OpenList 仍在正常上传，旧实现一律取消
    → 连本机 temp 一起废掉（实测丢 21.5GB）。
    """
    from main import _is_progressing, _progress_snapshot

    a = [{"id": "t1", "progress": 10}, {"id": "t2", "progress": 0}]
    b = [{"id": "t1", "progress": 40}, {"id": "t2", "progress": 0}]
    assert _progress_snapshot(a) == {"t1": 10.0, "t2": 0.0}
    assert _is_progressing(_progress_snapshot(a), _progress_snapshot(b)) is True
    # 完全不动 → 僵尸（可回收）
    assert _is_progressing(_progress_snapshot(b), _progress_snapshot(b)) is False
    # 任务被替换（新 id）也算推进，不能当僵尸杀
    d = [{"id": "t3", "progress": 0}]
    assert _is_progressing(_progress_snapshot(b), _progress_snapshot(d)) is True
    # 空输入不炸
    assert _progress_snapshot(None) == {} and not _is_progressing({}, {})


# ---------- 组合转存（一条链接 → 多个网盘，仅管理员） ----------

def test_group_transfer_creates_children_in_order(client):
    """1 条链接 → N 个子任务：同 group_id、seq 升序，且在列表中相邻按序出现。"""
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000001",
        "targets": ["local", "uc", "baidu"], "owner_id": "u-grp"})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["grouped"] is True and d["group_id"]
    assert d["targets"] == ["local", "uc", "baidu"]
    assert len(d["task_ids"]) == 3

    lst = client.get("/api/tasks?owner=u-grp").json()["tasks"]
    idx = [i for i, t in enumerate(lst) if t["group_id"] == d["group_id"]]
    assert idx == list(range(idx[0], idx[0] + 3)), "同组子任务必须在列表中相邻"
    kids = [lst[i] for i in idx]
    assert [k["group_seq"] for k in kids] == [1, 2, 3]
    assert [k["target"] for k in kids] == ["local", "uc", "baidu"]


def test_group_transfer_requires_admin(client, monkeypatch):
    """组合转存仅管理员：非管理员传 targets → 403（后端硬校验，不靠前端置灰）。"""
    monkeypatch.setattr(main, "is_admin", lambda *a, **k: False)
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000002",
        "targets": ["local", "uc"], "owner_id": "u-x"})
    assert r.status_code == 403
    assert "管理员" in r.json()["detail"]


def test_group_transfer_rejects_over_limit(client):
    """目标数 > 5 → 422（GROUP_MAX=5）。

    注意：平台总共只有 5 个，凑不出 6 个**合法**目标，故第 6 个用非法值——
    长度校验先于逐目标校验，仍应命中「上限」而非「不支持」。
    """
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000003",
        "targets": ["baidu", "quark", "uc", "local", "ali", "nope"], "owner_id": "u-x"})
    assert r.status_code == 422
    assert "最多" in r.json()["detail"]


def test_group_transfer_rejects_multi_links(client):
    """多目标时原链接只允许 1 条（不接受「多链接 × 多目标」）。"""
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/g000000001\nhttps://pan.quark.cn/s/g000000002",
        "targets": ["local", "uc"], "owner_id": "u-x"})
    assert r.status_code == 422
    assert "1 条链接" in r.json()["detail"]


def test_group_transfer_merges_duplicate_targets(client):
    """重复目标自动合并（不报错骚扰用户）。"""
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000004",
        "targets": ["local", "local"], "owner_id": "u-x"})
    assert r.status_code == 200
    assert r.json()["targets"] == ["local"]


def test_group_transfer_validates_each_target(client):
    """逐目标 fail-fast：不支持的平台 / 百度源缺提取码 都要当场 422。"""
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000005",
        "targets": ["nope", "local"], "owner_id": "u-x"})
    assert r.status_code == 422 and "不支持" in r.json()["detail"]

    r = client.post("/api/transfer", json={
        "text": "https://pan.baidu.com/s/1NNR4rNxfwO2ImD2x95DNzw",
        "targets": ["local", "uc"], "owner_id": "u-x"})
    assert r.status_code == 422 and "提取码" in r.json()["detail"]


def test_group_cancel_cancels_whole_group(client):
    """取消组内任一子任务 = 取消整组（含尚未执行的兄弟）。"""
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000006",
        "targets": ["local", "uc", "baidu"], "owner_id": "u-gc"})
    ids = r.json()["task_ids"]
    rr = client.post(f"/api/tasks/{ids[1]}/cancel?owner=u-gc")
    assert rr.status_code == 200 and rr.json().get("group") is True
    for tid in ids:
        t = client.get(f"/api/tasks/{tid}?owner=u-gc").json()
        assert t["status"] == "cancelled"


def test_group_delete_removes_all_children(client):
    """删除按「一条记录」整体生效，不留残缺兄弟。"""
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000007",
        "targets": ["local", "uc"], "owner_id": "u-gd"})
    ids = r.json()["task_ids"]
    # 任务仍是 pending → 需要 force
    rr = client.delete(f"/api/tasks/{ids[0]}?owner=u-gd&force=true")
    assert rr.status_code == 200
    assert rr.json().get("group") is True and rr.json()["deleted"] == 2
    for tid in ids:
        assert client.get(f"/api/tasks/{tid}?owner=u-gd").status_code == 404


def test_group_transfer_dedups_same_targets(client):
    """同一分享 + 同一目标集合在窗口内重复提交 → 复用已有组，不重复建任务。"""
    body = {"text": "https://pan.quark.cn/s/grp0000009",
            "targets": ["local", "uc"], "owner_id": "u-dd"}
    d1 = client.post("/api/transfer", json=body).json()
    d2 = client.post("/api/transfer", json=body).json()
    assert d2["deduped"] is True
    assert d2["group_id"] == d1["group_id"]
    assert d2["task_ids"] == d1["task_ids"]
    lst = client.get("/api/tasks?owner=u-dd").json()["tasks"]
    assert len([t for t in lst if t["group_id"] == d1["group_id"]]) == 2


def test_task_api_hides_dl_token(client):
    """任务接口不得下发 dl_token（本机高速下载凭证，等同下载权限）。"""
    r = client.post("/api/transfer", json={
        "text": "https://pan.quark.cn/s/grp0000011",
        "targets": ["local", "uc"], "owner_id": "u-hd"})
    tid = r.json()["task_ids"][0]
    assert "dl_token" not in client.get(f"/api/tasks/{tid}?owner=u-hd").json()
    listed = client.get("/api/tasks?owner=u-hd").json()["tasks"]
    assert all("dl_token" not in t for t in listed)
    # 服务端仍能按凭证查到任务（/dl 端点不受影响）
    assert main.store is not None


def test_dl_purge_urls_and_cache_headers():
    """CF 边缘缓存开关：purge URL 覆盖短链+逐文件；未配置三件套不下发 Cache-Control。"""
    from main import _dl_cache_headers, _dl_purge_urls
    task = {"target": "local", "dl_token": "tok123",
            "result_files": ["a.apk", "b c.txt"]}
    assert _dl_purge_urls(task) == ["/dl/tok123", "/dl/tok123/a.apk", "/dl/tok123/b%20c.txt"]
    # 非 local / 缺 token → 不产生 purge URL
    assert _dl_purge_urls({"target": "quark", "dl_token": "x", "result_files": ["a"]}) == []
    assert _dl_purge_urls({"target": "local", "dl_token": "", "result_files": ["a"]}) == []
    # 测试环境未配置 CF 三件套 → 不下发 Cache-Control（保持现状）
    assert _dl_cache_headers() == {}


def test_dl_nested_folder_files(client):
    """源分享含文件夹：result_files 为含子目录的相对路径，/dl 按相对路径下载。"""
    from pathlib import Path
    from config import settings
    from main import store
    work = Path(settings.local_dir) / settings.transfer_dir
    sub = work / "风暴MOD"
    sub.mkdir(parents=True, exist_ok=True)
    (sub / "game.apk").write_bytes(b"GAME")
    r = client.post("/api/transfer",
                    json={"text": "https://pan.quark.cn/s/dlfolder01", "target": "local"})
    tid = r.json()["task_id"]
    tk = store.get(tid)["dl_token"]
    store.update(tid, status="success", progress=100, message="ok",
                 result_files=["风暴MOD/game.apk"])
    r = client.get(f"/dl/{tk}")   # 单文件 → 短链直接下载
    assert r.status_code == 200
    assert r.content == b"GAME"
    assert "game.apk" in r.headers.get("content-disposition", "")
    r2 = client.get(f"/dl/{tk}/风暴MOD/game.apk")   # 含子目录的相对路径路由
    assert r2.status_code == 200
    assert r2.content == b"GAME"
    store.delete(tid)


def test_icon_routes(client):
    """图标静态路由：Linux.do Connect 等外部平台的应用图标 URL 依赖它们。"""
    r = client.get("/icon.png")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("image/png")
    assert r.content[:4] == b"\x89PNG"
    r = client.get("/favicon.svg")
    assert r.status_code == 200
    assert "image/svg+xml" in r.headers["content-type"]
