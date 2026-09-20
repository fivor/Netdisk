"""Linux.do Connect（OAuth2）登录接入测试。

身份模型：ld 用户 token = ld1_<uid>_<sig>（无状态签名会话），owner 强制为
ld_<uid>（封住改 owner 参数的水平越权）；登录方式与口令平级、均为普通用户。
"""
import pytest
from fastapi.testclient import TestClient

import main
from config import settings
from main import _linuxdo_token, _linuxdo_owner_from_token


@pytest.fixture()
def ld_configured(monkeypatch):
    """打开 Linux.do 开关并隔离「本机直连=管理员」的兜底（否则 TestClient 全是管理员）。"""
    monkeypatch.setattr(settings, "linuxdo_client_id", "cid-test")
    monkeypatch.setattr(settings, "linuxdo_client_secret", "sec-test")
    monkeypatch.setattr(settings, "admin_password", "admin-pwd-test")


@pytest.fixture()
def client(monkeypatch):
    """与 test_api 的 client 同款：起应用 + 编排器 submit 换 no-op 保证确定性。"""
    with TestClient(main.app) as c:
        if main.orch is not None:
            monkeypatch.setattr(main.orch, "submit", lambda *a, **k: None)
            monkeypatch.setattr(main.orch, "submit_group", lambda *a, **k: None)
        yield c


def test_options_public(client):
    """登录方式清单是公开接口；未配置 Linux.do 时报告 False。"""
    r = client.get("/api/auth/options")
    assert r.status_code == 200
    assert r.json() == {"password": True, "linuxdo": False}


def test_start_404_when_unconfigured(client):
    assert client.get("/api/auth/linuxdo/start").status_code == 404


def test_start_redirects_with_state(ld_configured, client):
    """start 一次性 state + 302 到 connect.linux.do 授权端点。"""
    r = client.get("/api/auth/linuxdo/start", follow_redirects=False)
    assert r.status_code in (301, 302, 307)
    loc = r.headers["location"]
    assert loc.startswith("https://connect.linux.do/oauth2/authorize")
    assert "client_id=cid-test" in loc and "state=" in loc
    assert "redirect_uri=" in loc


def test_token_signature_roundtrip(ld_configured):
    tok = _linuxdo_token("123")
    assert _linuxdo_owner_from_token(tok) == "ld_123"
    assert _linuxdo_owner_from_token(tok[:-1] + ("0" if tok[-1] != "0" else "1")) is None
    bad_uid = "ld1_999_" + tok.split("_")[2]
    assert _linuxdo_owner_from_token(bad_uid) is None      # 拿 A 的签名冒充 B
    assert _linuxdo_owner_from_token("ld1_abc_" + "0" * 32) is None   # uid 必须纯数字


def test_ld_token_valid_non_admin(ld_configured, client):
    """ld token 通过共享鉴权，但身份恒为普通用户（即便请求来自本机）。"""
    tok = _linuxdo_token("123")
    r = client.get("/api/status", headers={"X-Auth-Token": tok})
    assert r.status_code == 200
    assert r.json()["admin"] is False
    me = client.get("/api/auth/me", headers={"X-Auth-Token": tok}).json()
    assert me == {"admin": False, "owner": "ld_123", "kind": "linuxdo"}
    # 篡改签名 → 401
    r = client.get("/api/status", headers={"X-Auth-Token": tok[:-1] + "0"})
    assert r.status_code == 401


def test_ld_tasks_owner_injected(ld_configured, client):
    """ld 用户不带 owner 也不 400：归属由 token 派生；伪造他人 owner 被覆盖。"""
    tok = _linuxdo_token("123")
    r = client.get("/api/tasks", headers={"X-Auth-Token": tok})
    assert r.status_code == 200                       # 未带 owner → 注入 ld_123
    r = client.get("/api/tasks?owner=ld_999", headers={"X-Auth-Token": tok})
    assert r.status_code == 200                       # 伪造 owner → 同样被覆盖为 ld_123
    assert all(t["owner_id"] == "ld_123" for t in r.json()["tasks"])


def test_ld_transfer_owner_forced(ld_configured, client):
    """提交时 body.owner_id 对 ld 用户无效，任务归属强制 ld_<uid>。"""
    tok = _linuxdo_token("123")
    r = client.post("/api/transfer", headers={"X-Auth-Token": tok},
                    json={"text": "https://pan.quark.cn/s/ldowner01",
                          "target": "quark", "owner_id": "spoof"})
    assert r.status_code == 200, r.text
    tid = r.json()["task_id"]
    assert main.store.get(tid)["owner_id"] == "ld_123"


def test_callback_bad_code_friendly_redirect(ld_configured, client, monkeypatch):
    """state 合法但 code 无效：不 500，友好错误走 fragment 回前端。"""

    class _Resp:
        status_code = 400
        headers = {"content-type": "application/json"}

        def json(self):
            return {"error": "invalid_grant"}

    class _FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            return _Resp()

    monkeypatch.setattr(main.httpx, "AsyncClient", _FakeClient)
    # 先取一个合法 state（start 的 302 Location 里）
    r = client.get("/api/auth/linuxdo/start", follow_redirects=False)
    loc = r.headers["location"]
    state = loc.split("state=")[1].split("&")[0]
    r = client.get(f"/api/auth/linuxdo/callback?code=bad&state={state}",
                   follow_redirects=False)
    assert r.status_code in (301, 302, 307)
    assert r.headers["location"].startswith("/#ld_err=")


def test_callback_replays_state_rejected(ld_configured, client):
    """state 一次性：重放同一个 state 直接判过期。"""
    r = client.get("/api/auth/linuxdo/start", follow_redirects=False)
    state = r.headers["location"].split("state=")[1].split("&")[0]
    # 两次回调（无 code，先在参数校验分支被拦不算消费）——真正消费一次：
    from main import _ld_states
    with main._ld_states_lock:
        assert state in _ld_states
        _ld_states.pop(state)                  # 模拟已被消费
    r = client.get(f"/api/auth/linuxdo/callback?code=x&state={state}",
                   follow_redirects=False)
    assert r.headers["location"].startswith("/#ld_err=")
