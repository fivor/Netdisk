"""转存站 FastAPI 入口。

路由：
- GET  /                单页 Web UI（static/index.html）
- POST /api/parse       链接解析预览（平台/提取码识别）
- POST /api/transfer    创建转存任务（可指定目标平台）
- GET  /api/tasks       任务列表（用户只见自己的；管理员见全部）
- GET  /api/tasks/{id}  任务详情（同样按身份过滤）
- DELETE /api/tasks/{id} 删除任务（用户删自己的；管理员任意）
- GET  /api/status      平台列表；网盘就绪状态仅管理员可见
- POST /api/auth/login  统一登录（口令 → 判定身份/角色）
- GET  /healthz         存活探针

身份模型（小圈子自用，轻量）：
- 用户 = 浏览器匿名 UUID（owner_id，前端生成存 localStorage），任务归属按此隔离；
- 统一登录：**同一个输入框**输入 APP_PASSWORD 进普通界面，
  输入 ADMIN_PASSWORD 进管理员界面（服务端 /api/auth/login 判定角色）；
- 管理员 token = ADMIN_PASSWORD 的派生值（sha256），X-Auth-Token / X-Admin-Token
  任一携带即同时满足共享鉴权与管理员鉴权（管理员因此不必另知访问口令）；
- 两个口令都未配置时：共享鉴权直接放行，且仅本机直连（无代理/隧道转发头）视为管理员。
"""
from __future__ import annotations

import asyncio
import hashlib
import httpx
import logging
import os
import re
import secrets
import threading
import time
import uuid
from contextlib import asynccontextmanager
from html import escape
from pathlib import Path
from urllib.parse import quote as urlquote
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

import cache_mgr
from config import settings
from db import TaskStore
from http_client import new_client
from openlist import OpenListClient, parse_task_name
from orchestrator import Orchestrator
from parser import ParseError, PLATFORM_LABELS, _PASSWORD, parse_share_text, validate_password

store: TaskStore | None = None
orch: Orchestrator | None = None
_cache_stop = threading.Event()

# 可选目标平台（有适配器的四家 + 本机高速；解析支持的平台均可作为转存目标）
TARGETS = ("baidu", "quark", "ali", "uc", "local")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store, orch
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    store = TaskStore(settings.db_path)
    # 启动回收：上一轮遗留的 pending/running 任务（旧 worker 线程池已随
    # 重启消失，永不会被认领）标记为 failed，避免永久卡在「转存中」。
    reaped = store.reap_stuck()
    if reaped:
        print(f"[boot] 已回收 {reaped} 个重启中断的任务（pending/running → failed）")
    orch = Orchestrator(store, settings, max_workers=settings.task_workers)
    Path(settings.local_dir).mkdir(parents=True, exist_ok=True)
    if not settings.app_password:
        # 只警告、不阻断：口令由部署方在 .env 设定，擅自生成会把人锁在门外。
        if settings.admin_password:
            print("[boot] ⚠️ 未设置 APP_PASSWORD（已设 ADMIN_PASSWORD）：/api/* 需要"
                  "管理员 token 才能访问，普通口令用户无法使用本站。"
                  "如需普通用户访问请在 .env 配置 APP_PASSWORD。")
        else:
            print("[boot] ⚠️ 未设置 APP_PASSWORD：/api/* 对所有能访问到本服务的人开放"
                  "（仅管理员端点受 ADMIN_PASSWORD 保护）。公网暴露前请务必在 .env 配置。")
    # 孤儿回收放到后台线程：它需要采样 6 秒判断「是否仍在推进」，不能拖慢启动
    threading.Thread(target=_reap_openlist_orphans, name="openlist-reap",
                     daemon=True).start()
    cache_mgr.start_cache_thread(_cache_stop)
    # 挂载一致性校验含多次 HTTP（timeout 5/10）与可能的一次 sleep，
    # 放后台线程，别拖慢启动（与孤儿回收同策略）。
    threading.Thread(target=_validate_local_mount, name="local-mount-check",
                     daemon=True).start()
    yield
    _cache_stop.set()
    orch.shutdown()


def _progress_snapshot(tasks: list[dict]) -> dict[str, float]:
    """任务 id → progress% 快照（用于判断中转是否仍在推进）。"""
    out: dict[str, float] = {}
    for t in tasks or []:
        tid = str(t.get("id") or "")
        if not tid:
            continue
        try:
            out[tid] = float(t.get("progress") or 0)
        except (TypeError, ValueError):
            out[tid] = 0.0
    return out


def _is_progressing(before: dict[str, float], after: dict[str, float]) -> bool:
    """两次快照有差异（进度变化 / 任务增删）→ 判定「仍在推进」。"""
    return before != after


def _reap_openlist_orphans() -> None:
    """启动回收：取消我们中转目录下「无人认领**且完全停滞**」的 OpenList 复制任务。

    ⚠️ 为什么不能一律取消（2026-09-18 事故）：重启后 OpenList 的复制任务**很可能
    仍在正常上传**——此时取消会把用户等了很久的传输连本机 temp 一起废掉
    （实测一次丢掉 21.5GB 已下载数据）。故改为**连续采样两轮**（每轮间隔
    `RELAY_REAP_STALL_SEC`，默认 45 秒）：
    - 只要 progress 有任何推进 → 判定「仍在传输」，**一律不取消**，仅提示；
    - **连续两轮**快照完全一致 → 才视为僵尸任务并取消。
    两次快照都只统计**本任务**（mine）的 id，口径必须一致 —— 否则混入他人的未完成
    任务会让比较恒为「有变动」，回收就永远不触发。
    需要强制清理时设 `RELAY_REAP_FORCE=1`（知道风险再用）。best-effort、不阻断启动。
    """
    if not (settings.relay_reap_on_boot and settings.openlist_token):
        return
    try:
        ol = OpenListClient(settings.openlist_base, settings.openlist_token)
        dsts = {m.rstrip("/") + "/" + settings.transfer_dir
                for m in settings.openlist_mounts.values() if m}
        with httpx.Client(trust_env=False, timeout=5) as c:
            mine = []
            for t in ol.copy_tasks(c, "undone"):
                info = parse_task_name(str(t.get("name") or ""))
                if not info:
                    continue
                dp = info["dp"].rstrip("/")
                if any(dp == d or dp.startswith(d + "/") for d in dsts):
                    mine.append(t)
            if not mine:
                return
            # ⚠️ 快照必须**同口径**：两轮都只统计 mine 的 id。旧实现 before 取 mine、
            # after 取全部 undone，只要存在任何非本任务的未完成复制任务，两次 key
            # 集合就不同 → 恒判「在推进」→ 回收永不触发。
            mine_ids = {str(t.get("id") or "") for t in mine}
            snap = _progress_snapshot(mine)
            stall_sec = max(10.0, float(getattr(settings, "relay_reap_stall_sec", 45) or 45))
            static_rounds = 0
            for _ in range(2):        # 连续两轮静止才认定僵尸（放宽误杀门槛）
                time.sleep(stall_sec)
                after_all = _progress_snapshot(ol.copy_tasks(c, "undone"))
                after = {k: v for k, v in after_all.items() if k in mine_ids}
                static_rounds = 0 if _is_progressing(snap, after) else static_rounds + 1
                snap = after
                if static_rounds >= 2:
                    break
            if static_rounds < 2 and not settings.relay_reap_force:
                print(f"[boot] 检测到 {len(mine)} 个复制任务仍有推进（或存在变动），"
                      f"**不取消**：它们可能正在上传，取消会丢掉已下载的数据。"
                      f"如确需清理请设 RELAY_REAP_FORCE=1")
                return
            n = sum(1 for t in mine if ol.cancel(c, str(t.get("id") or "")))
            if n:
                print(f"[boot] 已取消 {n} 个僵尸中转复制任务"
                      f"（连续 {stall_sec:.0f}s × 2 无进展）")
    except Exception as e:
        print(f"[boot] 孤儿中转任务回收失败（已跳过）: {e}")


def _validate_local_mount() -> None:
    """启动校验：local_dir（本机落盘根）与 OpenList /local 挂载根必须指向同一物理目录，

    否则本机高速下载会取不到文件（配置陷阱：两处分别在 docker-compose 与 OpenList 驱动里
    手动设定，改一处忘改另一处就静默失效）。纯提示、不阻断启动（best-effort）。
    """
    local = Path(settings.local_dir)
    local.mkdir(parents=True, exist_ok=True)
    if not os.access(str(local), os.W_OK):
        print(f"[local] ⚠️ local_dir 不可写: {local}（本机高速下载将无法落盘）")
    mount = settings.openlist_mounts.get("local")
    if not mount:
        print("[local] ⚠️ 未配置 OPENLIST_MOUNT_LOCAL，本机高速目标中转将失败")
        return
    if not settings.openlist_token:
        print(f"[local] 提示: 未配置 OPENLIST_TOKEN，跳过 OpenList 挂载一致性校验"
              f"（local_dir={local}, 期望 OpenList 挂载根={mount}）")
        return
    # 可达性 + 探针：在 local_dir/transfer_dir 写哨兵，查 OpenList 是否可见
    work = local / settings.transfer_dir
    work.mkdir(parents=True, exist_ok=True)
    sentinel = ".pan_transfer_mount_check"
    sp = work / sentinel
    try:
        with httpx.Client(trust_env=False, timeout=5) as c:
            r = c.get(f"{settings.openlist_base}/api/public/settings")
            if r.status_code != 200:
                print("[local] 警告: OpenList 不可达，跳过挂载一致性校验")
                return
        sp.write_text("")
        ol = OpenListClient(settings.openlist_base, settings.openlist_token)
        probe_dir = mount.rstrip("/") + "/" + settings.transfer_dir
        with httpx.Client(trust_env=False, timeout=10) as c:
            found = ol.existing_names(c, probe_dir, [sentinel])
            if sentinel not in found:
                # OpenList 索引可能有 1s 延迟，重试一次避免误报
                time.sleep(1)
                found = ol.existing_names(c, probe_dir, [sentinel])
        if sentinel in found:
            print(f"[local] OK: local_dir({local}) 与 OpenList 挂载({mount}) 指向同一物理目录")
        else:
            print(f"[local] ⚠️ 严重: local_dir={local} 与 OpenList 挂载 {mount} 不一致！"
                  f"本机高速下载将取不到文件，请核对 OpenList Local 驱动 root 与 LOCAL_DIR")
    except Exception as e:
        print(f"[local] 警告: 挂载一致性校验异常（已跳过）: {e}")
    finally:
        try:
            sp.unlink(missing_ok=True)
        except Exception:
            pass


app = FastAPI(title="网盘转存站", lifespan=lifespan)


@app.middleware("http")
async def _security_headers(request, call_next):
    """全局安全响应头（2026-09-21 加固）：不影响任何正常使用。

    - nosniff：防浏览器把 /dl 等响应猜成别的内容类型执行；
    - DENY 嵌框架：防点击劫持（本站无任何被嵌框场景）；
    - no-referrer：站内跳出的外链（用户粘贴的网盘分享等）不带本站 URL 细节。"""
    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    return resp


async def require_token(
    x_auth_token: str | None = Header(default=None, alias="X-Auth-Token"),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> None:
    """共享鉴权（运行时读取口令，便于测试与热改）。

    接受两种 token（与 /api/auth/login 的返回值一致）：
    - 访问口令本身（APP_PASSWORD）         → 普通访问
    - 管理员口令的派生 token（sha256 派生）→ 管理员

    两个头（X-Auth-Token / X-Admin-Token）任一携带均可：统一登录后管理员只持有
    一个 token，不该因为「放错头」而被拒。

    **任一口令存在即启用鉴权**（旧实现只看 APP_PASSWORD：只设 ADMIN_PASSWORD
    不设 APP_PASSWORD 的组合会让 /api/* 完全裸奔）。两者都未配置时不鉴权
    （启动时会打印醒目告警）。比较用 secrets.compare_digest（防时序侧信道）。
    /healthz 与静态页豁免。
    """
    pwd = settings.app_password
    admin_pwd = settings.admin_password
    if not pwd and not admin_pwd:
        return
    if pwd:
        t = x_auth_token or ""
        # 明文口令与派生 token 都收：新登录发派生值（明文不再在请求头复用），
        # 存量会话（token=明文口令）不断线。管理员派生 token 走下面独立分支。
        if secrets.compare_digest(t, pwd) or secrets.compare_digest(t, _user_token(pwd)):
            return
    if admin_pwd:
        derived = _admin_token(admin_pwd)
        if secrets.compare_digest(x_auth_token or "", derived) or \
                secrets.compare_digest(x_admin_token or "", derived):
            return
    if _linuxdo_owner_from_token(x_auth_token):
        return      # Linux.do OAuth 登录的普通用户 token
    raise HTTPException(status_code=401, detail="需要访问口令（X-Auth-Token）")


# ---- 身份与权限 ----

def _admin_token(password: str) -> str:
    """管理员 token = 口令派生值（口令更换即全体失效，无需服务端会话存储）。"""
    return hashlib.sha256(("pan-admin-v1:" + password).encode()).hexdigest()


def _user_token(password: str) -> str:
    """普通用户 token = 口令派生值（2026-09-21 起，与管理员 token 同思路）。

    派生后明文口令只在登录那一出现一次，日常请求头里流转的都是派生值——
    token 在途中被截获 ≠ 口令泄露（口令可能被人在别的站点复用）。
    require_token 同时接受明文口令本身：存量会话（token=口令）平滑过渡不断线。"""
    return hashlib.sha256(("pan-user-v1:" + password).encode()).hexdigest()


def _is_from_local(cf_ip: str | None, forwarded: str | None) -> bool:
    """经 cloudflared/反向代理转发的请求会携带转发头，视为外部来源。"""
    return not (cf_ip or forwarded)


def is_admin(
    x_admin_token: str | None,
    cf_connecting_ip: str | None,
    x_forwarded_for: str | None,
) -> bool:
    """管理员判定：口令 token 匹配（常数时间比较）；口令未配置时仅本机直连放行。"""
    pwd = settings.admin_password
    if pwd:
        return bool(x_admin_token) and secrets.compare_digest(
            x_admin_token, _admin_token(pwd))
    return _is_from_local(cf_connecting_ip, x_forwarded_for)


async def admin_flag(
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_auth_token: str | None = Header(default=None, alias="X-Auth-Token"),
    cf_connecting_ip: str | None = Header(default=None, alias="CF-Connecting-IP"),
    x_forwarded_for: str | None = Header(default=None, alias="X-Forwarded-For"),
) -> bool:
    """管理员判定。

    统一登录后管理员只持有**一个** token（X-Auth-Token 与 X-Admin-Token 同值），
    所以两个头任一匹配都算管理员 —— 能出示该 token 就等价于知道管理员口令，
    不存在「拿访问口令冒充管理员」的可能（访问口令 ≠ 派生 token）。
    """
    return is_admin(x_admin_token or x_auth_token, cf_connecting_ip, x_forwarded_for)


# 登录尝试限速：按客户端 IP 每分钟最多 10 次（防公网暴力猜口令；小圈子自用够用）
_LOGIN_RL_MAX = 10
_LOGIN_RL_WINDOW = 60.0
_login_attempts: dict[str, tuple[float, int]] = {}
_login_rl_lock = threading.Lock()

# 登录审计：每次登录尝试（成功/失败/限速）都落一行到 stderr → docker logs 可查。
# ⚠️ 只记 IP 与结果，绝不记口令本身。排查「谁在用有效凭证扫接口」全靠这几行。
_auth_log = logging.getLogger("pan.auth")


def _login_rate_ok(ip: str) -> bool:
    now = time.time()
    with _login_rl_lock:
        cnt, win = _login_attempts.get(ip, (0, now))
        if now - win > _LOGIN_RL_WINDOW:
            cnt, win = 0, now
        cnt += 1
        _login_attempts[ip] = (cnt, win)
        if len(_login_attempts) > 4096:      # 防无界增长：顺手清过期项
            for k in [k for k, (c, w) in _login_attempts.items() if now - w > 300]:
                _login_attempts.pop(k, None)
        return cnt <= _LOGIN_RL_MAX


@app.post("/api/auth/login")
def api_login(
    req: dict,
    request: Request,
    cf_connecting_ip: str | None = Header(default=None, alias="CF-Connecting-IP"),
    x_forwarded_for: str | None = Header(default=None, alias="X-Forwarded-For"),
) -> dict:
    """统一登录：一个输入框，按口令判定身份（前端据 role 进入对应界面）。

    - 命中 ADMIN_PASSWORD → 管理员：返回派生 token，该 token 同时可用作
      X-Auth-Token 与 X-Admin-Token（于是管理员不必再额外知道访问口令）；
    - 命中 APP_PASSWORD   → 普通访问：token 即访问口令本身（沿用旧语义）；
    - 两者都未配置        → 本机直连视为管理员，外部来源拒绝（沿用旧行为）。

    管理员优先判定：若两个口令恰好被设成同一个值，按管理员处理（取权限更大的一方）。
    按 IP 限速（同步 def：FastAPI 自动扔线程池，sqlite/限速器阻塞不了事件循环）。
    """
    peer = request.client.host if request.client else ""
    ip = (cf_connecting_ip or x_forwarded_for or peer or "unknown").split(",")[0].strip()
    if not _login_rate_ok(ip):
        _auth_log.warning("login ip=%s result=rate_limited", ip)
        raise HTTPException(status_code=429, detail="尝试过于频繁，请 1 分钟后再试")
    supplied = str(req.get("password") or "")
    app_pwd = settings.app_password
    admin_pwd = settings.admin_password
    if admin_pwd and supplied == admin_pwd:
        _auth_log.warning("login ip=%s role=admin ok=1", ip)
        return {"role": "admin", "admin": True, "token": _admin_token(admin_pwd)}
    if app_pwd and supplied == app_pwd:
        _auth_log.warning("login ip=%s role=user ok=1", ip)
        return {"role": "user", "admin": False, "token": _user_token(app_pwd)}
    if not app_pwd and not admin_pwd:
        if not _is_from_local(cf_connecting_ip, x_forwarded_for):
            _auth_log.warning("login ip=%s result=denied_no_password_remote", ip)
            raise HTTPException(status_code=403, detail="口令未配置，仅限本机访问")
        _auth_log.warning("login ip=local role=admin ok=1 mode=no_password")
        return {"role": "admin", "admin": True, "token": _admin_token("")}
    _auth_log.warning("login ip=%s result=bad_password", ip)
    raise HTTPException(status_code=401, detail="口令错误")


# ---- Linux.do Connect（OAuth2 登录，可选）----
# 流程：前端 /api/auth/linuxdo/start → 302 到 connect.linux.do 授权 →
#       用户同意 → 回调 /api/auth/linuxdo/callback?code&state → 换 token →
#       拉 /api/user → 签发本站普通用户 token → 302 回前端 /#ld_token=...
# 身份恒为普通用户（非管理员），与口令登录平级、二选一。
# 端点（Linux.do Connect 官方，2026-09 核对）：
LD_AUTHORIZE = "https://connect.linux.do/oauth2/authorize"
LD_TOKEN = "https://connect.linux.do/oauth2/token"
LD_USER = "https://connect.linux.do/api/user"

_ld_states: dict[str, float] = {}          # state -> 过期时间（防 CSRF，一次性）
_ld_states_lock = threading.Lock()


def _linuxdo_sig(uid: str) -> str:
    """本站 Linux.do 用户 token 的签名（无状态会话：不落库，改密钥即全员下线）。"""
    secret = (settings.linuxdo_client_secret or settings.admin_password
              or settings.app_password or "pan-transfer")
    return hashlib.sha256(f"pan-ld-v1:{uid}:{secret}".encode()).hexdigest()[:32]


def _linuxdo_token(uid: str) -> str:
    return f"ld1_{uid}_{_linuxdo_sig(uid)}"


def _linuxdo_owner_from_token(token: str | None) -> str | None:
    """X-Auth-Token 是合法的 Linux.do 用户 token 时返回归属标识 ld_<uid>。"""
    if not token or not token.startswith("ld1_"):
        return None
    parts = token.split("_")
    if len(parts) != 3 or not re.fullmatch(r"[0-9]{1,12}", parts[1]):
        return None
    if not secrets.compare_digest(parts[2], _linuxdo_sig(parts[1])):
        return None
    return f"ld_{parts[1]}"


def _linuxdo_redirect_uri(request: Request) -> str:
    """回调地址：优先显式配置；否则按 Host / X-Forwarded-Proto 推导。"""
    if settings.linuxdo_redirect_uri:
        return settings.linuxdo_redirect_uri
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme or "http"
    host = request.headers.get("host") or "localhost"
    return f"{proto}://{host}/api/auth/linuxdo/callback"


@app.get("/api/auth/options")
def auth_options() -> dict:
    """登录方式清单（公开接口）：前端登录弹窗据此显示/隐藏 Linux.do 按钮。"""
    return {"password": True,
            "linuxdo": bool(settings.linuxdo_client_id
                            and settings.linuxdo_client_secret)}


@app.get("/api/auth/linuxdo/start")
async def linuxdo_start(request: Request) -> RedirectResponse:
    if not (settings.linuxdo_client_id and settings.linuxdo_client_secret):
        raise HTTPException(status_code=404, detail="Linux.do 登录未配置")
    state = uuid.uuid4().hex
    now = time.time()
    with _ld_states_lock:
        for k in [k for k, exp in _ld_states.items() if exp < now]:
            _ld_states.pop(k, None)
        if len(_ld_states) > 1024:       # 防无界增长：异常洪峰下整体重来也无妨
            _ld_states.clear()
        _ld_states[state] = now + 600
    q = urlencode({"response_type": "code", "client_id": settings.linuxdo_client_id,
                   "redirect_uri": _linuxdo_redirect_uri(request), "state": state})
    return RedirectResponse(f"{LD_AUTHORIZE}?{q}", status_code=302)


@app.get("/api/auth/linuxdo/callback")
async def linuxdo_callback(request: Request) -> RedirectResponse:
    if not (settings.linuxdo_client_id and settings.linuxdo_client_secret):
        raise HTTPException(status_code=404, detail="Linux.do 登录未配置")
    err = request.query_params.get("error") or ""
    code = request.query_params.get("code") or ""
    state = request.query_params.get("state") or ""
    if err or not code or not state:
        return RedirectResponse("/#ld_err=" + urlquote("登录被取消或参数缺失"))
    with _ld_states_lock:
        exp = _ld_states.pop(state, None)
    if not exp or exp < time.time():
        return RedirectResponse("/#ld_err=" + urlquote("登录会话已过期，请重试"))
    async with httpx.AsyncClient(trust_env=False, timeout=15) as c:
        r = await c.post(LD_TOKEN, data={
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": _linuxdo_redirect_uri(request),
            "client_id": settings.linuxdo_client_id,
            "client_secret": settings.linuxdo_client_secret})
        try:
            tok = r.json()
        except ValueError:
            tok = {}
        access = tok.get("access_token") or ""
        if r.status_code != 200 or not access:
            reason = tok.get("error") or f"HTTP {r.status_code}"
            return RedirectResponse("/#ld_err=" + urlquote(f"获取令牌失败（{reason}）"))
        r2 = await c.get(LD_USER, headers={"Authorization": f"Bearer {access}"})
        try:
            info = r2.json()
        except ValueError:
            return RedirectResponse("/#ld_err=" + urlquote("获取用户信息失败"))
    uid = info.get("id")
    if not isinstance(uid, int) or not info.get("active", False) \
            or info.get("silenced", False):
        return RedirectResponse("/#ld_err="
                                + urlquote("Linux.do 账号状态不可用（需活跃且未禁言）"))
    if (info.get("trust_level") or 0) < settings.linuxdo_min_trust_level:
        return RedirectResponse("/#ld_err=" + urlquote(
            f"信任等级不足（需 {settings.linuxdo_min_trust_level} 级及以上）"))
    owner = f"ld_{uid}"
    # token 走 URL fragment（#）：不会出现在任何服务器/代理访问日志里
    return RedirectResponse(
        f"/#ld_token={_linuxdo_token(str(uid))}&owner={owner}"
        f"&name={urlquote(str(info.get('username') or ''))}")


@app.get("/api/auth/me", dependencies=[Depends(require_token)])
def auth_me(x_auth_token: str | None = Header(default=None, alias="X-Auth-Token"),
            admin: bool = Depends(admin_flag)) -> dict:
    """当前会话身份：Linux.do 用户返回其归属标识；口令用户沿用浏览器 UUID。"""
    ld = _linuxdo_owner_from_token(x_auth_token)
    return {"admin": admin, "owner": ld,
            "kind": "linuxdo" if ld else "password"}


async def effective_owner(
    x_auth_token: str | None = Header(default=None, alias="X-Auth-Token"),
    owner: str | None = None,
) -> str | None:
    """任务归属解析：Linux.do 登录用户**强制**使用其派生身份（owner 参数仅对
    口令用户生效）——封住「改个 owner 参数看别人任务」的水平越权。"""
    return _linuxdo_owner_from_token(x_auth_token) or owner


# ---- 请求/响应模型 ----

class ParseReq(BaseModel):
    text: str = Field(min_length=1, max_length=2000)


class TransferReq(BaseModel):
    text: str = Field(min_length=1, max_length=2000, description="粘贴的分享链接（可含提取码）")
    password: str | None = Field(default=None, max_length=16, description="补充提取码（可选）")
    target: str | None = Field(default=None, max_length=16,
                               description="目标平台 baidu/quark/ali/uc/local（本机高速），默认与源相同")
    targets: list[str] | None = Field(
        default=None, max_length=20,
        description="【仅管理员】组合转存：一条链接 → 最多 5 个网盘（组内串行）")
    owner_id: str | None = Field(default=None, max_length=64,
                                 description="提交者匿名标识（浏览器生成）")


class BatchTransferReq(BaseModel):
    text: str = Field(min_length=1, max_length=20000,
                      description="多行分享链接，每行一条（一次最多 50 条）")
    password: str | None = Field(default=None, max_length=16, description="补充提取码（可选，套用到全部）")
    target: str | None = Field(default=None, max_length=16, description="目标平台，套用到全部")
    owner_id: str | None = Field(default=None, max_length=64, description="提交者匿名标识")


BATCH_MAX = 50
# 组合转存（一条链接 → 多个目标盘）单次目标数上限
GROUP_MAX = 5


# ---- 页面 ----

@app.get("/")
async def index() -> FileResponse:
    # no-cache：UI 迭代频繁，确保浏览器拿到最新页面（JS 资产内联在同一文件）
    return FileResponse(Path(__file__).parent / "static" / "index.html",
                        headers={"Cache-Control": "no-cache"})


@app.get("/favicon.svg", include_in_schema=False)
async def favicon_svg() -> FileResponse:
    """站点图标（SVG 源文件）；也是 Linux.do Connect 等外部平台「应用图标 URL」的填法之一。"""
    return FileResponse(Path(__file__).parent / "static" / "favicon.svg",
                        media_type="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=86400"})


@app.get("/icon.png", include_in_schema=False)
async def icon_png() -> FileResponse:
    """站点图标 256x256 PNG（部分平台的图标表单不收 SVG，用这张更稳）。"""
    return FileResponse(Path(__file__).parent / "static" / "icon.png",
                        media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})


# ---- API ----

@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.post("/api/parse", dependencies=[Depends(require_token)])
def api_parse(req: ParseReq) -> dict:
    try:
        share = parse_share_text(req.text)
    except ParseError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return {"platform": share.platform, "platform_label": PLATFORM_LABELS[share.platform],
            "share_id": share.share_id, "password": share.password}


def _target_label(key: str) -> str:
    return "本机高速" if key == "local" else PLATFORM_LABELS.get(key, key)


def _check_target_ready(share, target: str) -> None:
    """fail-fast：提交前校验目标可用性，不达标当场 422。

    否则用户要干等 worker 跑完才收到「平台未配置」，体验极差（点完外卖
    才告诉你店关门）。校验只读内存注册表与配置，无网络请求，零成本。
    """
    assert orch is not None
    st = orch.status()
    if not st["adapters"].get(target):
        raise HTTPException(status_code=422,
                            detail=f"目标平台「{_target_label(target)}」未配置凭据，无法转存")
    if share.platform == target:
        return
    # 跨平台依赖 OpenList 中继：token 与双端挂载缺一不可
    if not st["openlist_relay"]:
        raise HTTPException(status_code=422,
                            detail="跨平台转存需要配置 OPENLIST_TOKEN（OpenList 管理 token）")
    mounts = st["openlist_mounts"]
    missing = [p for p in (share.platform, target) if not mounts.get(p)]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"OpenList 未挂载 {'/'.join(_target_label(p) for p in missing)}"
                   f"（检查 OPENLIST_MOUNT_* 配置）")


def _create_transfer(text: str, password: str | None, target_raw: str | None,
                     owner_id: str | None, admin: bool = False) -> dict:
    """单条提交的公共实现（/api/transfer 与 /api/transfer/batch 共用）。"""
    assert orch is not None and store is not None
    try:
        share = parse_share_text(text)
    except ParseError as e:
        raise HTTPException(status_code=422, detail=str(e))
    target = target_raw or share.platform
    if target not in TARGETS:
        raise HTTPException(status_code=422,
                            detail=f"不支持的目标平台: {target}（可选：百度/夸克/阿里/UC/本机高速）")
    # 阿里作为目标永远拿不到分享链接（官方接口不支持程序化分享），仅管理员可选；
    # 服务端硬约束，避免仅靠前端置灰被绕过。
    if target == "ali" and not admin:
        raise HTTPException(status_code=403, detail="「阿里」仅管理员可选，请选择其他目标")
    pwd = validate_password(share, password)
    # 源平台为百度时必须提取码（百度分享的 verify 接口要求）；
    # 注意与目标平台无关——夸克链接转百度不需要提取码
    if share.platform == "baidu" and not pwd:
        raise HTTPException(status_code=422, detail="百度分享需要提取码")

    share_url = text[:500]
    # 同链接短时间去重：窗口内已有相同 (源,目标,分享文本) 的任务 → 复用，避免重复建任务。
    # 只复用**同一提交者**的：别人的同链接任务与己无关，挂到别人的 task_id 上
    # 既是信息面也是困惑源（「我的任务怎么失败成那样」）。
    if settings.dedup_seconds > 0:
        dup = store.find_recent(source=share.platform, target=target,
                                share_url=share_url,
                                within_seconds=settings.dedup_seconds,
                                owner_id=(owner_id or "")[:64])
        if dup and dup["status"] in ("pending", "running", "success"):
            return {"task_id": dup["id"], "source": share.platform, "target": target,
                    "deduped": True}

    _check_target_ready(share, target)
    task_id = store.create(source=share.platform, target=target, share_url=share_url,
                           password=pwd, owner_id=(owner_id or "")[:64])
    queued = bool(orch.submit(task_id, share, target, pwd))
    return {"task_id": task_id, "source": share.platform, "target": target,
            "deduped": False, "queued": queued, "queue": orch.queue_info()}


def _create_group(text: str, password: str | None, targets: list[str],
                  owner_id: str | None, admin: bool = False) -> dict:
    """组合转存：一条链接 → 多个目标盘（**仅管理员**，组内串行执行）。

    约束（按用户确认的语义实现）：
    - 目标数 1..GROUP_MAX 且不得重复；**非管理员一律拒绝**（后端硬校验，不靠前端置灰）；
    - **多目标时原链接只允许 1 条**（不支持「多条链接 × 多个目标」的笛卡尔积）；
    - 每个目标各自 fail-fast 校验可用性，任一不达标则整组不建（避免建出半残组合）；
    - 「同源同目标」是否放行由**执行期**判定：原链接若是本人分享，同源目标无意义
      （适配器会走「文件已在盘内」降级路径），该子任务判失败并给出人话原因。
    """
    assert orch is not None and store is not None
    if not admin:
        raise HTTPException(status_code=403, detail="组合转存仅管理员可用")
    tgt: list[str] = []
    for t in targets:
        s = str(t or "").strip()
        if s and s not in tgt:          # 顺带去重（重复目标直接合并，不报错骚扰）
            tgt.append(s)
    if not tgt:
        raise HTTPException(status_code=422, detail="组合转存至少需要 1 个目标")
    if len(tgt) > GROUP_MAX:
        raise HTTPException(status_code=422,
                            detail=f"组合转存一次最多 {GROUP_MAX} 个目标")

    blocks = _split_link_blocks(text)
    if len(blocks) != 1:
        raise HTTPException(
            status_code=422,
            detail=f"组合转存一次只能提交 1 条链接（当前识别到 {len(blocks)} 条），"
                   f"不支持多链接 × 多目标")
    line = blocks[0]
    try:
        share = parse_share_text(line)
    except ParseError as e:
        raise HTTPException(status_code=422, detail=str(e))
    pwd = validate_password(share, password)
    if share.platform == "baidu" and not pwd:
        raise HTTPException(status_code=422, detail="百度分享需要提取码")

    for t in tgt:                        # 逐目标 fail-fast
        if t not in TARGETS:
            raise HTTPException(status_code=422,
                                detail=f"不支持的目标平台: {t}（可选：百度/夸克/阿里/UC/本机高速）")
        if t == "ali" and not admin:
            raise HTTPException(status_code=403, detail="「阿里」仅管理员可选，请选择其他目标")
        _check_target_ready(share, t)

    share_url = line[:500]
    # 同链接去重（与单目标提交同口径）：同一分享 + 同一目标集合在窗口内重复提交
    # → 复用已有组，避免双击/回车连发建出一堆重复的组。
    if settings.dedup_seconds > 0:
        dup = store.find_recent_group(share_url=share_url, targets=tgt,
                                      within_seconds=settings.dedup_seconds,
                                      owner_id=(owner_id or "")[:64])
        if dup:
            return {"group_id": dup[0]["group_id"],
                    "task_ids": [k["id"] for k in dup], "source": share.platform,
                    "targets": tgt, "grouped": True, "deduped": True}
    group_id = uuid.uuid4().hex[:12]
    base = time.time()
    task_ids = []
    for i, t in enumerate(tgt, start=1):
        task_ids.append(store.create(
            source=share.platform, target=t, share_url=share_url, password=pwd,
            owner_id=(owner_id or "")[:64], group_id=group_id, group_seq=i,
            # 列表按 created_at DESC 排序：组内 seq=1 用最新时间，保证同组子任务
            # 相邻且按 1..N 升序出现（否则各自取 time.time() 会把顺序倒过来）。
            created_at=base - (i - 1) * 0.001))
    queued = bool(orch.submit_group(group_id, share, pwd))
    return {"group_id": group_id, "task_ids": task_ids, "source": share.platform,
            "targets": tgt, "grouped": True, "deduped": False,
            "queued": queued, "queue": orch.queue_info()}


@app.post("/api/transfer", dependencies=[Depends(require_token)])
def api_transfer(req: TransferReq, admin: bool = Depends(admin_flag),
                 x_auth_token: str | None = Header(default=None, alias="X-Auth-Token")) -> dict:
    # Linux.do 登录用户：归属强制用其派生身份（body 里的 owner_id 仅对口令用户生效）
    req.owner_id = _linuxdo_owner_from_token(x_auth_token) or req.owner_id
    if req.targets is not None:          # 组合转存（仅管理员，_create_group 内硬校验）
        return _create_group(req.text, req.password, req.targets, req.owner_id, admin=admin)
    return _create_transfer(req.text, req.password, req.target, req.owner_id, admin=admin)


@app.post("/api/transfer/batch", dependencies=[Depends(require_token)])
def api_transfer_batch(req: BatchTransferReq,
                       admin: bool = Depends(admin_flag),
                       x_auth_token: str | None = Header(default=None, alias="X-Auth-Token")) -> dict:
    """批量提交：按「链接块」拆分多条分享，逐条建任务；单条失败不影响其余。

    一条链接块 = 含 URL 的行 + 紧随其后的非 URL 行（如单独一行的「提取码：xxx」），
    用换行拼接后整体解析——这样跨行提取码不会丢（parse_share_text 会在整块内搜码）。
    """
    # Linux.do 登录用户：归属强制用其派生身份（body 里的 owner_id 仅对口令用户生效）
    req.owner_id = _linuxdo_owner_from_token(x_auth_token) or req.owner_id
    lines = _split_link_blocks(req.text)
    if not lines:
        raise HTTPException(status_code=422, detail="没有可解析的链接")
    if len(lines) > BATCH_MAX:
        raise HTTPException(status_code=422, detail=f"一次最多提交 {BATCH_MAX} 条链接")
    results = []
    for ln in lines:
        try:
            r = _create_transfer(ln, req.password, req.target, req.owner_id, admin=admin)
            results.append({"ok": True, "input": ln[:120], **r})
        except HTTPException as e:
            results.append({"ok": False, "input": ln[:120], "error": str(e.detail)})
    ok_n = sum(1 for r in results if r["ok"])
    return {"total": len(results), "ok": ok_n, "failed": len(results) - ok_n,
            "results": results}


# 链接块切分：含网盘 URL 的行开启新块；后续无 URL 的行（常见为单独一行的提取码）
# 并入上一块，确保「链接一行 + 提取码另起一行」这种常见粘贴被当作同一条链接解析。
_URL_BLOCK_HINT = re.compile(
    r"(?:pan\.baidu\.com|pan\.quark\.cn|(?:www\.)?(?:alipan|aliyundrive)\.com|drive\.uc\.cn)/s/"
)


def _split_link_blocks(text: str) -> list[str]:
    """把批量粘贴文本切成「每条链接一块」：URL 行开启新块，仅当续行像提取码
    （含「提取码/pwd=…」等）才并入上一块——避免把无关废行也吞进链接。"""
    blocks: list[list[str]] = []
    for raw in text.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        if _URL_BLOCK_HINT.search(ln):            # 含网盘链接 → 新块
            blocks.append([ln])
        elif blocks and _PASSWORD.search(ln):     # 续行且像提取码 → 并入上一块
            blocks[-1].append(ln)
        else:                                     # 其余（废行/独立密码行）→ 独立成块（解析报错反馈）
            blocks.append([ln])
    return ["\n".join(b) for b in blocks]


# 对外返回任务时裁掉的敏感字段：
#   dl_token —— 本机高速的下载凭证，**等同下载权限**（知道就能取文件）→ 必须裁掉。
# 注：`password`（分享提取码）不裁——它是提交者自己的提取码，且多数情况下本就
# 内嵌在 share_url 里（前端要展示原链接）；裁掉它只会让「原链接+提取码」的
# 自我核对失效，收益为负。
_TASK_PRIVATE_FIELDS = ("dl_token",)


def _public_task(task: dict) -> dict:
    """任务 → 对外视图（裁掉敏感字段）。"""
    return {k: v for k, v in task.items() if k not in _TASK_PRIVATE_FIELDS}


@app.get("/api/tasks", dependencies=[Depends(require_token)])
def api_tasks(limit: int = 20,
              owner: str | None = Depends(effective_owner),
              admin: bool = Depends(admin_flag)) -> dict:
    """管理员返回全部任务；普通用户必须携带自己的 owner 标识。"""
    assert store is not None
    if admin:
        tasks = store.list(max(1, min(limit, 100)))
    else:
        if not owner:
            raise HTTPException(status_code=400, detail="缺少 owner 标识")
        tasks = store.list(max(1, min(limit, 100)), owner_id=owner[:64])
    return {"tasks": [_public_task(t) for t in tasks], "admin": admin}


def _can_access(task: dict, owner: str | None, admin: bool) -> bool:
    """任务可见性：管理员任意；用户须携带 owner 且与任务归属匹配。

    旧版无主任务（owner_id 为空）一律仅管理员可见，避免匿名可按 id 探测。
    """
    if admin:
        return True
    task_owner = (task.get("owner_id") or "").strip()
    return bool(task_owner) and task_owner == (owner or "")[:64]


@app.get("/api/tasks/{task_id}", dependencies=[Depends(require_token)])
def api_task(task_id: str, owner: str | None = Depends(effective_owner),
             admin: bool = Depends(admin_flag)) -> dict:
    assert store is not None
    task = store.get(task_id)
    if task is None or not _can_access(task, owner, admin):
        # 非本人任务一律按不存在处理，不泄露信息
        raise HTTPException(status_code=404, detail="任务不存在")
    return _public_task(task)


@app.delete("/api/tasks/{task_id}", dependencies=[Depends(require_token)])
def api_task_delete(task_id: str, owner: str | None = Depends(effective_owner),
                    force: bool = False,
                    admin: bool = Depends(admin_flag)) -> dict:
    assert store is not None and orch is not None
    task = store.get(task_id)
    if task is None or not _can_access(task, owner, admin):
        raise HTTPException(status_code=404, detail="任务不存在")
    if not admin:
        # 「删除」语义（2026-09-21 起）：普通用户不再有服务端删除权限——前端把删除
        # 按钮做成本设备隐藏（localStorage），服务器记录与 /dl 下载链接都保留；
        # 永久删除（记录消失 + 链接失效 + CF 缓存清理）只有管理员能做。
        # 顺序讲究：先按可见性判 404（别人的任务不暴露存在性），再判 403。
        raise HTTPException(status_code=403,
                            detail="普通用户仅可在本设备隐藏记录；永久删除需管理员登录")
    gid = task.get("group_id") or ""
    if gid:
        # 组合转存按「一条记录」整体删除（否则会留下残缺的兄弟任务）
        children = store.list_group(gid)
        if any(c["status"] in ("pending", "running") for c in children):
            if not force:
                raise HTTPException(status_code=409, detail="组合转存中有任务进行中，无法删除")
            # force 删除前**必须取消底层执行**：只删 DB 行的话 worker 会把整个转存
            # 继续跑完（几十 GB 上传照烧带宽与风控额度），结果再被静默丢弃。
            orch.cancel_group(gid)
        n = sum(store.delete(c["id"]) for c in children)
        # 兑现「任务删除即失效」：清掉 CF 边缘可能存在的 /dl 缓存副本
        for c in children:
            _purge_dl_cache(_dl_purge_urls(c))
        return {"ok": True, "forced": force, "group": True, "deleted": n}
    if task["status"] in ("pending", "running"):
        if not force:
            # 进行中的任务默认拒绝（避免与 worker 写回竞态）；
            # 用户明确确认后可 force 删除：worker 写回对已删任务容错（KeyError 吞掉）
            raise HTTPException(status_code=409, detail="任务进行中，无法删除")
        orch.cancel(task_id)   # 同上：先取消底层执行，再删记录
    if store.delete(task_id) == 0:
        raise HTTPException(status_code=404, detail="任务不存在")
    _purge_dl_cache(_dl_purge_urls(task))
    return {"ok": True, "forced": task["status"] in ("pending", "running")}


@app.post("/api/tasks/{task_id}/cancel", dependencies=[Depends(require_token)])
def api_task_cancel(task_id: str, owner: str | None = Depends(effective_owner),
                    admin: bool = Depends(admin_flag)) -> dict:
    """取消进行中任务：置取消信号 + 立即落 cancelled（worker 阶段间检查后收手）。

    允许 pending / running / needs_confirm 三种状态取消：needs_confirm 任务 worker
    已自行退出，orch.cancel 仅做幂等标记，随后直接落 cancelled。
    比 force 硬删体面：任务记录保留（前端可见「取消」），worker 不再回写成功。
    """
    assert store is not None and orch is not None
    task = store.get(task_id)
    if task is None or not _can_access(task, owner, admin):
        raise HTTPException(status_code=404, detail="任务不存在")
    if task["status"] not in ("pending", "running", "needs_confirm"):
        raise HTTPException(status_code=409, detail="任务已结束，无法取消")
    gid = task.get("group_id") or ""
    if gid:
        # 组合转存：取消组内任一子任务 = 取消整组（含尚未执行的兄弟），否则串行
        # 执行中的兄弟会把同一份内容继续转存到其他网盘。
        n = orch.cancel_group(gid)
        return {"ok": True, "cancelled": True, "group": True, "cancelled_count": n}
    orch.cancel(task_id)  # 对 needs_confirm 任务（worker 已退出）仅为幂等空操作
    store.update(task_id, status="cancelled", progress=0, message="已取消")
    return {"ok": True, "cancelled": True}


@app.post("/api/tasks/{task_id}/confirm-continue", dependencies=[Depends(require_token)])
def api_task_confirm_continue(task_id: str, owner: str | None = Depends(effective_owner),
                              admin: bool = Depends(admin_flag)) -> dict:
    """超限确认后继续：跳过超过目标盘单文件上限的文件，转存其余文件。

    仅当任务处于 needs_confirm 时可用；重复点击或非该状态均返回 409。
    """
    assert store is not None and orch is not None
    task = store.get(task_id)
    if task is None or not _can_access(task, owner, admin):
        raise HTTPException(status_code=404, detail="任务不存在")
    if task["status"] != "needs_confirm":
        raise HTTPException(status_code=409, detail="任务不在待确认状态")
    try:
        orch.confirm_continue(task_id)
    except KeyError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ParseError as e:
        # share_url 重建解析失败（orchestrator 已把任务留在 needs_confirm 并写明原因）
        raise HTTPException(status_code=422, detail=f"分享链接解析失败：{e}")
    return {"ok": True, "resumed": True}


@app.get("/api/status", dependencies=[Depends(require_token)])
async def api_status(admin: bool = Depends(admin_flag)) -> dict:
    """平台列表公开；网盘就绪状态与 OpenList 连通性仅管理员可见。"""
    base: dict = {"platforms": [{"key": k, "label": v} for k, v in PLATFORM_LABELS.items()],
                  "targets": list(TARGETS),
                  "admin": admin,
                  # 并发概况：对所有登录用户可见（不含任何任务内容），
                  # 前端据此在提交后提示「需要排队等待」。
                  "queue": orch.queue_info() if orch is not None else None}
    if admin:
        assert orch is not None
        base["ready"] = orch.status()
        base["openlist_ok"] = await _openlist_ok()
        base["cache"] = cache_mgr.stats()
        # 访问口令未设置 = /api/* 对所有可达者开放（仅管理员端点另有口令保护）
        base["auth_open"] = not bool(settings.app_password)
    return base


async def _openlist_ok() -> bool:
    try:
        async with new_client(timeout=3.0) as c:
            r = await c.get(f"{settings.openlist_base}/api/public/settings")
            return r.status_code == 200
    except Exception:
        return False


# ---- 本机文件管理（下载历史 + 缓存清理，仅管理员） ----

@app.get("/api/local/files", dependencies=[Depends(require_token)])
def api_local_files(admin: bool = Depends(admin_flag)) -> dict:
    """本机「转存」目录清单：文件名/大小/落盘时间/下载次数（管理员可见）。"""
    if not admin:
        raise HTTPException(status_code=403, detail="仅管理员可见")
    files = cache_mgr.list_files()
    return {"files": files, "stats": cache_mgr.stats()}


@app.delete("/api/local/files/{name:path}", dependencies=[Depends(require_token)])
def api_local_file_delete(name: str, admin: bool = Depends(admin_flag)) -> dict:
    """删除本机缓存中的单个文件（管理员；支持子目录相对路径，穿越防护在
    cache_mgr.delete_file 内做：拒绝绝对路径与 .. 段 + resolve 包含校验）。"""
    if not admin:
        raise HTTPException(status_code=403, detail="仅管理员可见")
    if not cache_mgr.delete_file(name):
        raise HTTPException(status_code=404, detail="文件不存在")
    _dl_cache_drop()          # 让 /dl 的短 TTL 缓存立刻失效，别还能下到已删文件
    return {"ok": True}


# ---- 本机高速下载（/dl）：链接即凭证（dl_token），任务删除即失效 ----

# /dl 的短 TTL 缓存：一次下载会被切成几百个 Range 请求，每个都全目录 iterdir() 太浪费。
# 只缓存**命中**（失效/不存在不缓存，免得新任务撞上 404）；3 秒足够吃掉一整轮分片请求，
# 任务被删除后最多 3 秒内仍可下载（可接受，且删除接口会主动清缓存）。
_DL_CACHE_TTL = 3.0
_dl_cache: dict[str, tuple[float, dict[str, Path]]] = {}
_dl_cache_lock = threading.Lock()


def _dl_cache_drop() -> None:
    """让 /dl 缓存整体失效（删除本机文件后调用，避免 3 秒窗口内仍能取到已删文件）。"""
    with _dl_cache_lock:
        _dl_cache.clear()


def _dl_task_ok(token: str) -> dict | None:
    """凭证对应的任务是否仍可用（存在 + 成功 + 目标为本机 + 有 result_files）。"""
    assert store is not None
    task = store.get_by_dl_token(token)
    if (task is None or task.get("status") != "success"
            or task.get("target") != "local"):
        return None
    if not (task.get("result_files") or []):
        return None
    return task


def _dl_files(token: str) -> dict[str, Path] | None:
    """校验凭证对应任务存在、成功且目标为本机；返回「本任务」文件名 -> 磁盘路径 映射。

    任何不满足都按 404 处理（链接失效语义）。

    - 凭证解耦：按 dl_token 查任务（不再用 task_id），分享链接不泄露任务 ID；
    - 隔离：所有 local 任务共享同一 transfer_dir，故只暴露本任务记录的
      result_files，杜绝用 A 任务链接读取 B 任务文件（跨任务串档）。
      旧任务无记录时一律不放行（返回 None），保证不退化成全目录暴露；
    - 结果带 `_DL_CACHE_TTL` 秒缓存，省掉 Range 分片下载时的重复 iterdir；
      **但命中也要复核**（一次 DB 查询 + 对每个文件 stat）：任务刚被删除/失败、
      文件刚被缓存线程清掉时必须立刻 404 —— 「任务删除即失效」是对外承诺，
      不能为了省几次 syscall 打折。
    """
    assert store is not None
    now = time.time()
    with _dl_cache_lock:
        hit = _dl_cache.get(token)
    if hit and now - hit[0] < _DL_CACHE_TTL:
        cached = hit[1]
        if _dl_task_ok(token) and all(p.is_file() for p in cached.values()):
            return cached
        _dl_cache_drop()
    task = _dl_task_ok(token)
    if task is None:
        return None
    work = Path(settings.local_dir) / settings.transfer_dir
    if not work.is_dir():
        return None
    allowed = set(task.get("result_files") or [])
    # **递归枚举**：result_files 可能含子目录相对路径（源分享整体含文件夹时，
    # OpenList 会把目录结构原样复制进来），顶层 iterdir 会漏掉嵌套文件
    found: dict[str, Path] = {}
    for p in work.rglob("*"):
        if p.is_file():
            rel = p.relative_to(work).as_posix()
            if rel in allowed:
                found[rel] = p
    if not found:
        return None
    with _dl_cache_lock:
        if len(_dl_cache) > 512:      # 防无界增长：量小，整体淘汰即可
            _dl_cache.clear()
        _dl_cache[token] = (now, found)
    return found


def _dl_disposition(filename: str) -> str:
    """Content-Disposition 双写：ASCII 兜底名 + RFC 5987 UTF-8 真实名。

    Starlette 自带的 filename*=utf-8'' 赋值会被无条件覆盖（死代码），裸
    filename="中文"在 Firefox/Safari 乱码、非 latin-1 字符直接 UnicodeEncodeError
    ——故手动双写：filename="..." 仅放 ASCII 兜底名，真实（含中文）文件名放
    filename*=UTF-8''（RFC 5987，全浏览器支持）。
    """
    enc = urlquote(filename)
    ext = os.path.splitext(filename)[1]
    if filename.isascii() and '"' not in filename and "\\" not in filename:
        fallback = filename
    else:
        fallback = "download" + (ext if ext.isascii() else "")
    return f'attachment; filename="{fallback}"; filename*=UTF-8\'\'{enc}'


def _dl_cache_headers() -> dict[str, str]:
    """CF 边缘缓存开关：purge 三件套（token/zone/origin）配置齐全时，显式允许
    CF 缓存 /dl 响应——同一链接的多用户重复下载直接吃边缘缓存、绕过慢速隧道；
    删除任务时 _purge_dl_cache 会清掉缓存，兑现「任务删除即失效」。
    未配置时不下发 Cache-Control（注意：apk/exe 等扩展名本就在 CF 默认缓存
    列表内，此开关只是把行为变得确定可控）。"""
    if settings.cf_purge_token and settings.cf_purge_zone and settings.cf_public_origin:
        return {"Cache-Control": "public, max-age=14400"}
    return {}


def _dl_purge_urls(task: dict) -> list[str]:
    """收集一个 local 任务的所有 /dl URL（短链 + 逐文件旧式链接），供 Purge 用。"""
    if task.get("target") != "local":
        return []
    token = task.get("dl_token") or ""
    if not token:
        return []
    urls = [f"/dl/{token}"]
    for name in task.get("result_files") or []:
        urls.append(f"/dl/{token}/{urlquote(name)}")
    return urls


def _purge_dl_cache(dl_urls: list[str]) -> None:
    """任务删除后清掉 CF 边缘缓存（best-effort，失败仅记日志）。"""
    if not (settings.cf_purge_token and settings.cf_purge_zone and settings.cf_public_origin):
        return
    urls = [f"{settings.cf_public_origin}{u}" for u in dl_urls if u]
    if not urls:
        return
    try:
        with httpx.Client(trust_env=False, timeout=10) as c:
            r = c.post(
                f"https://api.cloudflare.com/client/v4/zones/{settings.cf_purge_zone}/purge_cache",
                headers={"Authorization": f"Bearer {settings.cf_purge_token}",
                         "Content-Type": "application/json"},
                json={"files": urls})
        ok = bool((r.json() or {}).get("success"))
        print(f"[purge] CF 边缘缓存清除{'成功' if ok else '失败'}: {len(urls)} 个 URL")
    except Exception as e:
        print(f"[purge] CF 缓存清除失败（忽略，不影响删除）: {e}")


def _dl_serve(files: dict, filename: str, request: Request) -> FileResponse:
    """吐文件 + 下载计数。Range 分片（视频续传/多线程下载）不计数，防刷爆统计。"""
    if not request.headers.get("range"):
        host = request.client.host if request.client else ""
        cache_mgr.record_download(filename, client=host)
    # FileResponse 原生支持 Range 断点续传（starlette >= 0.33）
    # 下载保存名用 base name（子目录路径不进文件名），缓存头按需下发
    return FileResponse(files[filename], media_type="application/octet-stream",
                        headers={"Content-Disposition": _dl_disposition(Path(filename).name),
                                 **_dl_cache_headers()})


@app.get("/dl/{token}")
def dl_index(token: str, request: Request):
    """单文件任务：/dl/<token> **直接下载**（短链）。

    文件名太长时 URL 会变成一长串 %XX 转义，又丑又难复制——真实文件名改走
    Content-Disposition，浏览器保存时仍然用原名，链接本身只剩凭证。
    多文件任务：返回文件清单页。
    """
    files = _dl_files(token)
    if not files:
        raise HTTPException(status_code=404, detail="下载链接不存在或已失效")
    if len(files) == 1:
        return _dl_serve(files, next(iter(files)), request)

    def _mb(p: Path) -> str:
        try:
            return f"{p.stat().st_size / 2 ** 20:.1f} MB"
        except OSError:
            return "大小未知"   # 文件恰好被清理线程删掉：别让 500 毁掉整个清单页

    items = "".join(
        f'<li><a href="/dl/{escape(token)}/{urlquote(name)}">{escape(name)}</a>'
        f' <small>({_mb(files[name])})</small></li>'
        for name in sorted(files))
    return HTMLResponse(
        f"<meta charset='utf-8'><title>下载</title>"
        f"<h3>本机高速下载 · {len(files)} 个文件</h3><ul>{items}</ul>")


@app.get("/dl/{token}/{filepath:path}")
def dl_file(token: str, filepath: str, request: Request) -> FileResponse:
    """带相对路径的下载路由（多文件/子目录；旧链接兼容）。

    filepath 为空串（/dl/<token>/ 尾斜杠）时等价于清单/单文件入口。
    """
    files = _dl_files(token)
    if not files:
        raise HTTPException(status_code=404, detail="下载链接不存在或已失效")
    if not filepath:
        return dl_index(token, request)
    if filepath not in files:
        raise HTTPException(status_code=404, detail="下载链接不存在或已失效")
    return _dl_serve(files, filepath, request)


def main() -> None:  # 本地调试入口：python main.py
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=settings.app_port)


if __name__ == "__main__":
    main()
