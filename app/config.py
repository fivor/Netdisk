"""配置加载：环境变量 + 可选 .env 文件（键值对格式）。

安全约束：
- 凭据只经环境变量进入容器，不落盘、不写日志；
- 所有对外 HTTP 调用必须复用 http_client.py 的会话（trust_env=False）。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _load_dotenv(path: str | None = None) -> None:
    """极简 .env 加载（KEY=VALUE），不覆盖已存在的环境变量。"""
    p = Path(path or os.environ.get("ENV_FILE", ".env"))
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv()


def _clean_cookie(raw: str) -> str:
    """容忍多行/带引号的 Cookie 粘贴。"""
    return " ".join(raw.replace("\n", " ").split()).strip()


def _env_bool(name: str, default: bool) -> bool:
    """布尔环境变量：大小写不敏感（FALSE/False/false 等价），未设置用默认值。

    旧实现 `not in ("", "0", "false")` 是大小写敏感的——把 RELAY_REAP_FORCE=FALSE
    当成「开启」这种事故必须杜绝（这是个明知毁数据才用的开关）。
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("", "0", "false", "no", "off")


@dataclass
class Settings:
    app_port: int = int(os.environ.get("APP_PORT", "8100"))
    db_path: str = os.environ.get("DB_PATH", "/data/app/tasks.db")
    cache_dir: str = os.environ.get("CACHE_DIR", "/data/cache")

    # OpenList（转存引擎底座）
    openlist_base: str = os.environ.get("OPENLIST_BASE", "http://openlist:5244").rstrip("/")
    openlist_token: str = os.environ.get("OPENLIST_TOKEN", "")
    # 各平台在 OpenList 中的挂载目录（跨平台中转：源盘 save → fs/copy → 目标盘 locate）
    openlist_mounts: dict = field(default_factory=lambda: {
        "baidu": os.environ.get("OPENLIST_MOUNT_BAIDU", "/baidu"),
        "quark": os.environ.get("OPENLIST_MOUNT_QUARK", "/quark"),
        "ali": os.environ.get("OPENLIST_MOUNT_ALI", "/ali"),
        "uc": os.environ.get("OPENLIST_MOUNT_UC", "/uc"),
        "local": os.environ.get("OPENLIST_MOUNT_LOCAL", "/local"),
    })

    # 本机高速下载：缓存根目录 + TTL/容量（local 目标的落盘位置）
    local_dir: str = os.environ.get("LOCAL_DIR", "/data/downloads")
    cache_ttl_days: int = int(os.environ.get("CACHE_TTL_DAYS", "7"))
    cache_max_gb: float = float(os.environ.get("CACHE_MAX_GB", "300"))

    # 网盘凭据
    baidu_cookie: str = field(default_factory=lambda: _clean_cookie(os.environ.get("BAIDU_COOKIE", "")))
    quark_cookie: str = field(default_factory=lambda: _clean_cookie(os.environ.get("QUARK_COOKIE", "")))
    uc_cookie: str = field(default_factory=lambda: _clean_cookie(os.environ.get("UC_COOKIE", "")))
    ali_client_id: str = os.environ.get("ALI_CLIENT_ID", "")
    ali_client_secret: str = os.environ.get("ALI_CLIENT_SECRET", "")
    ali_refresh_token: str = os.environ.get("ALI_REFRESH_TOKEN", "")
    # 阿里在线续期（个人用户通行方案，同 OpenList use_online_api）
    ali_online_renew_api: str = os.environ.get("ALI_ONLINE_RENEW_API",
                                               "https://api.oplist.org/alicloud/renewapi")
    ali_driver_txt: str = os.environ.get("ALI_DRIVER_TXT", "alicloud_qr")

    # 业务参数
    task_workers: int = int(os.environ.get("TASK_WORKERS", "2"))
    baidu_share_days: int = int(os.environ.get("BAIDU_SHARE_DAYS", "7"))
    ali_share_days: int = int(os.environ.get("ALI_SHARE_DAYS", "7"))
    # 转存产物统一存放的目录名（各网盘根目录下，不存在时自动创建）
    transfer_dir: str = os.environ.get("TRANSFER_DIR", "转存")

    # ---- Linux.do Connect（OAuth2 登录，可选；两个值都填了才启用）----
    # 在 https://connect.linux.do/dash/sso/new 申请接入，回调地址填
    #   https://<你的域名>/api/auth/linuxdo/callback
    # 登录后的身份恒为普通用户（非管理员），与访问口令登录平级、二选一。
    linuxdo_client_id: str = os.environ.get("LINUXDO_CLIENT_ID", "").strip()
    linuxdo_client_secret: str = os.environ.get("LINUXDO_CLIENT_SECRET", "").strip()
    # 信任等级门槛（0-4）：默认 0 不限制；建议 1 挡住新注册小号
    linuxdo_min_trust_level: int = int(os.environ.get("LINUXDO_MIN_TRUST_LEVEL", "0"))
    # 回调地址：留空则按请求的 Host/X-Forwarded-Proto 动态推导；域名固定时建议显式配置
    linuxdo_redirect_uri: str = os.environ.get("LINUXDO_REDIRECT_URI", "").strip()

    # ---- 跨平台中转（OpenList 复制）保护参数 ----
    # OpenList 跨盘 copy 会「先下载到本机临时文件、再上传目标」，因此：
    #  · 停滞检测：字节连续 STALL_MIN 分钟零增长才判卡死（替代固定超时）
    #  · 硬上限：单个中转任务最长 MAX_HOURS 小时
    #  · 空间校验：最大单文件 × SAFETY ≤ 中转盘剩余空间，否则拒绝（避免撑爆磁盘）
    relay_stall_min: float = float(os.environ.get("RELAY_STALL_MIN", "15"))
    relay_max_hours: float = float(os.environ.get("RELAY_MAX_HOURS", "24"))
    # 启动期「孤儿中转任务」回收的静止判据：连续两轮（各间隔 N 秒）progress 完全不动
    # 才认定僵尸并取消。旧实现只看 6 秒，大文件在分片校验/秒传比对时恰好 6 秒无进展
    # 就会被误杀（正是 2026-09-18 那次丢数据的场景）。
    relay_reap_stall_sec: float = float(os.environ.get("RELAY_REAP_STALL_SEC", "45"))
    relay_disk_safety: float = float(os.environ.get("RELAY_DISK_SAFETY", "1.2"))
    # OpenList 临时目录（只读挂进本容器，用于计量「在途」字节：下载中但尚未上传完）
    openlist_temp_dir: str = os.environ.get("OPENLIST_TEMP_DIR", "/data/openlist-temp")
    # 启动时回收：取消我们中转目录下「无人认领**且完全停滞**」的复制任务。
    # ⚠️ 只会取消「两次采样 progress 完全没变化」的僵尸任务——重启时若 OpenList
    # 仍在正常上传，一律保留（取消会把已下载的临时数据一起废掉）。
    relay_reap_on_boot: bool = _env_bool("RELAY_REAP_ON_BOOT", True)
    # 强制清理开关：置 1 时忽略「仍在推进」判断，直接取消中转目录下的所有未完成任务
    # （明知会毁掉在途上传时才会用，例如磁盘告急必须立刻止血）
    relay_reap_force: bool = _env_bool("RELAY_REAP_FORCE", False)

    # 单文件上限（GB）：跨平台中转/直转时，目标盘单文件超过此值会被服务端拒绝。
    # 实测值（2026-09-18）：百度单文件上限 8GB（约 79.97% 处被拒）、夸克 20GB；
    # 其他网盘（ali/uc）暂时不做限制（置 0 = 无上限）。local（本机高速）落本机、
    # 无云盘单文件上限，不在此表即视为无上限。
    # 超限时不整单失败，而是暂停并就「哪些文件超限」向用户确认：继续则跳过超限文件
    # 转存其余，取消则整单作罢（见 orchestrator._relay 的 needs_confirm 流程）。
    # 开了会员想调高，用 OPENLIST_CAP_<平台>_GB 覆盖即可。
    target_file_cap_gb: dict = field(default_factory=lambda: {
        "baidu": float(os.environ.get("OPENLIST_CAP_BAIDU_GB", "8")),
        "quark": float(os.environ.get("OPENLIST_CAP_QUARK_GB", "20")),
        "ali":   float(os.environ.get("OPENLIST_CAP_ALI_GB", "0")),
        "uc":    float(os.environ.get("OPENLIST_CAP_UC_GB", "0")),
    })
    # 访问口令：非空时所有 /api/* 要求 X-Auth-Token 头（/healthz 与静态页除外）。
    # CF Access 未启用前的过渡防线；启用 CF Access 后可留空作为纵深防御。
    app_password: str = os.environ.get("APP_PASSWORD", "")
    # 管理员口令：非空时凭口令换取管理员 token（查看全部任务 + 网盘就绪状态）；
    # 为空时仅限本机直连（无代理/隧道转发头）自动获得管理员权限（开发阶段）。
    admin_password: str = os.environ.get("ADMIN_PASSWORD", "")

    # 完成通知（可选）：Bark 推送地址前缀，如 https://api.day.app/<KEY>。
    # 非空时任务成功落终态会 GET <前缀>/<标题>/<正文> 推送到手机。
    bark_url: str = os.environ.get("BARK_URL", "")
    # 同链接短时间去重窗口（秒）：窗口内重复提交相同 (源,目标,分享文本) 直接复用旧任务
    dedup_seconds: int = int(os.environ.get("DEDUP_SECONDS", "600"))

    # CF 边缘缓存加速（可选，三项齐全才启用）：让 /dl 文件在 CF 边缘缓存，
    # 同一链接的多用户重复下载直接吃边缘、绕过慢速隧道；删除任务时自动 Purge
    # 兑现「任务删除即失效」。需要：CF_PURGE_TOKEN（Zone.Cache Purge 权限的
    # API Token）+ CF_PURGE_ZONE（Zone ID）+ CF_PUBLIC_ORIGIN（公网来源，
    # 如 https://your-domain.example）。未配置时不下发 Cache-Control（维持现状）。
    cf_purge_token: str = os.environ.get("CF_PURGE_TOKEN", "")
    cf_purge_zone: str = os.environ.get("CF_PURGE_ZONE", "")
    cf_public_origin: str = os.environ.get("CF_PUBLIC_ORIGIN", "").rstrip("/")


settings = Settings()
