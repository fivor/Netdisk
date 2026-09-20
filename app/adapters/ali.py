"""阿里云盘适配器。

个人开发者申请已于 2025-07 起被官方暂停（企业通道除外），因此 token
获取采用 OpenList 生态通行方案：
- refresh_token 从 https://api.oplist.org/ （OpenList 官方工具，勾选
  "使用 OpenList 提供的参数"）获取，绑定 OpenListTeam 应用；
- 刷新走 OpenList 官方在线续期 API（renewapi），与 OpenList 本体的
  "使用在线 API" 完全同源；
- 阿里 refresh_token 会轮换，适配器自动把最新值持久化到
  <db_path 目录>/ali_token_state.json，重启后接着用。

若未来拿到企业应用的 client_id/client_secret，填入 .env 即自动切换
为官方直连刷新（不再经过在线续期）。

⚠️ 风控红线（方案 5.3）：阿里 token 严禁多 IP 频繁访问；
   本适配器全部流量走家宽直连（http_client 强制 trust_env=False）。
"""
from __future__ import annotations

import json
import os
import re
import threading
import time

from adapters.base import (Adapter, AdapterError, AdapterNotConfigured,
                           CapabilityError, ProgressFn, SavedRef,
                           TransferResult, _noop_progress)
from parser import ShareLink

BASE = "https://openapi.alipan.com"


class AliAdapter(Adapter):
    platform = "ali"
    label = "阿里云盘"

    # 类级共享 token 状态：同进程所有实例（每任务新建）共享缓存。
    # 刷新用类级锁串行化：阿里 refresh_token 是**轮换型**（刷新即作废旧值），
    # 并发双刷会让后持久化者把状态文件回退成已作废的旧 token → 凭据永久失效、
    # 只能人工重新获取。旧注释「刷新为幂等操作，至多多刷一次」的前提是错的。
    # （各 worker 是独立线程独立事件循环，跨 await 持有 threading.Lock 只会让
    #   并发刷新者等几秒，无死锁面——死锁需要同一线程重复获取。）
    _cache: dict = {}  # base_rt -> {"access_token", "exp", "current_rt"}；另有 "<base>:drive_id"
    _rt_lock = threading.Lock()

    def configured(self) -> bool:
        return bool(getattr(self.settings, "ali_refresh_token", ""))

    # ---- token 管理 ----

    def _base_rt(self) -> str:
        return self.settings.ali_refresh_token

    def _state_file(self) -> str:
        db = getattr(self.settings, "db_path", "") or "/data/app/tasks.db"
        return os.path.join(os.path.dirname(db), "ali_token_state.json")

    def _load_current_rt(self) -> str:
        """取当前可用 refresh_token：内存缓存 → 状态文件 → env 原始值。

        env 被用户更新（与状态文件记录的 base 不一致）时以 env 为准重置。
        """
        base = self._base_rt()
        st = AliAdapter._cache.get(base)
        if st and st.get("current_rt"):
            return st["current_rt"]
        try:
            with open(self._state_file(), encoding="utf-8") as f:
                data = json.load(f)
            if data.get("base") == base and data.get("current_rt"):
                return data["current_rt"]
        except (OSError, ValueError):
            pass
        return base

    def _persist_rt(self, base_rt: str, current_rt: str) -> None:
        try:
            p = self._state_file()
            os.makedirs(os.path.dirname(p), exist_ok=True)
            tmp = p + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"base": base_rt, "current_rt": current_rt}, f)
            os.replace(tmp, p)
        except OSError as e:
            print(f"[ali] refresh_token 持久化失败（重启后可能需重新获取）: {e}")

    async def _refresh_official(self, c, current_rt: str) -> tuple[str, str]:
        """企业应用直连官方刷新（client_id/secret 已配置时）。"""
        r = await c.post(f"{BASE}/oauth/access_token", json={
            "grant_type": "refresh_token", "refresh_token": current_rt,
            "client_id": self.settings.ali_client_id,
            "client_secret": self.settings.ali_client_secret,
        })
        try:
            data = r.json()
        except ValueError:
            raise AdapterError(f"阿里 access_token 获取失败（HTTP {r.status_code}，非 JSON）")
        if not isinstance(data, dict):
            raise AdapterError(f"阿里 access_token 获取失败（HTTP {r.status_code}，响应非对象）")
        access = data.get("accessToken") or ""
        refresh = data.get("refreshToken") or data.get("refresh_token") or ""
        if not access or not refresh:
            raise AdapterError(
                f"阿里 access_token 获取失败: {data.get('code', '')} {data.get('message', '')}")
        return access, refresh

    async def _refresh_online(self, c, current_rt: str) -> tuple[str, str]:
        """OpenList 官方在线续期（个人用户的通行方案，同 OpenList use_online_api）。"""
        url = getattr(self.settings, "ali_online_renew_api", "") or \
            "https://api.oplist.org/alicloud/renewapi"
        r = await c.get(url, params={
            "refresh_ui": current_rt,
            "server_use": "true",
            "driver_txt": getattr(self.settings, "ali_driver_txt", "") or "alicloud_qr",
        })
        try:
            data = r.json()
        except ValueError:
            raise AdapterError(f"阿里在线续期返回异常（HTTP {r.status_code}，非 JSON）")
        access = data.get("access_token") or ""
        refresh = data.get("refresh_token") or ""
        if not access or not refresh:
            msg = data.get("text") or ""
            raise AdapterError(
                f"阿里 token 在线续期失败: {msg or '返回为空（refresh_token 可能无效，请重新获取）'}")
        return access, refresh

    async def _token(self, c) -> str:
        base = self._base_rt()
        now = time.time()
        # 快路径：缓存命中直接返回（纯内存读，不持锁）
        st = AliAdapter._cache.get(base)
        if st and st.get("access_token") and now < st.get("exp", 0) - 120:
            return st["access_token"]
        # 慢路径：类级锁串行化刷新。锁内先**复查缓存**（double-check：别的线程
        # 可能刚刷完），避免对轮换型 refresh_token 并发双刷把状态文件回退成废值。
        with AliAdapter._rt_lock:
            st = AliAdapter._cache.get(base)
            if st and st.get("access_token") and time.time() < st.get("exp", 0) - 120:
                return st["access_token"]
            current_rt = self._load_current_rt()
            if getattr(self.settings, "ali_client_id", "") and \
                    getattr(self.settings, "ali_client_secret", ""):
                access, new_rt = await self._refresh_official(c, current_rt)
            else:
                access, new_rt = await self._refresh_online(c, current_rt)
            AliAdapter._cache[base] = {"access_token": access, "exp": time.time() + 7200,
                                       "current_rt": new_rt}
            if new_rt != current_rt:
                self._persist_rt(base, new_rt)
            return access

    async def _api(self, c, path: str, body: dict) -> dict:
        """带 401 自动重试的 API 调用（在线续期按 2h 缓存，若提前失效则强刷一次）。"""
        token = await self._token(c)
        r = await c.post(f"{BASE}{path}", json=body,
                         headers={"Authorization": f"Bearer {token}",
                                  "Content-Type": "application/json"})
        if r.status_code == 401:
            AliAdapter._cache.pop(self._base_rt(), None)  # 清缓存强刷一次
            token = await self._token(c)
            r = await c.post(f"{BASE}{path}", json=body,
                             headers={"Authorization": f"Bearer {token}",
                                      "Content-Type": "application/json"})
        return await self._check(r)

    async def _check(self, resp) -> dict:
        if resp.status_code == 401:
            raise AdapterError("阿里授权失效（refresh_token 需重新获取）")
        try:
            data = resp.json()
        except Exception:
            raise AdapterError(f"阿里接口返回非 JSON（HTTP {resp.status_code}）")
        if resp.status_code >= 400 or data.get("code"):
            raise AdapterError(
                f"阿里接口错误 code={data.get('code', resp.status_code)} msg={data.get('message', '')}")
        return data

    # ---- save：他人分享 → 自己网盘 ----
    # ⚠️ 能力边界（2026-09 实测探测确认）：阿里个人开放 API（openapi.alipan.com）
    #    的 v2/v4 分享接口面（share_link/*、share/*）整体不存在，仅 v1.0
    #    openFile/* 文件管理可用；Web API（api.alipan.com）的分享保存需要
    #    Web 登录态（OpenAPI token 会 401）。因此「保存他人分享」在
    #    OpenAPI token 下不可用，除非未来配置企业应用或 Web token。

    async def save(self, share: ShareLink, password: str | None,
                   progress: ProgressFn) -> SavedRef:
        raise CapabilityError(
            "阿里官方个人开放 API 不提供「保存他人分享」接口（企业应用通道 2025-07 起暂停）。"
            "阿里分享链接暂时无法转存，可改用夸克/百度链接，或把文件转存到阿里盘后手动分享")

    # ---- share：已保存文件 → 新分享 ----

    async def share(self, ref: SavedRef, progress: ProgressFn) -> TransferResult:
        raise CapabilityError(
            "阿里官方个人开放 API 不提供「创建分享」接口，无法程序化生成阿里分享链接。"
            "文件已可转存到你的阿里云盘，请在网盘 App 内手动分享")

    # ---- locate：在自己网盘内按名定位文件（OpenAPI v1.0 已验证可用） ----

    async def _drive_id(self, c) -> str:
        """真实 drive_id（getDriveInfo → resource_drive_id），进程内缓存。

        缓存键带 base_rt：换账号（换 refresh_token）后旧 drive_id 不能复用，
        否则 locate 会全部打到旧账号的盘上。
        """
        key = "__drive_id__:" + self._base_rt()
        cached = AliAdapter._cache.get(key)
        if cached:
            return cached
        token = await self._token(c)
        r = await c.post(f"{BASE}/adrive/v1.0/user/getDriveInfo", headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={})
        if r.status_code != 200:
            raise AdapterError(f"阿里 getDriveInfo 失败（HTTP {r.status_code}）")
        try:
            payload = r.json()
        except ValueError:
            raise AdapterError(f"阿里 getDriveInfo 返回非 JSON（HTTP {r.status_code}）")
        rid = (payload.get("resource_drive_id") if isinstance(payload, dict) else "") or ""
        if not rid:
            raise AdapterError("阿里 getDriveInfo 未返回 resource_drive_id")
        AliAdapter._cache[key] = rid
        return rid

    async def locate(self, names: list[str], mount_dir: str = "",
                     progress: ProgressFn = _noop_progress) -> SavedRef:
        """mount_dir 语义：目录名（如 settings.transfer_dir）或 file_id；
        传目录名时自动解析为 file_id。"""
        if not self.configured():
            raise AdapterNotConfigured("阿里云盘 refresh_token 未配置（api.oplist.org 获取）")
        c = self.client()
        progress(75, "定位转存文件")
        rid = await self._drive_id(c)
        parent = mount_dir or "root"
        if mount_dir and mount_dir != "root" and not re.fullmatch(r"[0-9a-f]{32,}", mount_dir):
            # 目录名 → file_id（从根目录第一页找同名文件夹）
            data = await self._api(c, "/adrive/v1.0/openFile/list", {
                "drive_id": rid, "parent_file_id": "root", "limit": 100})
            fid = next((it["file_id"] for it in (data.get("items") or [])
                        if it.get("type") == "folder"
                        and it.get("name") == mount_dir), None)
            if not fid:
                raise AdapterError(
                    f"阿里云盘内未找到「{mount_dir}」目录（请在网盘中创建）")
            parent = fid
        data = await self._api(c, "/adrive/v1.0/openFile/list", {
            "drive_id": rid, "parent_file_id": parent, "limit": 100})
        items = {it.get("name") or it.get("file_name"): it
                 for it in (data.get("items") or [])}
        refs = [{"file_id": items[n]["file_id"]} for n in names if n in items]
        if not refs:
            raise AdapterError("阿里云盘内未找到转存后的文件（检查 OpenList 阿里挂载与复制目录）")
        return SavedRef(platform=self.platform, names=names,
                        mount_dir=mount_dir, refs=refs)
