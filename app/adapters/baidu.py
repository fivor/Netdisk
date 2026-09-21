"""百度网盘适配器（Cookie 方案，参考 BaiduPanFilesTransfers / BaiduPCS-Py）。

save 流程：verify 提取码(BDCLND) → 分享页取 shareid/uk → share/list 文件列表
           → share/transfer 服务器端转存（落到「转存」目录，按名定位副本）
share 流程：share/set 对已保存文件（fid_list）生成新私密分享
locate 流程：/api/list 按名定位已保存文件（跨平台目标盘侧）

errno 速查（常见）：0 成功；-9 提取码错误；-62 请求过于频繁（风控）；
-12 已禁止访问；105 链接错误；-70 属主文件违规。
"""
from __future__ import annotations

import asyncio
import json
import random
import re
import string
import time as _time
from typing import Any
from urllib.parse import quote, unquote

from adapters.base import (Adapter, AdapterError, AdapterNotConfigured,
                           ProgressFn, SavedRef, TransferResult, _noop_progress)
from parser import ShareLink

APP_ID = "250528"
BASE = "https://pan.baidu.com"
# 分享页抓取用浏览器 UA（shorturlinfo 接口对 API 侧调用有风控，页面本身公开可访问）
_PAGE_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
ERRNO_MSG = {
    2: "分享链接已失效或不存在（可能已被取消/删除）",
    -9: "提取码错误",
    -62: "请求过于频繁，可能触发风控，请稍后再试",
    -12: "分享链接已被取消或禁止访问",
    105: "分享链接错误",
    -70: "分享内容涉嫌违规，无法转存",
    -20: "操作过于频繁，触发频控",
    -7: "分享参数校验失败（sekey 无效或触发风控），请稍后重试",
}


class BaiduAdapter(Adapter):
    platform = "baidu"
    label = "百度网盘"

    def configured(self) -> bool:
        raw = getattr(self.settings, "baidu_cookie", "")
        jar = self.parse_cookie(raw) if raw else {}
        return "BDUSS" in jar

    def _headers(self, cookie: str | None = None) -> dict[str, str]:
        return {
            "Referer": BASE + "/",
            "Cookie": cookie or getattr(self.settings, "baidu_cookie", ""),
        }

    @staticmethod
    def _err(errno: int, show_msg: str = "") -> AdapterError:
        # show_msg 是百度的原始人话提示（如"文件已存在"/"链接出错了"），优先透传
        if show_msg and "文件已存在" in show_msg:
            return AdapterError(
                "文件已存在于你的网盘中——自己网盘分享出去的文件无需转存，"
                "可直接在网盘内对它生成新分享")
        msg = ERRNO_MSG.get(errno, f"百度接口错误 errno={errno}")
        if show_msg:
            msg += f"（百度提示：{show_msg}）"
        if errno == -62:
            msg += "（提示：减小批量转存频率）"
        return AdapterError(msg)

    @staticmethod
    def _json(resp) -> dict:
        """百度风控时会返回 HTML 页而非 JSON，统一转译成人话。"""
        try:
            return resp.json()
        except ValueError:
            raise AdapterError(f"百度接口返回异常（HTTP {resp.status_code}，可能触发风控，请稍后再试）")

    async def _share_meta_via_init_page(self, c, surl: str, pwd: str) -> dict | None:
        """主路径：/share/init 落地页提取 shareid / share_uk（不依赖登录态）。

        实测（2026-09-16）：该页内嵌 share_uk:"890984392","shareid":23844440942。
        百度对无 Cookie 高频请求会**间歇性 302**（风控抖动），必须重试。
        文件列表由 share/list 拉取。
        """
        for attempt in range(3):
            try:
                r = await c.get(
                    f"{BASE}/share/init",
                    params={"surl": surl, "pwd": pwd},
                    headers={"User-Agent": _PAGE_UA,
                             "Accept": "text/html,application/xhtml+xml,application/xml;"
                                       "q=0.9,*/*;q=0.8",
                             "Accept-Language": "zh-CN,zh;q=0.9"},
                )
                if r.status_code == 200:
                    html = r.text
                    m_sid = re.search(r'"shareid"\s*[:=]\s*"?(\d+)', html)
                    m_uk = re.search(r'share_uk["\']?\s*[:=]\s*["\']?(\d+)', html)
                    if m_sid and m_uk:
                        return {"shareid": m_sid.group(1), "uk": m_uk.group(1)}
                # 302/风控抖动 → 退避重试
                await asyncio.sleep(1.2 * (attempt + 1))
            except Exception:
                await asyncio.sleep(1.2 * (attempt + 1))
        return None

    async def _share_meta_via_page(self, c, surl_full: str, pwd: str,
                                   cookie: str) -> dict | None:
        """备用路径：抓完整分享页 /s/1xxx（带 Cookie 时 200，内嵌元数据）。

        显式不跟随重定向（百度偶发 302 到去前缀的伪 404 页），带重试。
        解析失败返回 None。
        """
        for attempt in range(3):
            try:
                r = await c.get(
                    f"{BASE}/s/{surl_full}", params={"pwd": pwd},
                    headers={"User-Agent": _PAGE_UA,
                             "Accept-Language": "zh-CN,zh;q=0.9",
                             "Cookie": cookie},
                    follow_redirects=False,
                )
                if r.status_code == 200:
                    html = r.text
                    m_sid = re.search(r'"shareid"\s*[:=]\s*"?(\d+)', html)
                    m_uk = re.search(r'share_uk["\']?\s*[:=]\s*["\']?(\d+)', html)
                    if m_sid and m_uk:
                        return {"shareid": m_sid.group(1), "uk": m_uk.group(1)}
                await asyncio.sleep(1.2 * (attempt + 1))
            except Exception:
                await asyncio.sleep(1.2 * (attempt + 1))
        return None

    # ---- save：他人分享 → 自己网盘 ----

    async def save(self, share: ShareLink, password: str | None,
                   progress: ProgressFn) -> SavedRef:
        if not self.configured():
            raise AdapterNotConfigured("百度网盘 Cookie 未配置（需要 BDUSS）")
        c = self.client()
        surl = share.share_id  # parser 已去掉前导 1
        pwd = (password or "").strip()
        if not pwd:
            raise AdapterError("百度分享必须提供提取码")

        # 1. 校验提取码 -> randsk
        progress(5, "校验提取码")
        t = int(random.random() * 0x7FFFFFFF)
        r = await c.post(
            f"{BASE}/share/verify",
            params={"surl": surl, "t": t, "channel": "chunlei", "web": "1",
                    "app_id": APP_ID, "clienttype": "web"},
            data={"pwd": pwd},
            headers=self._headers(),
        )
        data = self._json(r)
        if data.get("errno") != 0 or not data.get("randsk"):
            raise self._err(data.get("errno", -1))
        # randsk 本身已是 URL 编码形态（含 %2F 等）：
        # - Cookie 里原样放（BDCLND=<已编码值>）
        # - query 参数里需先 unquote 还原，再由 httpx 自动编码（否则双重编码 → errno=2）
        randsk = data["randsk"]
        headers = self._headers(
            getattr(self.settings, "baidu_cookie", "") + f"; BDCLND={randsk}")
        sekey = unquote(randsk)

        # 2. shareid / uk（主路径：/share/init 落地页，稳定且不依赖 Cookie；
        #    shorturlinfo 接口对 API 调用有风控，页面 /s/1xxx 也偶发 302）。
        #    三级链路对风控抖动都脆弱（2026-09-20 实锤：init 连续 302、shorturlinfo
        #    errno=2），整链失败后歇 2s 再跑一轮，把「偶发」压成「几乎必成」。
        progress(20, "获取分享文件列表")
        meta = None
        for rnd in range(2):
            meta = await self._share_meta_via_init_page(c, surl, pwd)
            if meta is None:
                # 备用 1：带 Cookie 抓完整分享页 /s/1xxx
                meta = await self._share_meta_via_page(c, f"1{surl}", pwd, headers["Cookie"])
            if meta is not None:
                break
            if rnd == 0:
                await asyncio.sleep(2.0)
        if meta is not None:
            shareid, uk = meta["shareid"], meta["uk"]
        else:
            # 备用 2：shorturlinfo。注意 errno=2 ≠ 分享失效——该接口被风控拒绝时
            # 也返回 2（2026-09-20 实锤：同一分享 share/list errno=0 活得好好的），
            # 错文案绝不能咬定「链接已失效」误导用户，应如实说「请重试」。
            r = await c.get(
                f"{BASE}/api/shorturlinfo",
                params={"appid": APP_ID, "clienttype": "web", "surl": f"1{surl}",
                        "root": "1"},
                headers=headers,
            )
            data = self._json(r)
            if data.get("errno") != 0:
                raise AdapterError(
                    "暂时无法获取分享信息（百度风控临时拒绝），分享本身可能是有效的，"
                    f"请稍后重试（errno={data.get('errno')}，"
                    f"百度提示：{data.get('show_msg') or '无'}）")
            shareid, uk = str(data["shareid"]), str(data["uk"])

        # 3. 文件列表：share/list（shareid + uk + sekey=randsk）
        r = await c.get(
            f"{BASE}/share/list",
            params={"shareid": shareid, "uk": uk, "root": "1", "sekey": sekey},
            headers=headers,
        )
        data = self._json(r)
        if data.get("errno") != 0:
            raise self._err(data.get("errno", -1))
        files = data.get("list") or []
        # fs_id 与文件名按**单文件配对**提取：旧的独立推导式在个别条目缺名时错位
        # （转存了该文件但 SavedRef.names 没有 → 跨平台复制静默漏文件）；fs_id 用
        # .get 防裸 KeyError——那会变成 500 而不是人话报错。
        # **文件与目录都接受**：百度 transfer 对目录 fs_id 做服务端整树复制，
        # 且 _locate_saved_files/locate 均按名匹配目录名（2026-09-20 实锤：源
        # 分享根目录只有文件夹时，旧 isdir 过滤直接把合法任务判死「目录需展开」）。
        pairs: list[tuple[str, str]] = []
        for f in files:
            fid = f.get("fs_id")
            nm = (f.get("server_filename") or f.get("file_name")
                  or f.get("filename") or "").strip()
            if not fid or not nm:
                continue
            pairs.append((fid, nm))       # fid 保持原值（int/str 均可，序列化无差）
        fsids = [p[0] for p in pairs]
        names = [p[1] for p in pairs]
        if not fsids:
            raise AdapterError("分享内没有可转存的文件（列表为空或条目缺少标识）")

        # 4. 服务器端转存到「转存」目录（目录不存在时自动创建；
        #    sekey 用未编码值，httpx 自动编码——双重编码会 errno=2）
        progress(50, "转存到自己的网盘")
        if not (self.settings.transfer_dir or "").strip():
            # 空值时 save_dir 会变成 "/"：文件全进根目录，且定位在整个根目录里
            # 按名匹配（可能匹配到别处的同名旧文件）——必须当场拒绝。
            raise AdapterError(
                "TRANSFER_DIR 未配置：百度转存目标目录不能为空（请检查 .env）")
        save_dir = "/" + self.settings.transfer_dir
        await self._ensure_work_dir(c, save_dir)
        r = await c.post(
            f"{BASE}/share/transfer",
            params={"shareid": shareid, "from": files[0].get("from", uk), "sekey": sekey,
                    "ondup": "newcopy", "async": "1", "appid": APP_ID, "clienttype": "web"},
            data={"path": save_dir, "filelist": _json_list(fsids)},
            headers=headers,
        )
        data = self._json(r)
        if data.get("errno") != 0:
            show = data.get("show_msg", "")
            # 「文件已存在」→ 分享者本人转存自己的文件（文件本来就在盘里），
            # 降级为定位盘内文件，直接交给后续跨平台复制环节
            if show and "文件已存在" in show:
                progress(55, "文件已在网盘中，定位源文件")
                return await self._locate_own_files(c, files, names, headers)
            if data.get("errno") == 2:
                # transfer 阶段的 errno=2「不是分享内的文件」实测（2026-09-20 对照
                # 实验：同一账号同一参数对正常分享返回「文件已存在」）多为**内容被
                # 审核/和谐**——md5 被打乱（含非 hex 字符）的文件 list 可见但转存被拒；
                # 与「链接失效」是两回事，文案必须如实，别让用户去怀疑链接。
                # 和谐特征可直接从 list 的 md5 判定：正常为 32 位 hex，被打乱则含
                # 非 hex 字符（如 "bc3df6344q156ae9..."）——命中就点名道姓，别让用户白等。
                md5s = [str(f.get("md5") or "") for f in files]
                censored = any(m and re.fullmatch(r"[0-9a-f]{32}", m) is None
                               for m in md5s)
                if censored:
                    raise AdapterError(
                        "文件已被百度和谐（md5 特征异常：list 可见但禁止转存），"
                        "该资源在百度盘内已失效，请更换资源源或用其他网盘的分享")
                # 根目录全是文件夹时没有 md5 可查（文件夹无 md5 字段），且文件夹
                # 内容无法通过接口列举（2026-09-21 实测：子目录列举四种参数形态
                # 全被拒 errno=2）。此时 errno=2 无法区分「文件数超单次转存上限」
                # 与「内容被和谐」——网页手动转存会给出确切提示（实测同分享网页
                # 弹「转存文件数已超限，开通SVIP可单次转存50000文件」，API 却只
                # 给 errno=2「不是分享内的文件」）。如实给两条线索，不武断。
                if any(str(f.get("isdir")) == "1" for f in files):
                    raise AdapterError(
                        "百度拒绝整树转存该文件夹（errno=2，百度提示："
                        + (show or "未知") + "）。文件夹内容无法通过接口列举，"
                        "常见原因：①文件夹内文件数超过当前账户的单次转存上限——"
                        "用百度网盘网页手动转存一次可以看到确切提示（如「转存文件数"
                        "已超限，开通SVIP可单次转存50000文件」），可请分享者把文件夹"
                        "拆成多个小分享，或升级 SVIP；②内容被和谐。")
                raise AdapterError(
                    "百度拒绝转存该分享的内容（百度提示：" + (show or "未知") + "）。"
                    "分享链接本身可能仍是有效的——这通常意味着文件正被百度审核或已"
                    "失效（和谐），也可能是分享者刚修改了分享内容；可稍后重试")
            raise self._err(data.get("errno", -1), show)

        # 转存成功 → 按名定位「转存」目录里刚落盘的副本（file-level refs）：
        # 若返回目录级 ref，share() 会把整个「转存」文件夹分享出去（历史文件泄漏）；
        # 且 ondup=newcopy 遇同名会自动重命名，需按 server_mtime 匹配实际副本
        progress(65, "校验转存产物")
        ref = await self._locate_saved_files(c, names, save_dir, headers)
        progress(70, "转存完成")
        return ref

    async def _ensure_work_dir(self, c, path: str) -> None:
        """确保「转存」目录存在（已存在时创建请求的 errno 被忽略）。"""
        r = await c.post(
            f"{BASE}/api/create",
            params={"method": "create", "app_id": APP_ID, "web": "1",
                    "channel": "chunlei", "clienttype": "web"},
            data={"path": path, "isdir": "1", "rtype": "1"},
            headers=self._headers(),
        )
        try:
            d = r.json()
        except ValueError:
            return
        # errno 12/-8 = 已存在，视为成功
        if d.get("errno") not in (0, 12, -8):
            raise AdapterError(f"创建「{path}」目录失败（errno={d.get('errno')}）")

    async def _list_all(self, c, directory: str, headers: dict,
                        page_size: int = 100, max_pages: int = 50) -> list[dict]:
        """分页拉取目录下全部条目。

        旧实现写死 num=100 只查第一页：转存目录堆积 >100 文件时按名定位会漏名
        （误报「未找到」）。这里循环翻页直到取完或触顶。
        """
        items: list[dict] = []
        page = 1
        while page <= max_pages:
            r = await c.get(
                f"{BASE}/api/list",
                params={"dir": directory, "web": "1", "app_id": APP_ID,
                        "clienttype": "web", "page": str(page), "num": str(page_size)},
                headers=headers,
            )
            data = self._json(r)
            if data.get("errno") != 0:
                if page == 1:
                    raise self._err(data.get("errno", -1))
                break  # 后续页出错：返回已拿到的部分，不整单失败
            batch = data.get("list") or []
            items.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
        return items

    async def _locate_saved_files(self, c, names: list[str], save_dir: str,
                                  headers: dict) -> SavedRef:
        """转存完成后在 save_dir 按名定位刚落盘的副本（file-level refs）。

        - ondup=newcopy 遇同名自动重命名（"abc(1).txt"）→ 兜底匹配
          「去 (n) 后缀」的同名文件；多候选取 server_mtime 最新的（刚转存那份）；
        - 实际文件名（含重命名）回写 SavedRef.names，供 OpenList 复制使用；
        - **重试**：share/transfer 用 async=1 提交，响应 errno=0 只代表「已受理」，
          服务端落稳需要一点时间 → 定位缺名时退避重查（最多 3 轮），消除间歇性
          「转存完成但未找到」假失败。
        """
        last_err: AdapterError | None = None
        for attempt in range(3):
            # 文件与目录都接受（源分享整体含文件夹时，转存产物就是同名目录；
            # 2026-09-20 曾在此过滤目录导致「转存成功却永远找不到」）
            items = await self._list_all(c, save_dir, headers)
            actual_names: list[str] = []
            refs: list[dict] = []
            missing: list[str] = []
            for n in names:
                cands = [it for it in items
                         if (it.get("server_filename") == n
                             or _strip_rename_suffix(it.get("server_filename") or "") == n)]
                if not cands:
                    missing.append(n)
                    continue
                best = max(cands, key=lambda it: it.get("server_mtime") or 0)
                actual_names.append(best.get("server_filename") or n)
                refs.append({"fs_id": best["fs_id"], "path": best["path"]})
            if not missing:
                return SavedRef(platform=self.platform, names=actual_names,
                                mount_dir=save_dir, refs=refs)
            last_err = AdapterError(
                f"转存完成但在「{save_dir}」未找到「{missing[0]}」，请到网盘确认后重试")
            await asyncio.sleep(1.5 * (attempt + 1))
        raise last_err or AdapterError("转存完成但定位转存产物失败")

    async def _locate_own_files(self, c, files: list, names: list[str],
                                headers: dict) -> SavedRef:
        """「文件已存在」降级：定位自己盘内的源文件，交给跨平台复制环节。

        仅对「分享者=本人」有效（share/list 的 path 即盘内真实路径）；
        校验文件确实存在，否则给出人话错误。
        """
        paths = [f.get("path") for f in files if f.get("path")]
        if not paths:
            raise AdapterError(
                "文件已存在于你的网盘中，但分享未返回文件位置，无法跨平台转存")
        dirs = {(p.rsplit("/", 1)[0] or "/") for p in paths}
        if len(dirs) > 1:
            raise AdapterError(
                "分享内的文件位于多个目录，跨平台转存暂不支持，请分别转存")
        mount_dir = dirs.pop()
        # 校验这些文件确实存在于该目录（防止他人分享恰好同名被误判），
        # 同时抓 fs_id：带 fs_id 的 file-level ref 让 share() 走 fid_list（正确），
        # 否则只用 path → share/set 的 path 仅用于「目录」分享，对文件会失败。
        items = {it.get("server_filename"): it
                 for it in await self._list_all(c, mount_dir, headers)}
        refs = []
        for n in names:
            it = items.get(n)
            if not it:
                raise AdapterError(
                    f"文件已存在于你的网盘中，但在 {mount_dir} 未找到同名文件"
                    f"（{n}），无法跨平台转存")
            refs.append({"fs_id": it["fs_id"], "path": it["path"]})
        return SavedRef(platform=self.platform, names=names,
                        mount_dir=mount_dir, refs=refs, self_share=True)

    # ---- share：已保存文件 → 新分享 ----

    async def share(self, ref: SavedRef, progress: ProgressFn) -> TransferResult:
        if not self.configured():
            raise AdapterNotConfigured("百度网盘 Cookie 未配置（需要 BDUSS）")
        c = self.client()
        fs_ids = [rr.get("fs_id") for rr in ref.refs if rr.get("fs_id")]
        dir_paths = [rr.get("path") for rr in ref.refs
                     if rr.get("path") and not rr.get("fs_id")]
        if not fs_ids and not dir_paths:
            raise AdapterError("百度分享失败：没有可分享的已保存内容")
        progress(85, "生成新分享链接")
        new_pwd = _rand_pwd()
        # 私密分享（schannel=0，必须带 pwd）+ 提取码。
        # 参数名实测（2026-09-17）：文件与**目录**都用 `fid_list`；目录若走路径分支，
        # 参数名必须是 `path_list`（写成 `path` 会 errno=2「path_list or fid_list
        # param need」）；且 schannel=0 缺 pwd 会 errno=115「账号异常，禁止分享」。
        payload = {"period": str(self.settings.baidu_share_days), "pwd": new_pwd,
                   "schannel": "0", "channel_list": "[]"}
        if fs_ids:
            payload["fid_list"] = _json_list(fs_ids)
        else:
            payload["path_list"] = _json_list(dir_paths)
        r = await c.post(
            f"{BASE}/share/set",
            params={"clienttype": "0", "app_id": APP_ID, "web": "1", "channel": "chunlei"},
            data=payload,
            headers=self._headers(),
        )
        data = self._json(r)
        if data.get("errno") != 0:
            # 转存可能已成功、分享失败：如实反馈
            raise self._err(data.get("errno", -1))
        link = data.get("link") or ""
        if not link and data.get("shorturl"):
            link = BASE + data["shorturl"]
        if not link:
            # errno=0 但两种链接字段都缺：直接返回会产出「https://pan.baidu.com?pwd=xxxx」
            # 这种看似正常实则无效的垃圾链接——宁可如实报错。
            raise AdapterError("百度分享创建成功但未返回链接，请稍后到网盘查看或重试")
        # 提取码拼进链接（?pwd=xxx，与百度 App 分享文案一致）：
        # 产物链接粘贴回来即可直接再转存，无需手动补提取码
        if new_pwd and "?" not in link:
            link = f"{link}?pwd={new_pwd}"
        progress(100, "完成")
        return TransferResult(
            new_url=link, password=new_pwd,
            files_saved=len(fs_ids) if fs_ids else len(dir_paths),
            message="百度转存完成",
            details={"saved_path": dir_paths[0] if dir_paths else ""})

    # ---- locate：跨平台复制完成后按名定位（目标盘侧） ----

    async def locate(self, names: list[str], mount_dir: str = "",
                     progress: ProgressFn = _noop_progress) -> SavedRef:
        """mount_dir 语义：目录名（如 settings.transfer_dir）或绝对路径。"""
        if not self.configured():
            raise AdapterNotConfigured("百度网盘 Cookie 未配置（需要 BDUSS）")
        c = self.client()
        directory = (mount_dir if mount_dir.startswith("/")
                     else ("/" + mount_dir if mount_dir else "/"))
        progress(75, "定位转存文件")
        # 按名定位，**文件与目录都接受**：夸克等源盘的分享可以整体包含文件夹，
        # OpenList 会把整个目录复制到目标「转存」目录——此时 names 里就是目录名，
        # 且百度对目录同样支持 fid_list 分享。2026-09-19 曾在此过滤目录，导致
        # 「源分享含文件夹」的合法转存必失败（目录被滤掉→永远"缺失"），已回退。
        items_by_name: dict[str, dict] = {}
        for it in await self._list_all(c, directory, self._headers()):
            nm = it.get("server_filename") or it.get("file_name")
            if nm and nm not in items_by_name:
                items_by_name[nm] = it
        refs: list[dict] = []
        missing: list[str] = []
        for n in names:
            it = items_by_name.get(n)
            if it is None:
                # ondup=newcopy 的自动重命名兜底："abc(1).txt" ↔ 请求的 "abc.txt"
                for cand_name, cand in items_by_name.items():
                    if _strip_rename_suffix(cand_name) == n:
                        it = cand
                        break
            if it is None:
                missing.append(n)
                continue
            refs.append({"fs_id": it["fs_id"], "path": it["path"]})
        if missing:
            # 严格缺失校验：部分命中就放行会让下游生成缺文件的分享（比失败更糟）
            raise AdapterError(
                f"百度网盘内未找到转存后的文件（缺 {len(missing)} 个，如「{missing[0]}」；"
                f"检查 OpenList 百度挂载与复制目录）")
        return SavedRef(platform=self.platform, names=names,
                        mount_dir=directory, refs=refs)




def _rand_pwd() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=4))


def _strip_rename_suffix(filename: str) -> str:
    """"abc(1).txt" → "abc.txt"（百度 ondup=newcopy 的自动重命名格式）。"""
    m = re.fullmatch(r"(.*?)\(\d+\)((?:\.[^.]*)?)", filename)
    if m and m.group(1):
        return m.group(1) + m.group(2)
    return filename


def _json_list(items: list[Any]) -> str:
    import json
    return json.dumps(items, ensure_ascii=False, separators=(",", ":"))
