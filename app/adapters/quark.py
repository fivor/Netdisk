"""夸克网盘适配器（Cookie 方案，参考 QuarkAutoSave / QuarkPanTool 公开实现）。

save 流程：token(stoken) → sharepage/detail 文件列表 → sharepage/save 转存
           → task 轮询，落「转存」目录
share 流程：share 创建新分享（file_id_list → share_id → 链接）
locate 流程：file/sort 按名定位已保存文件（跨平台目标盘侧）

UC 网盘与夸克同属阿里系、开放接口同构（仅域名/Cookie/品牌不同），
由 adapters/uc.py 继承本类实现，仅覆写类属性与凭据读取。
"""
from __future__ import annotations

import asyncio
import re

from adapters.base import (Adapter, AdapterError, AdapterNotConfigured,
                           ProgressFn, SavedRef, TransferResult, _noop_progress)
from parser import ShareLink

# 「自己的分享」盘内定位的**兜底** BFS 扫描上限（条目数）。
# 盘内条目极多时（实测 5 千条量级）从根 BFS 会扫不完 → 首选 `_dir_path` 自底向上定位。
SELF_SHARE_SCAN_LIMIT = 20000


class QuarkAdapter(Adapter):
    platform = "quark"
    label = "夸克网盘"
    # 类属性：UC 适配器覆写域名与品牌参数即可复用全部实现
    BASE = "https://drive-pc.quark.cn/1/clouddrive"
    PR = {"pr": "ucpro", "fr": "pc"}
    WEB_BASE = "https://pan.quark.cn"       # 分享链接域名
    COOKIE_KEY = "quark_cookie"

    def __init__(self, settings, client=None) -> None:
        super().__init__(settings, client=client)
        # 实例级拷贝，避免子类间共享可变 dict
        self.PR = dict(self.PR)

    def _cookie(self) -> str:
        return getattr(self.settings, self.COOKIE_KEY, "")

    def configured(self) -> bool:
        raw = self._cookie()
        jar = self.parse_cookie(raw) if raw else {}
        # __pus / __puus 是夸克/UC 会话核心 Cookie
        return "__pus" in jar or "__puus" in jar

    def _headers(self) -> dict[str, str]:
        return {
            "Referer": f"{self.WEB_BASE}/",
            "Content-Type": "application/json",
            "Cookie": self._cookie(),
        }

    # 业务错误码 → 人话（41017 实测确认；其余为常见码）
    _ERRNO_MSG = {
        41017: "不允许转存自己（或本站）生成的分享，请使用他人分享的链接",
        41008: "分享链接不存在或已失效",
        41013: "分享内容已被封禁，无法转存",
        32011: "账号未完成实名认证——请先到网盘 App/官网完成实名认证后再试",
    }

    async def _check(self, resp) -> dict:
        try:
            data = resp.json()
        except Exception:
            raise AdapterError(f"{self.label}接口返回非 JSON（HTTP {resp.status_code}）")
        if not isinstance(data, dict):
            # 合法 JSON 但非对象（数组/字符串）时，data.get 会裸抛 AttributeError
            raise AdapterError(f"{self.label}接口返回异常（HTTP {resp.status_code}，非 JSON 对象）")
        if resp.status_code != 200 or data.get("code") not in (0, "0"):
            code = data.get("code")
            hint = self._ERRNO_MSG.get(code)
            if hint:
                raise AdapterError(
                    f"{self.label}接口错误：{hint}（code={code} {data.get('message', '')}）")
            raise AdapterError(f"{self.label}接口错误 code={code} msg={data.get('message', '')}")
        return data.get("data") or {}

    async def _wait_task(self, c, task_id: str, progress: ProgressFn, base_pct: int) -> dict:
        """轮询转存任务。

        进度上界必须压在下游「转存完成」的百分比之下（base_pct+25），否则
        长时间轮询会把进度顶到高位，回落时前端进度条会明显跳水。
        """
        running_cap = min(base_pct + 20, 92)   # 轮询中的上界（45 → 65）
        done_pct = min(base_pct + 25, 95)      # 完成（45 → 70，与下游「转存完成」齐平）
        misses = 0                             # 连续瞬时失败计数（网络抖动/瞬时 5xx 容错）
        for i in range(60):
            await asyncio.sleep(1.0)
            try:
                r = await c.get(f"{self.BASE}/task", params={**self.PR, "task_id": task_id},
                                headers=self._headers())
                data = await self._check(r)
                misses = 0
            except AdapterError:
                # 单次查询失败不该终止整个转存（服务端任务可能已在跑甚至已完成）；
                # 连续 5 次失败才放弃——对齐百度页面抓取的退避重试风格。
                misses += 1
                if misses >= 5:
                    raise
                continue
            status = (data.get("status") or 0)
            if status == 2:  # finished
                progress(min(done_pct, 95), "任务完成")
                return data
            if status == 3:  # failed
                err = (data.get("err") or {}).get("msg") or "任务失败"
                raise AdapterError(f"{self.label}任务失败: {err}")
            progress(min(running_cap, base_pct + i), "处理中")
        raise AdapterError(f"{self.label}任务超时（60 秒）")

    # ---- save：他人分享 → 自己网盘（「转存」目录） ----
    # 接口契约对齐 quark-auto-save（实战验证版）：
    #   stoken:  POST /share/sharepage/token {pwd_id, passcode}
    #   列表:    GET  /share/sharepage/detail?pwd_id&passcode&stoken&pdir_fid
    #   转存:    POST /share/sharepage/save {fid_list, fid_token_list,
    #            to_pdir_fid, pwd_id, stoken, pdir_fid, scene:"link"}

    async def save(self, share: ShareLink, password: str | None,
                   progress: ProgressFn) -> SavedRef:
        if not self.configured():
            raise AdapterNotConfigured(f"{self.label} Cookie 未配置（需要 __pus/__puus）")
        c = self.client()
        pwd = (password or "").strip() or ""

        # 1. stoken
        progress(5, "获取分享凭证")
        r = await c.post(f"{self.BASE}/share/sharepage/token", params=self.PR,
                         json={"pwd_id": share.share_id, "passcode": pwd},
                         headers=self._headers())
        data = await self._check(r)
        stoken = data.get("stoken") or ""
        if not stoken:
            raise AdapterError(f"{self.label} stoken 获取失败（检查分享链接/提取码）")

        # 2. 分享文件列表（**翻页**：写死 _size=50 只取第一页，大分享会被静默截断
        #    成前 50 个且无任何提示——只转一半比失败更糟）
        progress(20, "获取分享文件列表")
        filelist: list = []
        is_owner = False
        for page in range(1, 21):              # 20 页 × 50 条 = 1000 项封顶
            r = await c.get(f"{self.BASE}/share/sharepage/detail",
                            params={**self.PR, "pwd_id": share.share_id, "passcode": pwd,
                                    "stoken": stoken, "pdir_fid": "0", "_page": page,
                                    "_size": 50, "_fetch_banner": 1, "_fetch_share": 1,
                                    "_fetch_total": 1,
                                    "_sort": "file_type:asc,updated_at:desc"},
                            headers=self._headers())
            data = await self._check(r)
            if page == 1:
                is_owner = data.get("is_owner") is True
            batch = data.get("list") or []
            filelist.extend(batch)
            if len(batch) < 50:
                break
        # fid 与 fid_token 按**单文件配对**提取：旧写法两个独立列表推导式各自过滤，
        # 个别文件缺 token 时两列表错位 → len 不等整单失败，还报出「分享内没有
        # 可转存的文件」这种与真实原因完全不符的文案。
        fid_list: list[str] = []
        fid_token_list: list[str] = []
        names: list[str] = []
        for f in filelist:
            fid = f.get("fid")
            token = f.get("share_fid_token")
            if not fid or not token:
                continue                       # 缺 token 的单个文件跳过，不连坐整单
            fid_list.append(str(fid))
            fid_token_list.append(str(token))
            nm = (f.get("file_name") or "").strip()
            if nm:
                names.append(nm)
        if not fid_list:
            raise AdapterError("分享内没有可转存的文件（目录需展开，MVP 暂不支持整树）")

        # 2.5 自己的分享（detail 直接给出 is_owner）→ 不必再试转存（必然 41017），
        #     直接走盘内定位。省一次请求，也避免无谓的错误噪音。
        if is_owner:
            progress(35, "检测到自己的分享，定位盘内文件")
            return await self._self_share_ref(c, filelist, fid_list, names, progress)

        # 3. 转存到「转存」目录（自己的分享会报 41017 → 降级为「文件已在盘内」）
        progress(40, "转存到自己的网盘")
        work_fid = await self._ensure_work_dir(c)
        r = await c.post(f"{self.BASE}/share/sharepage/save",
                         params={**self.PR, "app": "clouddrive"},
                         json={"fid_list": fid_list, "fid_token_list": fid_token_list,
                               "to_pdir_fid": work_fid, "pwd_id": share.share_id,
                               "stoken": stoken, "pdir_fid": "0", "scene": "link"},
                         headers=self._headers())
        try:
            body = r.json()
        except Exception:
            body = {}
        if body.get("code") == 41017:
            # 注意：此处百分比必须 >= 上面的 40，否则前端进度条会回退
            progress(40, "检测到自己的分享，定位盘内文件")
            return await self._self_share_ref(c, filelist, fid_list, names, progress)
        data = await self._check(r)
        task_id = data.get("task_id")
        saved_fids: list[str] = list(data.get("save_as_top_fids") or [])
        if task_id:
            task_data = await self._wait_task(c, task_id, progress, 45)
            saved_fids = task_data.get("save_as_top_fids") or saved_fids
        if not saved_fids:
            # 兜底：按名在「转存」目录定位（locate 失败则显式报错）
            try:
                ref = await self.locate(names, self.settings.transfer_dir, progress)
                saved_fids = [rr["fid"] for rr in ref.refs]
            except AdapterError:
                raise AdapterError(
                    f"{self.label}转存完成但未返回转存文件列表，无法生成分享（可稍后重试）")

        progress(70, "转存完成")
        return SavedRef(platform=self.platform, names=names,
                        mount_dir="/" + self.settings.transfer_dir,
                        refs=[{"fid": f} for f in saved_fids])

    async def _ensure_work_dir(self, c) -> str:
        """确保「转存」目录存在，返回其 fid（不存在则创建）。

        翻页遍历根目录：根下目录超 100 个时单页查找会漏掉已存在的「转存」，
        进而重复创建出「转存(1)」这类分身目录。
        """
        for page in range(1, 21):
            r = await c.get(f"{self.BASE}/file/sort",
                            params={**self.PR, "pdir_fid": "0", "_page": page,
                                    "_size": 100, "_fetch_total": 1,
                                    "_sort": "file_type:asc,updated_at:desc"},
                            headers=self._headers())
            data = await self._check(r)
            batch = data.get("list") or []
            for it in batch:
                if it.get("dir") and it.get("file_name") == self.settings.transfer_dir:
                    return it["fid"]
            if len(batch) < 100:
                break
        r = await c.post(f"{self.BASE}/file", params=self.PR,
                         json={"pdir_fid": "0", "file_name": self.settings.transfer_dir,
                               "dir_path": "", "dir_init_lock": False},
                         headers=self._headers())
        data = await self._check(r)
        fid = data.get("fid") or ""
        if not fid:
            raise AdapterError(
                f"{self.label}「{self.settings.transfer_dir}」目录创建失败")
        return fid

    async def _self_share_ref(self, c, filelist: list, fid_list: list[str],
                              names: list[str], progress: ProgressFn) -> SavedRef:
        """自己的分享：文件已在盘内，定位其所在目录，返回 SavedRef（含盘内真实 fid）。

        两条定位路径（2026-09-18 重写）：
        ① **自底向上（首选）**：分享详情里已带 `pdir_fid`，用
           `GET /file/info?fid=` 逐级上溯拼出目录路径——深度只有几层，约 3~5 次请求；
        ② **兜底 BFS**：从根目录逐层 `file/sort` 搜索 fid。盘内条目很多时会撞扫描上限
           （实测 5 千条量级就误报「盘内未定位到…可能已被删除」），故只作兜底，
           且上限放宽、耗尽时给出**真实原因**而不是「文件可能已被删除」。
        """
        fids = [str(f) for f in fid_list if f]
        if not fids:
            raise AdapterError("分享内没有可转存的文件（目录需展开，MVP 暂不支持整树）")

        # ① 自底向上：用 pdir_fid 拼出所在目录
        dir_of: dict[str, str] = {}
        pdir_cache: dict[str, str | None] = {}   # 同目录多文件复用上溯结果（50 个文件
        for f in filelist:                       # 同目录时不加缓存要打上百次重复请求）
            fid = str(f.get("fid") or "")
            pdir = str(f.get("pdir_fid") or "")
            if not fid or not pdir:
                dir_of = {}
                break
            if pdir in pdir_cache:
                path = pdir_cache[pdir]
            else:
                path = await self._dir_path(c, pdir)
                pdir_cache[pdir] = path
            if path is None:
                dir_of = {}
                break
            dir_of[fid] = path
        if len(dir_of) == len(fids):
            dirs = set(dir_of.values())
            if len(dirs) > 1:
                raise AdapterError(
                    "分享内的文件位于多个目录，跨平台转存暂不支持，请分别转存")
            progress(45, "已定位盘内文件所在目录")
            # self_share **两条路径都必须带**：本函数只在「自己的分享」场景被调用，
            # 主路径漏标记会让编排器「本人链接+同源目标」的拒绝语义静默失效。
            return SavedRef(platform=self.platform, names=names,
                            mount_dir=dirs.pop(), refs=[{"fid": f} for f in fids],
                            self_share=True)

        # ② 兜底：从根 BFS 搜 fid
        want = set(fids)
        found: dict[str, str] = {}
        queue: list[tuple[str, str]] = [("0", "")]
        seen: set[str] = set()
        scanned = 0
        while queue and len(found) < len(want) and scanned < SELF_SHARE_SCAN_LIMIT:
            pdir_fid, path = queue.pop(0)
            if pdir_fid in seen:
                continue
            seen.add(pdir_fid)
            page = 1
            while True:
                r = await c.get(f"{self.BASE}/file/sort",
                                params={**self.PR, "pdir_fid": pdir_fid, "_page": page,
                                        "_size": 100, "_fetch_total": 1,
                                        "_sort": "file_type:asc,updated_at:desc"},
                                headers=self._headers())
                data = await self._check(r)
                items = data.get("list") or []
                scanned += len(items)
                for it in items:
                    if it.get("fid") in want:
                        found[it["fid"]] = path
                    if it.get("dir"):
                        queue.append((it["fid"], f"{path}/{it.get('file_name', '')}"))
                if len(items) < 100 or scanned >= SELF_SHARE_SCAN_LIMIT:
                    break
                page += 1
            progress(min(45, 30 + scanned // 50), "定位盘内文件")
        if len(found) < len(want):
            missing = want - set(found)
            if queue and scanned >= SELF_SHARE_SCAN_LIMIT:
                raise AdapterError(
                    f"自己的分享：盘内条目过多（已扫描 {scanned} 条仍未找齐 "
                    f"{len(missing)} 个文件），无法完成定位——"
                    f"可把该文件移到更浅的目录后重试")
            raise AdapterError(
                f"自己的分享：盘内未定位到 {len(missing)} 个文件（可能已被删除），无法跨平台转存")
        dirs = set(found.values())
        if len(dirs) > 1:
            raise AdapterError(
                "分享内的文件位于多个目录，跨平台转存暂不支持，请分别转存")
        return SavedRef(platform=self.platform, names=names,
                        mount_dir=dirs.pop(), refs=[{"fid": f} for f in fid_list],
                        self_share=True)

    async def _dir_path(self, c, fid: str, max_depth: int = 16) -> str | None:
        """用 `GET /file/info?fid=` 沿 pdir_fid 自底向上拼出目录路径。

        返回 ""（根目录）或 "/a/b"；任一级查询失败/异常返回 None（调用方走兜底 BFS）。
        """
        parts: list[str] = []
        cur = str(fid or "")
        for _ in range(max_depth):
            if not cur or cur == "0":
                return "/" + "/".join(reversed(parts)) if parts else ""
            try:
                r = await c.get(f"{self.BASE}/file/info",
                                params={**self.PR, "fid": cur},
                                headers=self._headers())
                d = await self._check(r)
            except Exception:
                return None
            name = d.get("file_name") or ""
            if name:
                parts.append(str(name))
            nxt = str(d.get("pdir_fid") or "0")
            if nxt == cur:
                return None
            cur = nxt
        return None

    # ---- share：已保存文件 → 新分享 ----
    # 契约对齐 QuarkPanTool：三步——POST /share（fid_list 为纯字符串数组）
    # → 轮询任务拿 share_id → POST /share/password 换最终链接。

    async def share(self, ref: SavedRef, progress: ProgressFn) -> TransferResult:
        if not self.configured():
            raise AdapterNotConfigured(f"{self.label} Cookie 未配置（需要 __pus/__puus）")
        c = self.client()
        fids = [rr.get("fid") for rr in ref.refs if rr.get("fid")]
        if not fids:
            raise AdapterError(f"{self.label}分享失败：没有可分享的文件 fid")

        # 1. 创建分享任务（url_type 1=公开；expired_type 1=永久）
        progress(85, "创建分享任务")
        r = await c.post(f"{self.BASE}/share", params=self.PR,
                         json={"fid_list": fids, "title": "", "url_type": 1,
                               "expired_type": 1, "passcode": ""},
                         headers=self._headers())
        data = await self._check(r)
        task_id = data.get("task_id")
        share_id = data.get("share_id") or ""
        if task_id:
            task_data = await self._wait_task(c, task_id, progress, 88)
            share_id = task_data.get("share_id") or share_id
        if not share_id:
            raise AdapterError(f"{self.label}新分享创建失败（未返回 share_id）")

        # 2. 定稿换取最终链接
        r = await c.post(f"{self.BASE}/share/password", params=self.PR,
                         json={"share_id": share_id},
                         headers=self._headers())
        data = await self._check(r)
        link = (data.get("share_url") or data.get("url") or
                f"{self.WEB_BASE}/s/{share_id}")
        progress(100, "完成")
        return TransferResult(new_url=link, password=None, files_saved=len(fids),
                              message=f"{self.label}转存完成")

    # ---- locate：跨平台复制完成后按名定位（目标盘侧） ----

    async def locate(self, names: list[str], mount_dir: str = "",
                     progress: ProgressFn = _noop_progress) -> SavedRef:
        """mount_dir 语义：目录名（如 settings.transfer_dir）或 fid；
        传目录名时自动解析为 fid。"""
        if not self.configured():
            raise AdapterNotConfigured(f"{self.label} Cookie 未配置（需要 __pus/__puus）")
        c = self.client()
        progress(75, "定位转存文件")
        pdir_fid = mount_dir or "0"
        if mount_dir and mount_dir != "0" and not re.fullmatch(r"[0-9a-f]{16,}", mount_dir):
            # 目录名 → fid（逐层解析；「转存」在根目录，查一层即可）
            fid = await self._dir_fid(c, mount_dir)
            if not fid:
                raise AdapterError(
                    f"{self.label}内未找到「{mount_dir}」目录（请在网盘中创建，或重试触发自动创建）")
            pdir_fid = fid
        # 目录列表接口为 GET /file/sort（POST /file/list 不存在，实测确认）。
        # **翻页**：转存目录是多任务共享、只增不减的，单页 100 条在堆积后会漏名，
        # 让跨平台转存假报「未找到转存后的文件」——百度侧 _list_all 修过的同款问题。
        items_by_name: dict[str, dict] = {}
        for page in range(1, 21):
            r = await c.get(f"{self.BASE}/file/sort", params={
                **self.PR, "pdir_fid": pdir_fid, "_page": page, "_size": 100,
                "_fetch_total": 1, "_sort": "file_type:asc,updated_at:desc"},
                headers=self._headers())
            data = await self._check(r)
            batch = data.get("list") or []
            for it in batch:
                nm = it.get("file_name")
                if nm and nm not in items_by_name:
                    items_by_name[nm] = it
            if len(batch) < 100:
                break
        refs = [{"fid": items_by_name[n]["fid"]} for n in names if n in items_by_name]
        missing = [n for n in names if n not in items_by_name]
        if missing:
            # 严格缺失校验：部分命中就放行会让下游把缺文件的分享当成功（比失败更糟）
            raise AdapterError(
                f"{self.label}内未找到转存后的文件（缺 {len(missing)} 个，如「{missing[0]}」；"
                f"检查 OpenList 挂载与复制目录）")
        return SavedRef(platform=self.platform, names=names,
                        mount_dir=mount_dir, refs=refs)

    async def _dir_fid(self, c, name: str, parent: str = "0") -> str | None:
        """按名查目录 fid（翻页遍历父目录，「转存」不在第一页也能找到）。"""
        for page in range(1, 21):
            r = await c.get(f"{self.BASE}/file/sort",
                            params={**self.PR, "pdir_fid": parent, "_page": page,
                                    "_size": 100, "_fetch_total": 1,
                                    "_sort": "file_type:asc,updated_at:desc"},
                            headers=self._headers())
            data = await self._check(r)
            batch = data.get("list") or []
            for it in batch:
                if it.get("dir") and it.get("file_name") == name:
                    return it["fid"]
            if len(batch) < 100:
                break
        return None
