"""本机高速下载适配器（伪网盘：仅可作为转存目标，不可作为源）。

定位：把「本机磁盘」当第五个目标平台接入现有跨平台中转机制——
  源盘 save → OpenList fs/copy 到 /local/转存（文件流式落盘本机）
  → locate（文件已在磁盘，直接校验）→ share（生成 /dl/<任务>/ 下载链接）。

下载链接形态：
  - 单文件  /dl/<task_id>/<文件名>
  - 多文件  /dl/<task_id>/（任务文件清单页，内含逐文件下载链接）

链接有效期与任务共存：/dl 端点实时校验任务存在且成功，任务删除即失效。
文件名只允许映射到本机「转存」目录内的实际文件（无路径穿越面）。
"""
from __future__ import annotations

import os
from pathlib import Path

from adapters.base import (Adapter, AdapterError, AdapterNotConfigured,
                           CapabilityError, ProgressFn, SavedRef, TransferResult,
                           _noop_progress)
from parser import ShareLink


class LocalAdapter(Adapter):
    platform = "local"
    label = "本机高速"

    def configured(self) -> bool:
        """是否已配置。

        ⚠️ **不能**写成 `bool(self._base())`：`Path("")` 就是 `Path('.')`，恒为真，
        会把「LOCAL_DIR 未配置」误判成「已就绪」，等真跑起来才炸。必须判原始字符串。
        """
        return bool(str(getattr(self.settings, "local_dir", "") or "").strip())

    def _base(self) -> Path:
        """本机缓存根目录（容器内 /data/downloads；测试注入 tmp_path）。"""
        return Path(getattr(self.settings, "local_dir", "") or "")

    def _work_dir(self, mount_dir: str = "") -> Path:
        """mount_dir 语义：目录名（如 settings.transfer_dir）或相对/绝对路径。"""
        base = self._base()
        sub = (mount_dir or "").strip("/")
        return (base / sub) if sub else base

    # ---- save：本机不提供分享保存（永不做源平台） ----

    async def save(self, share: ShareLink, password: str | None,
                   progress: ProgressFn) -> SavedRef:
        raise CapabilityError("本机高速只支持作为下载目标，不支持解析本机分享链接")

    # ---- locate：OpenList 复制完成后校验文件确实落盘 ----

    async def locate(self, names: list[str], mount_dir: str = "",
                     progress: ProgressFn = _noop_progress) -> SavedRef:
        """定位本机落盘产物。names 里可能是**目录名**（夸克分享整体含文件夹时，
        OpenList 会把整个目录复制进来）→ 递归展开收集目录下全部文件。

        refs/结果里的 name 一律是**相对 work 的 posix 相对路径**（含子目录），
        供 /dl 端点按相对路径提供下载。
        """
        if not self.configured():
            raise AdapterNotConfigured("本机高速未配置落盘根目录（LOCAL_DIR）")
        progress(75, "校验本机文件")
        work = self._work_dir(mount_dir)
        if not work.is_dir():
            raise AdapterError(f"本机缓存目录不存在：{work}")
        work_res = os.path.normcase(str(work.resolve()))
        refs, missing = [], []
        for n in names:
            p = work / n
            # 路径包含校验：n 来自外部分享文件名（不可信输入）——"../x" 可逃出
            # work 目录，Windows 下 "C:/abs/x" 拼接会整体替换。解析后必须仍落在
            # work 之内；share() 还有第二道防线，这里从源头掐掉。
            try:
                inside = os.path.normcase(str(p.resolve())).startswith(work_res + os.sep)
            except (OSError, ValueError):
                inside = False
            if not inside:
                missing.append(n)
                continue
            if p.is_file():
                refs.append({"path": str(p), "size": p.stat().st_size,
                             "name": Path(n).as_posix()})
                continue
            if p.is_dir():
                # 目录 → 递归展开为其中的文件（相对路径），目录本身不可下载
                found = 0
                for root, _dirs, fs in os.walk(p):
                    for f in fs:
                        fp = Path(root) / f
                        rel = fp.relative_to(work).as_posix()
                        try:
                            refs.append({"path": str(fp), "size": fp.stat().st_size,
                                         "name": rel})
                            found += 1
                        except OSError:
                            pass
                if found == 0:
                    missing.append(n)
                continue
            missing.append(n)
        if not refs:
            raise AdapterError(
                "本机缓存内未找到转存后的文件（检查 OpenList 本机挂载与复制目录）")
        if missing:
            raise AdapterError(f"本机缓存缺少 {len(missing)} 个文件（{missing[0]}…），转存不完整")
        return SavedRef(platform=self.platform, names=names,
                        mount_dir=mount_dir, refs=refs)

    # ---- share：生成 /dl 下载链接（相对路径，前端按当前域名补全） ----

    async def share(self, ref: SavedRef, progress: ProgressFn) -> TransferResult:
        if not self.configured():
            # 不查的话 _base() 返回 Path(".")，会把进程当前工作目录当成 base 做比较
            raise AdapterNotConfigured("本机高速未配置落盘根目录（LOCAL_DIR）")
        # 优先用 dl_token（分享下载链接不泄露 task_id）；旧任务无 token 时回退 task_id
        token = ref.dl_token or ref.task_id
        if not token:
            raise AdapterError("本机下载链接生成失败：缺少任务标识")
        base = self._base()
        base_res = os.path.normcase(str(base.resolve()))
        files: list[tuple[Path, str]] = []   # (绝对路径, 相对 base 的 posix 相对路径)
        for rr in ref.refs:
            p = Path(rr.get("path", ""))
            rel = rr.get("name") or p.name
            # resolve + normcase 比较：Windows 大小写不敏感、local_dir 配成相对路径时
            # 也不会把合法文件误判成「不可下载」（旧写法 base in p.parents 有此坑）
            try:
                inside = (p.is_file()
                          and os.path.normcase(str(p.resolve())).startswith(base_res + os.sep))
            except (OSError, ValueError):
                inside = False
            if inside:
                files.append((p, Path(rel).as_posix()))
        if not files:
            raise AdapterError("本机下载链接生成失败：没有可下载的本机文件")
        if len(files) == 1:
            # 单文件短链：/dl/<token> —— URL 不再塞一长串 %XX 转义文件名，
            # 下载时的真实文件名由 Content-Disposition 提供（服务端配合）
            url = f"/dl/{token}"
        else:
            url = f"/dl/{token}/"
        progress(100, "完成")
        return TransferResult(new_url=url, password=None, files_saved=len(files),
                              message="本机高速下载链接已生成",
                              files=[rel for _, rel in files])
