"""跨平台中转的计量与完整性校验。

背景（2026-09 事故）：
1) OpenList 对目录的 `fs/copy` 会拆成「每个节点一个任务」，且**目录级任务在子任务
   入队完成时就置为 done**——只等 `fs/copy` 返回的 task id 会得到「假成功」。
2) `fs/copy` 的 src_dir 可能只是「**包含待复制项的父目录**」（夸克自分享降级时只知道
   文件所在目录，不知道具体是哪个子项），所以**不能整个 src_dir 递归统计**，否则会把
   父目录里的无关文件算进总量——实测把 443MB 的分享算成 2.76TB（父目录里躺着用户
   全部游戏），进而在页面上显示出荒谬的总量与 ETA。
3) 复制任务名里的路径是**挂载内相对路径**（`copy [/quark](/微信分享/…) to
   [/baidu](/转存/…)`），而我们的 src_dir/dst_dir 带挂载前缀（`/quark/…`）。路径比较
   必须两种写法都容忍，否则完成判定永远认不出自己的任务（复制早已完成却卡在 0%）。

本模块提供（全部只读、无副作用）：
- `scan_source()`          枚举**待复制项**（names）→ {相对路径: 大小} + 总字节 + 最大单文件
- `poll()`                 归集「已落地」，并分别估算**下载量 / 上传量**（两路独立计量）
- `verify_destination()`   递归枚举目标目录，核对相对路径与大小是否齐全一致

两路计量的意义（2026-09-18 补）：旧实现把「本机 temp 字节」也算进「已传」，
于是下载一结束就显示假 100%、上传期间进度与速率全部冻结。现在：
- **下载量** = 已落地 + 本机暂存（temp）
- **上传量** = 已落地 + Σ(在途文件大小 × OpenList 任务 progress%)
配合前端两行显示，可分别看到下载与上传的实时进度/速率/剩余时间。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from openlist import (_dir_bytes, _same_or_under, _strip_first, _strip_mount,
                      _zh_error, parse_task_name)


@dataclass
class MeterStat:
    # 总量 / 文件数
    total_bytes: int = 0
    total_files: int = 0
    # 已落地（目标盘里真实存在、可校验的字节）= 上传完成量
    landed_bytes: int = 0
    landed_files: int = 0
    # 下载侧：源盘已取回字节（= 已落地 + 本机暂存的完整/部分下载）
    dl_bytes: int = 0
    # 上传侧：已上传字节（= 已落地 + 在途文件按 OpenList progress 折算）
    ul_bytes: int = 0
    inflight_uploaded: int = 0   # 在途文件中已上传的部分
    temp_bytes: int = 0          # 本机中转暂存字节（含部分下载）
    pending: int = 0             # 本任务在途（未完成）复制任务数
    fail_reason: str = ""        # 本任务范围内已失败的复制任务原因（若有）
    saw_ours: bool = False       # 是否见过「属于本任务」的复制任务（防枚举失败时假成功）

    # 兼容旧字段名（外部/测试仍可能引用）
    @property
    def done_bytes(self) -> int:
        return self.landed_bytes

    @property
    def done_files(self) -> int:
        return self.landed_files


def _progress(t: dict) -> float:
    """OpenList 复制任务的 progress（实测语义：该文件「上传阶段」百分比 0~100）。

    注意：任务重试时该值可能残留上一次的进度（不会重置），故只用于「上传量估算」
    与「是否在推进」的判定，不用于最终完成判定（完成一律以目标盘校验为准）。
    """
    try:
        p = float(t.get("progress") or 0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(p, 100.0))



def _succeeded(t: dict) -> bool:
    s = t.get("state")
    if isinstance(s, bool):
        return s
    if isinstance(s, int):
        return s == 2
    txt = str(s).lower()
    return "succeed" in txt or txt in ("done", "2")


class RelayMeter:
    """一次跨平台中转的计量器（源树 + 任务归集 + 目的地校验）。

    并发提示：OpenList 的 temp 目录是**全局共享**的，多个中转同时跑时谁也没法把
    「本机暂存」归属到自己名下。故用类级计数 `_active` 标记「当前在跑的中转数」：
    并发 >1 时**不计 temp**，下载量退化为 max(已落地, 上传量)。否则会把他人的在途
    字节算成自己的下载量 —— 进度虚高，且对方的 temp 收缩时会被误判成
    「整文件重下载」而触发看门狗中止。
    """

    _active = 0
    _active_lock = threading.Lock()

    @classmethod
    def enter(cls) -> None:
        """标记「一个中转进入复制/轮询阶段」（编排器在 ol.copy 前调用）。"""
        with cls._active_lock:
            cls._active += 1

    @classmethod
    def leave(cls) -> None:
        with cls._active_lock:
            cls._active = max(0, cls._active - 1)

    @classmethod
    def active_count(cls) -> int:
        with cls._active_lock:
            return cls._active

    @classmethod
    def temp_attributable(cls) -> bool:
        """temp 目录能否归属到单个任务（只有独占时才可信）。"""
        with cls._active_lock:
            return cls._active <= 1

    def __init__(self, ol, c, src_dir: str, dst_dir: str,
                 temp_dir: str = "", names: list[str] | None = None,
                 src_mount: str = "", dst_mount: str = "") -> None:
        self.ol = ol
        self.c = c
        self.src_dir = (src_dir or "").rstrip("/")
        self.dst_dir = (dst_dir or "").rstrip("/")
        self.src_mount = (src_mount or "").rstrip("/")
        self.dst_mount = (dst_mount or "").rstrip("/")
        self.temp_dir = temp_dir or ""
        self.names = [n for n in (names or []) if n]
        # 任务名里是「挂载内相对路径」，我们的目录带挂载前缀 → 两种写法都留作比较基准。
        # 挂载名未知时补一个「去掉首段」的猜测基准（容忍度更高，可能略宽）。
        self.src_bases = self._bases(self.src_dir, self.src_mount)
        self.dst_bases = self._bases(self.dst_dir, self.dst_mount)
        self.files: dict[str, int] = {}     # 相对 src_dir 的路径 -> 大小
        self.total = 0
        self.max_file = 0
        self._done: dict[str, int] = {}     # 已完成（只增不减：任务表可能被裁剪）
        self._pending = 0
        self._tick = 0                      # poll 计数（done 表拉取节流用）
        self.saw_ours = False
        self.enumerated = False

    @staticmethod
    def _bases(path: str, mount: str) -> list[str]:
        out = [p for p in (path,
                           _strip_mount(path, mount) if mount else "",
                           _strip_first(path) if not mount else "") if p and p != "/"]
        return list(dict.fromkeys(out))

    # ---- 源树枚举 ----

    def scan_source(self, max_calls: int = 600) -> None:
        """枚举**待复制项**（`names`）下的所有文件（相对路径 → 大小）。

        只统计 names 命中的子树，而不是整个 src_dir——src_dir 很可能只是「包含待复制
        项的父目录」（夸克自分享降级场景），整目录递归会把无关文件算进总量。
        """
        self.files.clear()
        self._done.clear()
        self.total = 0
        self.max_file = 0
        roots = [n.strip("/") for n in self.names if n and n.strip("/")]
        stack: list[str] = []
        if not roots:
            stack = [""]            # names 未知：退回枚举整个 src_dir（老行为）
        else:
            top: dict[str, dict] = {}
            try:
                for it in self.ol.list_dir(self.c, self.src_dir):
                    nm = str(it.get("name") or "")
                    if nm:
                        top[nm] = it
            except Exception:
                top = {}
            for r in roots:
                it = top.get(r)
                if it is None or it.get("is_dir"):
                    stack.append(r)     # 目录 → 展开；未命中 → 当子路径再试一次
                else:
                    self._add(r, int(it.get("size") or 0))
        calls = 0
        while stack and calls < max_calls:
            rel = stack.pop()
            calls += 1
            path = self.src_dir + ("/" + rel if rel else "")
            for it in self.ol.list_dir(self.c, path):
                name = str(it.get("name") or "")
                if not name:
                    continue
                r = f"{rel}/{name}" if rel else name
                if it.get("is_dir"):
                    stack.append(r)
                else:
                    self._add(r, int(it.get("size") or 0))
        self.enumerated = bool(self.files) or calls > 0

    def _add(self, rel: str, size: int) -> None:
        self.files[rel] = size
        self.total += size
        if size > self.max_file:
            self.max_file = size

    def _rel(self, src_path: str) -> str:
        p = (src_path or "").rstrip("/")
        for base in self.src_bases:
            if _same_or_under(p, base):
                return p[len(base):].strip("/")
        return p.lstrip("/")

    def _info(self, t: dict) -> dict | None:
        """解析任务名；属于本任务范围才返回解析结果，否则 None。

        判定三层（缺一不可）：
        1. 源侧命中：任务源挂载 == 源盘挂载，且源路径落在 src_dir 之下；
        2. 目标侧命中：任务目标挂载 == 目标盘挂载，且目标路径落在 dst_dir 之下；
        3. 项名命中：源相对路径等于/属于 `names` 里的某一项——否则「同一源目录
           复制别的子项」的任务会被误认成自己的，导致永远等不完或误判失败。
        挂载名未知时（兜底路径）退化为不带挂载名的宽容比较。
        """
        info = parse_task_name(str(t.get("name") or ""))
        if not info:
            return None
        sm = (info.get("sm") or "").rstrip("/")
        dm = (info.get("dm") or "").rstrip("/")
        sp, dp = info.get("sp") or "", info.get("dp") or ""
        src_ok = ((not self.src_mount or sm == self.src_mount)
                  and any(_same_or_under(sp, b) for b in self.src_bases))
        dst_ok = ((not self.dst_mount or dm == self.dst_mount)
                  and any(_same_or_under(dp, b) for b in self.dst_bases))
        if not (src_ok and dst_ok):
            return None
        rel = self._rel(sp)
        if self.names and rel:
            if not any(rel == n.strip("/") or rel.startswith(n.strip("/") + "/")
                       for n in self.names):
                return None
        return info

    # ---- 轮询归集 ----

    def poll(self) -> MeterStat:
        """轮询一次：归集「已落地」，并分别估算「下载量」「上传量」。

        - 下载量 = 已落地 + 本机暂存（temp，含部分下载的文件）
        - 上传量 = 已落地 + Σ(在途文件大小 × OpenList progress%)
        这样「下载完成但还在上传」时上传量继续增长，不会像旧实现那样
        把 temp 当成已传输、一上来就假 100%。
        """
        undone = self.ol.copy_tasks(self.c, "undone")
        pending = 0
        inflight_uploaded = 0
        for t in undone:
            info = self._info(t)
            if not info:
                continue
            pending += 1
            self.saw_ours = True
            sz = self.files.get(self._rel(info["sp"]))
            if sz:
                inflight_uploaded += int(sz * _progress(t) / 100.0)
        self._pending = pending
        # done 表「只增不减」（历史任务全部堆积在内），全量拉取是每拍最重的一笔。
        # 无在途任务时必须拉（完成判定/失败原因都依赖）；仍有在途时降到每 3 拍一次
        # ——期间 landed 冻结，但上传量仍由在途 progress% 驱动，速率与停滞检测不受影响。
        self._tick += 1
        if pending == 0 or self._tick % 3 == 1:
            done = self.ol.copy_tasks(self.c, "done")
        else:
            done = []
        fail_reason = ""
        for t in done:
            info = self._info(t)
            if not info:
                continue
            self.saw_ours = True
            rel = self._rel(info["sp"])
            if not rel or rel not in self.files:
                continue            # 目录级任务 / 不在待复制清单里：忽略
            if _succeeded(t):
                self._done.setdefault(rel, self.files[rel])
            elif rel not in self._done and not fail_reason:
                raw = str(t.get("error") or t.get("status") or t.get("state") or "未知原因")
                fail_reason = f"{rel}：{_zh_error(raw)}"

        landed = sum(self._done.values())
        # 并发多个中转时 temp 目录被共用，无法归属 → 不计 temp（见类注释）
        temp = _dir_bytes(self.temp_dir) if self.temp_attributable() else 0
        total = self.total
        # 上传量：已落地 + 在途文件按 OpenList 上传进度折算
        ul = landed + inflight_uploaded
        # 下载量：物理铁律——**已上传的字节必然已先从源盘下载下来**，故下载量 ≥ 上传量。
        # 小文件 / 单分片复制走**流式直传**，根本不落 temp（temp 恒 0），此时唯一可信
        # 的实时信号就是 OpenList 的上传进度 → 取二者较大值反推下载量。否则界面会出现
        # 「下载 0%（0 B）而上传 23%」这种反物理组合（2026-09-18 实测 8.1MB 百度→夸克）。
        dl = max(landed + temp, ul)
        if total:
            dl = min(dl, total)
            ul = min(ul, total)
        return MeterStat(total_bytes=total,
                         total_files=len(self.files),
                         landed_bytes=landed,
                         landed_files=len(self._done),
                         dl_bytes=dl,
                         ul_bytes=ul,
                         inflight_uploaded=inflight_uploaded,
                         temp_bytes=temp,
                         pending=self._pending,
                         fail_reason=fail_reason,
                         saw_ours=self.saw_ours)

    # ---- 目的地完整性校验 ----

    def verify_destination(self, max_calls: int = 600) -> tuple[bool, str]:
        """核对目标目录：源里每个文件都在（名字+大小一致）。"""
        dst: dict[str, int] = {}
        stack = [""]
        calls = 0
        while stack and calls < max_calls:
            rel = stack.pop()
            calls += 1
            path = self.dst_dir + ("/" + rel if rel else "")
            for it in self.ol.list_dir(self.c, path, refresh=True):
                name = str(it.get("name") or "")
                if not name:
                    continue
                r = f"{rel}/{name}" if rel else name
                if it.get("is_dir"):
                    stack.append(r)
                else:
                    dst[r] = int(it.get("size") or 0)
        if not self.files:
            # 源枚举失败时退化：每个待复制名字都要在目标里出现
            missing = [n for n in self.names
                       if not any(d == n or d.startswith(n + "/") for d in dst)]
            if missing:
                return False, "目标缺少：" + "、".join(missing[:3])
            return True, ""
        missing = [r for r in self.files if r not in dst]
        if missing:
            return False, f"目标缺少 {len(missing)} 个文件（如 {missing[0]}）"
        bad = [r for r in self.files if dst.get(r) != self.files[r]]
        if bad:
            r = bad[0]
            return False, (f"{len(bad)} 个文件大小不符（如 {r}：目标 {dst.get(r)} "
                           f"≠ 源 {self.files[r]}）")
        return True, ""
