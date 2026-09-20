"""OpenList 客户端：跨平台流式中转的执行器（阶段 3）。

职责：在两个已挂载的网盘存储之间发起 fs/copy，并等待复制任务完成。

数据路径（2026-09-19 更正）：OpenList 的跨驱动复制是
`op.Link(源) → stream.SeekableStream → op.Put(目标)` 的**流式管道**，并非
「先整体下载、再上传」的两段式。**是否落本机 temp** 由 SeekableStream 的读取
路径决定（见 upstream `internal/stream/stream.go`）：
- 消费方顺序读（单分片 PUT，无需随机访问）→ 不落盘；
- 要随机访问/分片，而源链支持 HTTP Range → 按 Range 直取云端，也不落盘；
- 要随机访问但源链只给得出顺序 Reader → `CacheFullInTempFile()` **整份写进
  temp_dir**（源码注释直言「文件写完之前不能开始上传」）→ 这时才等价于
  「先落盘再上传」。
（旧文档的「边下边传、不落盘」与旧注释的「整份下载完再上传」各只说对了一半。）

接口约定（AList/OpenList 通用）：
- POST /api/fs/copy        {src_dir, dst_dir, names[]}  → 提交复制任务
- GET  /api/task/copy/*    undone / working / done 三张任务表轮询
- POST /api/fs/mkdir       {path}                        → 目标目录不存在时创建

所有请求走家宽直连（trust_env=False，代理隔离铁律）。
"""
from __future__ import annotations

import os
import re
import time
from pathlib import Path

import httpx

from adapters.base import AdapterError

# 复制任务名形如：copy [/quark](/src/a/b.txt) to [/baidu](/转存/a/b.txt)
_TASK_RE = re.compile(r"^copy \[(?P<sm>.*?)\]\((?P<sp>.*)\) to \[(?P<dm>.*?)\]\((?P<dp>.*)\)$")


def parse_task_name(name: str) -> dict | None:
    """解析 OpenList 复制任务名，返回 {src_mount, src_path, dst_mount, dst_path}。

    注意：括号里的路径是**挂载内相对路径**（不含挂载名），例如
    `copy [/quark](/微信分享/游戏/a.txt) to [/baidu](/转存/a.txt)` 里 sp 是
    `/微信分享/游戏/a.txt` 而不是 `/quark/微信分享/游戏/a.txt`。与我们的
    src_dir/dst_dir（带挂载前缀）比较时必须走 `_same_or_under` 之类的宽容比较。
    """
    m = _TASK_RE.match((name or "").strip())
    return m.groupdict() if m else None


def _norm(path: str) -> str:
    """归一化路径：折叠重复/首尾斜杠（'' → '/'）。"""
    segs = [x for x in (path or "").split("/") if x]
    return "/" + "/".join(segs) if segs else "/"


def _strip_mount(path: str, mount: str) -> str:
    """去掉挂载前缀：('/baidu/转存', '/baidu') → '/转存'；不匹配则原样归一化返回。"""
    p, m = _norm(path), _norm(mount)
    if not mount:
        return p
    if p == m:
        return "/"
    if p.startswith(m + "/"):
        return p[len(m):] or "/"
    return p


def _strip_first(path: str) -> str:
    """去掉首个路径段：'/quark/转存' → '/转存'；单段路径返回 ''。"""
    segs = [x for x in (path or "").split("/") if x]
    return "/" + "/".join(segs[1:]) if len(segs) > 1 else ""


def _same_or_under(p: str, base: str) -> bool:
    """p 是否等于 base 或落在 base 之下（都先归一化；base 为根时恒真）。"""
    p, base = _norm(p), _norm(base)
    if base == "/":
        return True
    return p == base or p.startswith(base + "/")


def _dir_bytes(path: str) -> int:
    """本地目录字节数（OpenList 临时目录 → 「在途」下载量）。"""
    if not path:
        return 0
    try:
        total = 0
        for n in os.listdir(path):
            p = os.path.join(path, n)
            try:
                if os.path.isfile(p):
                    total += os.path.getsize(p)
            except OSError:
                pass
        return total
    except OSError:
        return 0

# OpenList 复制任务的底层错误 → 中文转译（关键词匹配）
_ERROR_ZH = [
    ("empty files are not allowed by baidu netdisk", "百度网盘不支持保存空文件"),
    ("quota not enough", "目标网盘空间不足"),
    ("file [", "目标「转存」目录已存在同名文件（同一分享重复转存会命中；可先删除旧文件再试）"),
    ("not found", "源文件不存在（可能已被删除）"),
    ("permission denied", "目标网盘权限不足"),
]


def _zh_error(err: str) -> str:
    low = err.lower()
    for key, zh in _ERROR_ZH:
        if key in low:
            return zh
    return err


class OpenListClient:
    def __init__(self, base: str, token: str, timeout: float = 30.0) -> None:
        self.base = base.rstrip("/")
        self.token = token

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self.token, "Content-Type": "application/json"}

    def _post(self, c: httpx.Client, path: str, json_body: dict) -> dict:
        try:
            r = c.post(f"{self.base}{path}", headers=self._headers(), json=json_body)
        except httpx.HTTPError as e:
            raise AdapterError(f"OpenList 连接失败（检查服务是否在线）: {e}")
        try:
            data = r.json()
        except ValueError:
            raise AdapterError(f"OpenList 返回异常（HTTP {r.status_code}，非 JSON）")
        if data.get("code") != 200:
            raise AdapterError(f"OpenList {path} 失败: {data.get('message', '')}")
        return data.get("data") or {}

    def _get(self, c: httpx.Client, path: str) -> dict:
        try:
            r = c.get(f"{self.base}{path}", headers=self._headers())
        except httpx.HTTPError as e:
            raise AdapterError(f"OpenList 连接失败（检查服务是否在线）: {e}")
        try:
            data = r.json()
        except ValueError:
            raise AdapterError(f"OpenList 返回异常（HTTP {r.status_code}，非 JSON）")
        if data.get("code") != 200:
            raise AdapterError(f"OpenList {path} 失败: {data.get('message', '')}")
        return data.get("data") or {}

    def ensure_dir(self, c: httpx.Client, path: str) -> None:
        """目标目录不存在时创建（已存在时 OpenList 返回非 200，忽略之）。"""
        try:
            self._post(c, "/api/fs/mkdir", {"path": path})
        except AdapterError:
            pass  # 目录已存在等场景

    def _list_page(self, c: httpx.Client, path: str, page: int,
                   per_page: int, refresh: bool) -> list[dict]:
        """取目录的一页内容；取不到（目录不存在/未同步）返回空列表。"""
        try:
            data = self._post(c, "/api/fs/list",
                              {"path": path, "page": page,
                               "per_page": per_page, "refresh": refresh})
        except AdapterError:
            return []
        content = data.get("content") if isinstance(data, dict) else data
        return [it for it in (content or []) if isinstance(it, dict)]

    def existing_names(self, c: httpx.Client, dst_dir: str, names: list[str],
                       per_page: int = 500, max_pages: int = 20) -> dict[str, int]:
        """查询 dst_dir 下与 names 同名的已存在条目 → `{名字: 大小}`。

        用于复制前的幂等预检（重复转存时跳过已复制文件，避免 fs/copy 因
        file exists 整单失败）。

        - **必须翻页**：只查第一页时，转存目录超过一页（旧实现固定 200 条）就漏判、
          幂等预检失效；命中全部待查名字（或本页未满）即收工。
        - 返回大小供调用方做**内容校验**：同名但大小不同 = 很可能是另一个文件，
          盲目复用会把错误的内容拿去生成分享。
        - 兼容：返回 dict 对 `x in result` 的用法与旧的 set 完全一致。
        """
        want = {n for n in names if n}
        if not want:
            return {}
        found: dict[str, int] = {}
        for page in range(1, max_pages + 1):
            items = self._list_page(c, dst_dir, page, per_page, refresh=(page == 1))
            for it in items:
                nm = it.get("name")
                if nm in want and nm not in found:
                    try:
                        found[nm] = int(it.get("size") or 0)
                    except (TypeError, ValueError):
                        found[nm] = 0
            if want <= set(found) or len(items) < per_page:
                break
        return found

    def list_dir(self, c: httpx.Client, path: str, refresh: bool = False,
                 per_page: int = 500, max_pages: int = 20) -> list[dict]:
        """列出目录内容（用于源树枚举 / 目的地完整性校验）。

        **必须翻页**：只取第一页会漏条目 —— 源树枚举会低估总字节（进度虚高），
        目的地校验会漏判缺失文件（**假通过**，最危险的一类）。本页未满即认为取完。
        `max_pages` 可调小，供「只需扫一眼顶层」的调用方（如同名文件大小核对）。
        """
        out: list[dict] = []
        for page in range(1, max_pages + 1):
            items = self._list_page(c, path, page, per_page,
                                    refresh=(refresh and page == 1))
            out.extend(items)
            if len(items) < per_page:
                break
        return out

    # ---- 复制任务表（进度归集 / 取消） ----

    def copy_tasks(self, c: httpx.Client, which: str) -> list[dict]:
        """which: 'undone'（未完成，含运行中）| 'done'（已结束）。取不到返回 []。"""
        try:
            data = self._get(c, f"/api/task/copy/{which}")
        except AdapterError:
            return []
        rows = data.get("tasks") if isinstance(data, dict) else data
        return [t for t in (rows or []) if isinstance(t, dict)]

    def pending_under(self, c: httpx.Client, dst_dir: str,
                      mount: str = "") -> list[dict]:
        """未完成任务中，目标目录落在 dst_dir 之下的（= 本任务尚未搬完的字节）。

        `mount` 为该目标盘的挂载名（如 `/baidu`）：任务名里的路径是挂载内相对
        路径，传了 mount 才能把 `/baidu/转存` 与任务里的 `/转存` 对齐。
        """
        want = _norm(dst_dir)
        want_rel = _strip_mount(dst_dir, mount) if mount else ""
        out = []
        for t in self.copy_tasks(c, "undone"):
            info = parse_task_name(str(t.get("name") or ""))
            if not info:
                continue
            dp = info["dp"]
            if _same_or_under(dp, want) or (want_rel and _same_or_under(dp, want_rel)):
                out.append(t)
        return out

    def cancel(self, c: httpx.Client, tid: str) -> bool:
        """取消单个复制任务（注意：tid 是 query 参数，放 body 会 404）。"""
        try:
            r = c.post(f"{self.base}/api/task/copy/cancel",
                       headers=self._headers(), params={"tid": tid})
        except httpx.HTTPError as e:
            raise AdapterError(f"OpenList 连接失败（取消任务）: {e}")
        try:
            return (r.json() or {}).get("code") == 200
        except ValueError:
            return False

    def cancel_under(self, c: httpx.Client, dst_dir: str, mount: str = "") -> int:
        """兜底：取消所有目标落在 dst_dir 之下的未完成任务，返回成功条数。"""
        n = 0
        for t in self.pending_under(c, dst_dir, mount=mount):
            tid = str(t.get("id") or "")
            if tid and self.cancel(c, tid):
                n += 1
        return n

    def copy(self, c: httpx.Client, src_dir: str, dst_dir: str,
             names: list[str]) -> list[str]:
        """提交复制任务，返回任务 id 列表（可能为空——部分版本不回传 id）。"""
        data = self._post(c, "/api/fs/copy",
                          {"src_dir": src_dir, "dst_dir": dst_dir, "names": names})
        tasks = data.get("tasks") if isinstance(data, dict) else data
        ids: list[str] = []
        for t in tasks or []:
            tid = t.get("id") if isinstance(t, dict) else t
            if tid is not None:
                ids.append(str(tid))
        return ids

    def wait_copy(self, c: httpx.Client, task_ids: list[str], src_dir: str,
                  dst_dir: str, names: list[str], progress,
                  *, meter=None, abort_check=None, stall_min: float = 15.0,
                  max_hours: float = 24.0, temp_dir: str = "",
                  src_mount: str = "", dst_mount: str = "",
                  poll_interval: float = 5.0) -> None:
        """等待跨盘复制「真正」完成，并**分两行**回报下载/上传实时进度。

        完成判定 = 目标目录下已无未完成任务 且 源文件全部成功 且 **目的地校验通过**。

        为什么不能只等 fs/copy 返回的 task id（2026-09 事故根因）：OpenList 对目录
        的 copy 会拆成「每个节点一个任务」，且**目录级任务在子任务入队完成时就置
        done**——只看返回的 id 会「假成功」，随后生成的分享链接内容不全。

        进度分两路（2026-09-18 加固）：
        - 下载 = 源盘 → 本机 temp（OpenList 是「整份下载完再上传」，非流式管道）
        - 上传 = 本机 temp → 目标盘（用 OpenList 任务 progress% 折算在途文件）
        保护：**双通道停滞检测**（下载或上传任一路在动就不算停滞，避免上传大文件
        时被误判停滞并取消任务）与 **max_hours 硬上限**。`abort_check` 命中协作式
        取消信号时由调用方抛异常。
        """
        from relay_meter import RelayMeter

        if meter is None:
            meter = RelayMeter(self, c, src_dir, dst_dir, temp_dir=temp_dir,
                               names=names, src_mount=src_mount,
                               dst_mount=dst_mount)
            meter.scan_source()
        total = meter.total
        t0 = time.time()
        deadline = t0 + max(0.1, float(max_hours)) * 3600
        stall = max(1.0, float(stall_min) * 60.0)   # 分钟 → 秒（下限 1 秒）
        # 双通道停滞检测：下载（源盘→本机）与上传（本机→目标盘）分别记「最后进展时刻」，
        # **任一路在动就不算停滞**。旧实现只看「下载量」，上传大文件时必然假停滞并误杀
        # （2026-09-17 事故：正在上传 21.5GB 时被判「停滞」，差点连数据一起废掉）。
        last_dl = last_ul = t0
        prev_dl = prev_ul = 0
        prev_t = t0
        dl_rate = ul_rate = 0.0
        max_dl_seen = 0          # 下载量历史峰值（用于侦测「整文件重下载」回退）
        dl_regress = 0           # **连续**回退次数（健康时归零；累计制会把良性的
                                 # temp↔landed 交接波动跨小时攒成 3 次，误杀健康任务）
        prev_attrib: bool | None = None   # temp 归属模式（并发开始/结束时翻转）
        while True:
            if abort_check:
                abort_check()          # 命中取消信号时抛出（协作式取消）
            time.sleep(max(0.5, poll_interval))
            st = meter.poll()
            now = time.time()
            # 防 OpenList 内部重试导致的无限重下载：百度/夸克单文件超上限被服务端拒绝后，
            # OpenList 会「整文件重新下载→重新上传」循环。下载量较历史峰值回退 >50% 即一次
            # 重下载信号；**连续** 3 次主动中止（真回退在爬回半峰值的整个过程中会连续命中，
            # 而单轮交接抖动只命中 1 次就被归零），避免把中转盘写满还永远跑不完（2026-09-18 实锤）。
            attrib = RelayMeter.temp_attributable()
            attrib_flipped = prev_attrib is not None and attrib != prev_attrib
            prev_attrib = attrib
            if attrib_flipped:
                # 并发中转开始/结束 → temp 归属切换，dl 口径突变是**正常现象**：
                # 本轮跳过回退检测并把基线重置到当前值，否则会把健康传输误判成
                # 「反复重试」而中止（2026-09-19 审查发现的误杀路径）。
                max_dl_seen = st.dl_bytes
                dl_regress = 0
            elif st.dl_bytes > max_dl_seen:
                # 创出新高 = 真实推进越过此前所有历史：回退历史一笔勾销
                max_dl_seen = st.dl_bytes
                dl_regress = 0
            elif max_dl_seen > 0 and st.dl_bytes < max_dl_seen * 0.5:
                dl_regress += 1
                if dl_regress >= 3:
                    raise AdapterError(
                        "OpenList 反复从头重试下载（疑似超过目标盘单文件上限被服务端拒绝），"
                        "已停止无限循环。请改用「本机高速」落本地，或将大文件分卷后转存未超限部分。")
            # 50%~100% 区间的正常波动：不增不清，维持「累计 3 次」的原始契约
            # （2026-09-18 事故的回归测试即按此语义编码，不可弱化）
            dt = max(0.1, now - prev_t)
            if st.dl_bytes > prev_dl:
                inst = (st.dl_bytes - prev_dl) / dt
                dl_rate = inst if dl_rate <= 0 else (0.7 * dl_rate + 0.3 * inst)
                last_dl = now
            if st.ul_bytes > prev_ul:
                inst = (st.ul_bytes - prev_ul) / dt
                ul_rate = inst if ul_rate <= 0 else (0.7 * ul_rate + 0.3 * inst)
                last_ul = now
            prev_dl, prev_ul, prev_t = st.dl_bytes, st.ul_bytes, now

            def _line(label: str, done: int, rate: float) -> str:
                pct = int(done * 100 / total) if total else 0
                s = f"{label} {_gb(done)}/{_gb(total)}（{pct}%）"
                if rate > 0:
                    s += f" · {_gb(rate)}/s"
                    left = max(0, total - done)
                    if left:
                        s += f" · 剩 {_dur(left / rate)}"
                return s

            # 两行显示：第 1 行下载（源盘 → 本机中转），第 2 行上传（本机中转 → 目标盘）
            msg = (_line("下载", st.dl_bytes, dl_rate) + "\n"
                   + _line("上传", st.ul_bytes, ul_rate))
            if st.pending:
                msg += f" · 在途 {st.pending} 个"
            if st.temp_bytes:
                msg += f" · 本机暂存 {_gb(st.temp_bytes)}"
            # 总进度取「下载 + 上传」两段的平均：只下载完不算完成，两边都满才 100%
            overall = int((st.dl_bytes + st.ul_bytes) * 100 / (2 * total)) if total else 0
            # 结构化明细：供「转存记录」分两行展示（下载 / 上传各自的字节数）
            detail = {"relay": True, "dl_done": st.dl_bytes,
                      "ul_done": st.ul_bytes, "total": total,
                      "total_files": st.total_files}
            progress(overall, msg, detail=detail)

            # 完成判定：队列清空 + 源文件全部成功 + 目的地校验
            if st.pending == 0:
                if st.total_files:
                    ready = st.landed_files >= st.total_files
                else:
                    # 源枚举为空（多为源目录枚举失败）：至少要见过属于本任务的
                    # 复制任务，否则「什么都没提交」也会被当成完成（假成功）
                    ready = st.saw_ours
                if ready:
                    ok, why = meter.verify_destination()
                    if ok:
                        progress(100, f"复制完成（{_gb(total)}，共 {st.total_files} 个文件）",
                                 detail={"relay": True, "dl_done": total,
                                         "ul_done": total, "total": total,
                                         "total_files": st.total_files})
                        return
                    # 任务表可能被裁剪 / 目标索引未同步 → 继续等，交由停滞检测兜底
                elif st.fail_reason:
                    raise AdapterError(f"OpenList 复制失败：{st.fail_reason}")

            if now - max(last_dl, last_ul) > stall:
                ok, why = meter.verify_destination()
                if ok:
                    progress(100, f"复制完成（{_gb(total)}）",
                             detail={"relay": True, "dl_done": total,
                                     "ul_done": total, "total": total,
                                     "total_files": st.total_files})
                    return
                raise AdapterError(
                    f"OpenList 中转停滞：{int(stall / 60)} 分钟内下载与上传均无任何进展，"
                    f"且目标校验未通过（{why}）")
            if now > deadline:
                raise AdapterError(f"OpenList 中转超时（超过 {max_hours} 小时），已中止")


def _gb(n: float) -> str:
    n = float(n or 0)
    if n >= 2 ** 40:
        return f"{n / 2 ** 40:.2f} TB"
    if n >= 2 ** 30:
        return f"{n / 2 ** 30:.2f} GB"
    if n >= 2 ** 20:
        return f"{n / 2 ** 20:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.0f} KB"
    return f"{n:.0f} B"


def _dur(sec: float) -> str:
    sec = int(max(0, sec))
    if sec >= 86400:
        return f"{sec // 86400}天{(sec % 86400) // 3600}小时"
    if sec >= 3600:
        return f"{sec // 3600}小时{(sec % 3600) // 60}分"
    if sec >= 60:
        return f"{sec // 60}分"
    return f"{sec}秒"
