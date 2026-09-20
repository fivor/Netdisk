"""编排引擎：按方案优先级调度转存任务。

阶段 2：同平台直转 —— source == target 时走对应适配器（save + share）。
阶段 3：跨平台中转 —— 源盘 save → OpenList fs/copy 流式复制（不落盘）
        → 目标盘 locate → share 生成新分享。

调度模型：ThreadPoolExecutor（低并发，方案定版不引 Celery/Redis）。
"""
from __future__ import annotations

import asyncio
import httpx
import shutil
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Callable
from urllib.parse import quote as urlquote

from adapters.base import (Adapter, AdapterError, AdapterNotConfigured,
                           CapabilityError, SavedRef, TransferResult,
                           build_registry)
from db import TaskStore
from http_client import new_client
from openlist import OpenListClient, _gb
from parser import ShareLink, PLATFORM_LABELS, parse_share_text, validate_password
from relay_meter import RelayMeter


class TaskCancelled(Exception):
    """用户在任务执行中请求取消（协作式，阶段间生效）。"""


class NeedsConfirm(Exception):
    """中转时发现部分文件超过目标盘单文件上限，需用户确认是否跳过超限继续。

    由 _relay 在「已把任务置为 needs_confirm 并写入超限清单」之后抛出，
    _run_task 捕获后仅结束本 worker 线程（不写失败），等待前端「继续/取消」回调。
    """


# 编排优先级（方案第二节）：
# 1. 同平台直转           -> adapter.transfer（save + share）
# 2. 跨平台流式中转        -> save(源) → OpenList fs/copy → locate(目标) → share(目标)
# 3. 秒传加速 / 离线下载    -> 后续迭代


class Orchestrator:
    def __init__(self, store: TaskStore, settings, max_workers: int = 2,
                 registry_builder=None, openlist_factory=None) -> None:
        self.store = store
        self.settings = settings
        # registry_builder(client) -> {platform: Adapter}；默认构建真实适配器。
        # 独立成构造参数既支持测试注入假适配器，也保证每任务拿到绑定新
        # AsyncClient 的适配器实例（httpx 客户端不可跨事件循环复用）。
        self._build_registry = registry_builder or (
            lambda client: build_registry(settings, client=client))
        # 静态注册表：仅用于就绪状态展示与目标平台校验（不发起网络请求）
        self._adapters = self._build_registry(None)
        self._openlist_factory = openlist_factory or (
            lambda: OpenListClient(settings.openlist_base, settings.openlist_token))
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="transfer")
        # 取消信号集合（协作式取消）：worker 在阶段间检查，命中即中止并落 cancelled
        self._cancelled: set[str] = set()
        self._cancel_lock = threading.Lock()
        # 进度单调护栏：记录每任务已上报的最大进度，防止适配器回调倒序
        # （如「40% 转存中」后又报「35% 定位中」）导致前端进度条回退。
        self._peak_progress: dict[str, int] = {}
        self._peak_lock = threading.Lock()
        # 正在执行中的任务集合：清理取消信号时必须跳过它们——否则「DB 已落 cancelled
        # 但 worker 还在收尾」的任务会被提前摘除信号，最后一道取消检查落空、误写回成功。
        self._executing: set[str] = set()
        self._exec_lock = threading.Lock()
        # 并发/排队可视：waiting = 已投进线程池但尚未拿到 worker 名额的「执行单元」数
        # （一个组合转存组算 1 个单元 —— 组内串行，只占 1 个 worker）；
        # running = 正在执行的单元数。用于给用户「需要排队等待」的提醒。
        self._max_workers = max(1, int(max_workers))
        self._q_lock = threading.Lock()
        self._q_waiting = 0
        self._q_running = 0

    # ---- 并发 / 排队可视 ----

    def queue_info(self) -> dict:
        """当前并发概况（前端据此提示「需要排队等待」）。"""
        with self._q_lock:
            return {"workers": self._max_workers,
                    "running": self._q_running,
                    "waiting": self._q_waiting}

    def _dispatch(self, fn, *args) -> bool:
        """把执行单元投进线程池；返回 True 表示它**需要排队等待**。

        判定必须在入队瞬间做：已有单元在排队、或所有 worker 都忙着 → 新的必然要等。
        （若改成「提交后再读计数」，worker 可能已经抢先开跑，永远读不到等待。）
        """
        with self._q_lock:
            will_wait = self._q_waiting > 0 or self._q_running >= self._max_workers
            self._q_waiting += 1
        try:
            self._pool.submit(fn, *args)
        except Exception:
            with self._q_lock:                      # 池已关闭 → 撤销排队计数
                self._q_waiting = max(0, self._q_waiting - 1)
            raise
        return will_wait

    def _q_enter(self) -> None:
        """worker 真正开始执行：排队数 -1、执行数 +1。"""
        with self._q_lock:
            self._q_waiting = max(0, self._q_waiting - 1)
            self._q_running += 1

    def _q_leave(self) -> None:
        with self._q_lock:
            self._q_running = max(0, self._q_running - 1)

    # ---- 取消 ----

    def cancel(self, task_id: str) -> None:
        """请求取消任务。worker 在下一个阶段检查点生效（无法中断阻塞中的网络调用）。

        组合转存：取消组内**任一**子任务 = 取消整组（其余子任务一并置取消信号），
        否则串行执行中的兄弟会继续把同一份内容转存到其他网盘。
        """
        ids = {task_id}
        try:
            t = self.store.get(task_id)
            gid = (t or {}).get("group_id") or ""
            if gid:
                sibs = {c["id"] for c in self.store.list_group(gid)}
                if sibs:
                    ids = sibs
        except Exception:
            pass
        with self._cancel_lock:
            self._cancelled.update(ids)
        self._prune_cancelled()

    def cancel_group(self, group_id: str, reason: str = "已取消") -> int:
        """取消整组：置全组取消信号 + 把未终态子任务**立即**落 cancelled。

        返回被落终态的子任务数。needs_confirm / pending 的子任务其 worker 已退出或
        尚未被认领，必须在此显式落终态，否则会永久停在「待确认 / 排队中」。
        """
        children = self.store.list_group(group_id)
        if not children:
            return 0
        with self._cancel_lock:
            self._cancelled.update(c["id"] for c in children)
        n = 0
        for c in children:
            st = (self.store.get(c["id"]) or {}).get("status")
            if st in ("pending", "running", "needs_confirm"):
                self._safe_update(c["id"], status="cancelled", progress=0, message=reason)
                n += 1
        self._prune_cancelled()
        return n

    def _is_cancelled(self, task_id: str) -> bool:
        with self._cancel_lock:
            return task_id in self._cancelled

    def _clear_cancel(self, task_id: str) -> None:
        with self._cancel_lock:
            self._cancelled.discard(task_id)
        # 同时释放该任务的进度峰值记录，避免长时间运行下字典无限增长
        with self._peak_lock:
            self._peak_progress.pop(task_id, None)

    def _prune_cancelled(self) -> None:
        """回收「永远不会被 worker 认领」的任务残留的取消信号与峰值进度。

        `_clear_cancel` 只在 `_exec_one` finally 里跑——用户取消**尚未执行**的
        pending 子任务（组合转存取消整组、_cancel_rest 路径）时，那些 id 会永远
        留在 `_cancelled`/`_peak_progress` 里慢性泄漏。这里批量清理，但**跳过
        正在执行中的任务**（提前摘信号会让最后一道取消检查落空、误写回成功）。
        """
        with self._cancel_lock:
            if len(self._cancelled) <= 64:      # 量小不值得扫库
                return
            ids = list(self._cancelled)
        with self._exec_lock:
            executing = set(self._executing)
        for tid in ids:
            if tid in executing:
                continue
            st = (self.store.get(tid) or {}).get("status")
            if st in ("success", "failed", "cancelled"):
                self._clear_cancel(tid)

    def confirm_continue(self, task_id: str) -> None:
        """用户在前端确认「跳过超限文件继续转存」：置跳过标志并重新提交 worker。

        复用已落库的 src_ref 跳过源盘 save（见 _relay ①），只转存未超限文件。
        从 share_url 重建 ShareLink（与提交时同源），失败则回退到 needs_confirm 状态。
        """
        task = self.store.get(task_id)
        if task is None or task.get("status") != "needs_confirm":
            raise KeyError(f"任务不在待确认状态: {task_id}")
        try:
            share = parse_share_text(task["share_url"])
        except Exception:
            self.store.update(task_id, status="needs_confirm",
                             message="分享链接解析失败，请取消后重新提交")
            raise
        pwd = validate_password(share, task.get("password"))
        # 复位进度峰值护栏：needs_confirm 时峰值已到 30，不复位的话恢复后的
        # 前几笔回调会被钳到 30，前端进度从 30 起跳而不是从 1 爬。
        with self._peak_lock:
            self._peak_progress.pop(task_id, None)
        self.store.update(task_id, confirmed_skip=True, status="running",
                          progress=1, message="排队中")
        gid = task.get("group_id") or ""
        if gid:
            # 组合转存：从该子任务继续，并把组内尚未执行的其余目标一并跑完
            self._dispatch(self._run_group, gid, share, pwd,
                           int(task.get("group_seq") or 1))
        else:
            self._dispatch(self._run_task, task_id, share, task["target"], pwd)

    # ---- 对外 ----

    def status(self) -> dict:
        mounts = self.settings.openlist_mounts
        return {
            "adapters": {p: a.configured() for p, a in self._adapters.items()},
            "openlist_relay": bool(self.settings.openlist_token),
            "openlist_mounts": mounts,
        }

    def submit(self, task_id: str, share: ShareLink, target: str,
               password: str | None) -> bool:
        """提交任务到线程池（立即返回，进度写库）。

        返回 True = 该任务将**排队等待**（worker 已满，需等前面的转存跑完）。
        """
        self.store.update(task_id, status="running", message="排队中", progress=1)
        return self._dispatch(self._run_task, task_id, share, target, password)

    def submit_group(self, group_id: str, share: ShareLink,
                     password: str | None) -> bool:
        """提交「组合转存」组（一条链接 → 多个目标盘，组内**串行**执行）。

        组内只有当前执行中的子任务占线程池名额，其余保持 pending —— 一个 5 目标的
        组合也只吃 1 个 worker，不会挤占其他人的任务。

        返回 True = 该组将**排队等待**（worker 已满）。
        """
        for ch in self.store.list_group(group_id):
            self._safe_update(ch["id"], status="pending", message="排队中", progress=0)
        return self._dispatch(self._run_group, group_id, share, password, 1)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    # ---- 内部 ----

    def _safe_update(self, task_id: str, **fields) -> None:
        """任务写回容忍「已被用户删除」（KeyError 静默），避免兜底分支二次抛错。"""
        try:
            self.store.update(task_id, **fields)
        except KeyError:
            pass

    # ---- 跨平台中转的保护措施（2026-09 大文件事故后的加固） ----

    def _cancel_guard(self, task_id: str):
        """返回一个「命中取消信号就抛异常」的检查函数，交给 OpenList 等待循环调用。"""
        def _check() -> None:
            if self._is_cancelled(task_id):
                raise TaskCancelled()
        return _check

    def _check_relay_disk(self, meter: RelayMeter) -> None:
        """前置空间校验：跨盘中转会「先下载到本机临时文件再上传」，

        因此最大单文件必须放得下（总量不需要——临时文件会随文件完成释放）。
        不满足直接拒绝，避免把中转盘写满。
        """
        if not meter.max_file:
            return
        base = getattr(self.settings, "local_dir", "") or "."
        try:
            free = shutil.disk_usage(str(base)).free
        except OSError:
            return
        safety = float(getattr(self.settings, "relay_disk_safety", 1.2) or 1.2)
        # 并发修正：跨盘中转「先落 temp 再上传」，同时有 N 个中转在跑时实际需要
        # N × 最大单文件 的空间。只按单任务算会低估，容易把中转盘写满。
        concurrent = max(1, RelayMeter.active_count() + 1)   # +1 = 本任务自己
        need = int(meter.max_file * safety * concurrent)
        if need > free:
            raise AdapterError(
                f"本机中转空间不足：最大单文件 {_gb(meter.max_file)}，"
                f"按 {safety:g}× 安全系数 × 并发 {concurrent} 需预留 {_gb(need)}，"
                f"当前可用 {_gb(free)}。"
                f"跨平台中转需先下载到本机再上传，请清理磁盘或拆分分享后重试")

    def _existing_size_mismatch(self, ol, sync_c, src_dir: str,
                                existing: dict[str, int],
                                max_pages: int = 3) -> list[str]:
        """同名条目里「大小对不上」的那些 —— 目标盘那个是**另一个文件**，不能盲目复用。

        幂等预检按**名字**跳过；若目标盘的同名文件其实来自别的分享，就会把错误的
        文件拿去生成分享。故在真的有同名命中时（rare path，普通新转存零开销）
        再核对一次大小。

        只核对源侧**顶层文件**（一次受限 list_dir 即可）；目录要递归才知道总大小、
        代价高，仍按名复用（已知局限）。源侧查不到或大小为 0 时一律放行 ——
        宁可退化成旧行为，也不误报失败。
        """
        if not existing:
            return []
        top: dict[str, dict] = {}
        for it in ol.list_dir(sync_c, src_dir, max_pages=max_pages):
            nm = str(it.get("name") or "")
            if nm:
                top.setdefault(nm, it)
        bad: list[str] = []
        for nm, dst_size in existing.items():
            it = top.get(nm)
            if it is None or it.get("is_dir"):
                continue
            try:
                src_size = int(it.get("size") or 0)
            except (TypeError, ValueError):
                continue
            if src_size and dst_size and src_size != dst_size:
                bad.append(nm)
        return bad

    def _cap_gb(self, target: str) -> float:
        """目标盘单文件上限（GB）。local（本机高速）与未配置上限的盘返回 0（无限制）。"""
        if target == "local":
            return 0.0
        caps = getattr(self.settings, "target_file_cap_gb", None) or {}
        return float(caps.get(target) or 0)

    def _cap_bytes(self, target: str) -> int:
        return int(self._cap_gb(target) * 2 ** 30)

    def _oversized_top_names(self, meter: "RelayMeter", top_names: list[str], target: str) -> set[str]:
        """返回 src_ref.names 中「包含超限文件」的顶层项（复制阶段整体跳过这些项）。

        文件级大小来自 meter.files（rel 路径 → 大小）；一个顶层目录里只要有一个文件
        超过上限，整项都标记为超限并跳过，避免部分复制导致目的地校验失败。
        """
        cap = self._cap_bytes(target)
        if not cap:
            return set()
        top = [n.strip("/") for n in top_names if n and n.strip("/")]
        over: set[str] = set()
        for rel, sz in meter.files.items():
            if sz <= cap:
                continue
            for n in top:
                if rel == n or rel.startswith(n + "/"):
                    over.add(n)
                    break
        return over

    def _abort_relay(self, ol, c, dst_dir: str, task_ids: list[str],
                     mount: str = "") -> int:
        """取消本任务在 OpenList 里产生的复制任务（精确 id + 目标目录兜底）。"""
        n = 0
        for tid in list(task_ids or []):
            try:
                if ol.cancel(c, tid):
                    n += 1
            except Exception:
                pass
        try:
            n += ol.cancel_under(c, dst_dir, mount=mount)
        except Exception:
            pass
        return n

    # ---- 单任务 / 组合转存执行 ----

    def _exec_one(self, task_id: str, share: ShareLink, target: str,
                  password: str | None, reject_self_share: bool = False) -> str:
        """执行**一个**子任务并落终态，返回走向供调用方决策：

        - `"ok"`     该子任务成功
        - `"failed"` 该子任务失败
        - `"pause"`  命中「单文件超限待确认」（已置 needs_confirm），需用户决策
        - `"cancel"` 命中取消信号

        组合转存遇 `"pause"` / `"cancel"` 会暂停或中断整组；遇 `"failed"` 则
        **继续**跑组内其余目标（不浪费已完成的工作），整组最终状态由
        `group_status()` 派生为「失败」。
        """
        with self._exec_lock:
            self._executing.add(task_id)
        progress: Callable[..., None] = lambda p, m, detail=None: self._progress(task_id, p, m, detail=detail)
        try:
            if self._is_cancelled(task_id):
                raise TaskCancelled()
            if target not in self._adapters:
                raise AdapterError(f"不支持的目标平台: {target}")
            if share.platform == target:
                result = self._direct(share, target, password, progress)
                # 组合转存：自己的分享 + 同源目标 = 无意义组合。适配器此时会走
                # 「文件已在盘内 → 定位盘内文件」降级路径（并未真正转存），据此判失败。
                if reject_self_share and (result.details or {}).get("self_share"):
                    raise AdapterError(
                        f"这是你自己在「{PLATFORM_LABELS.get(share.platform, share.platform)}」"
                        f"的分享，文件本就在该网盘中，无需转存到同源目标")
            else:
                result = self._relay(task_id, share, target, password, progress)
            # 完成前最后一道取消检查：期间被取消则不落 success
            if self._is_cancelled(task_id):
                raise TaskCancelled()
            self._safe_update(
                task_id, status="success", progress=100,
                message=result.message or "完成",
                result_url=result.new_url, result_pwd=result.password,
                result_files=result.files)
            self._notify_done(task_id, share, target, result)
            return "ok"
        except TaskCancelled:
            self._safe_update(task_id, status="cancelled", progress=0, message="已取消")
            return "cancel"
        except NeedsConfirm:
            # 已在中转流程里把任务置为 needs_confirm 并写入超限清单/源盘引用，
            # 这里只结束 worker 线程，等前端「继续(跳过超限)/取消」回调。
            return "pause"
        except AdapterNotConfigured as e:
            self._safe_update(task_id, status="failed", error=str(e),
                              message="平台未配置", progress=0)
            return "failed"
        except AdapterError as e:
            self._safe_update(task_id, status="failed", error=str(e),
                              message="转存失败", progress=0)
            return "failed"
        except Exception as e:  # 未知异常兜底，任务不能静默消失
            traceback.print_exc()
            self._safe_update(task_id, status="failed", error=f"内部错误: {e}",
                              message="内部错误", progress=0)
            return "failed"
        finally:
            with self._exec_lock:
                self._executing.discard(task_id)
            self._clear_cancel(task_id)

    def _run_task(self, task_id: str, share: ShareLink, target: str, password: str | None) -> None:
        """单目标任务（非组合转存）。"""
        self._q_enter()
        try:
            self._exec_one(task_id, share, target, password)
        finally:
            self._q_leave()

    def _run_group(self, group_id: str, share: ShareLink, password: str | None,
                   start_seq: int = 1) -> None:
        """组合转存的线程池入口：登记并发计数后执行（整组只占 1 个 worker）。"""
        self._q_enter()
        try:
            self._run_group_inner(group_id, share, password, start_seq)
        finally:
            self._q_leave()

    def _run_group_inner(self, group_id: str, share: ShareLink, password: str | None,
                         start_seq: int = 1) -> None:
        """组合转存：按 group_seq 升序**串行**执行组内子任务。

        为什么串行（用户确认的执行语义）：
        ① **源盘只 save 一次**——首个跨平台子任务保存出的 src_ref 会广播给后续兄弟
           （配合 reuse_src），避免同一份分享在源盘里被转存 N 份（配额浪费 + 同名冲突）；
        ② 避免 N 个目标同时拉源盘，导致源盘风控与中转盘 temp 占用成倍放大。

        start_seq > 1 用于「单文件超限暂停 → 用户点继续」后从断点恢复整组。
        """
        children = self.store.list_group(group_id)
        if not children:
            return
        for ch in children:
            seq = int(ch.get("group_seq") or 0)
            if seq < start_seq:
                continue
            tid = ch["id"]
            target = ch["target"]
            if (self.store.get(tid) or {}).get("status") in ("success", "failed", "cancelled"):
                self._clear_cancel(tid)     # 已终态且不再执行：顺手回收取消信号/峰值
                continue                    # 恢复场景下已终态的兄弟：跳过
            outcome = self._exec_one(tid, share, target, password,
                                     reject_self_share=True)
            if outcome == "cancel":
                self._cancel_rest(children, seq, "同组任务已取消")
                return
            if outcome == "pause":
                # 该子任务已落 needs_confirm，其余保持 pending，等用户「继续/取消」
                return
            # 广播源盘引用：源盘只 save 一次，后续兄弟直接复用
            if target != share.platform:
                src = (self.store.get(tid) or {}).get("src_ref")
                if src:
                    for sib in children:
                        if int(sib.get("group_seq") or 0) > seq and not sib.get("src_ref"):
                            self._safe_update(sib["id"], src_ref=src, reuse_src=True)

    def _cancel_rest(self, children: list[dict], after_seq: int, reason: str) -> None:
        """把组内 seq 之后的未终态子任务一并落 cancelled，避免僵尸 pending。"""
        for ch in children:
            if int(ch.get("group_seq") or 0) <= after_seq:
                continue
            st = (self.store.get(ch["id"]) or {}).get("status")
            if st in ("pending", "running", "needs_confirm"):
                self.cancel(ch["id"])
                self._safe_update(ch["id"], status="cancelled", progress=0,
                                  message=reason)

    @staticmethod
    def group_status(children: list[dict]) -> str:
        """组合转存整组状态派生：**任一子任务失败 → 整组失败**（用户确认的语义）。

        优先级：全组取消 → 取消；任一失败 → 失败；任一待确认 → 待确认；
        仍有未完成 → 进行中；否则成功。
        """
        sts = [c.get("status") or "pending" for c in children]
        if not sts:
            return "pending"
        if all(s == "cancelled" for s in sts):
            return "cancelled"
        if any(s == "failed" for s in sts):
            return "failed"
        if any(s == "needs_confirm" for s in sts):
            return "needs_confirm"
        if any(s in ("pending", "running") for s in sts):
            return "running"
        if any(s == "cancelled" for s in sts):
            return "cancelled"
        return "success"

    def _progress(self, task_id: str, p: int, m: str, detail: dict | None = None) -> None:
        """进度回调：先查取消信号（命中即抛出，中止后续阶段），再写进度。

        写库前经过单调护栏：进度百分比只增不减（消息始终更新）。适配器里
        偶发的回调倒序（例如先报 40% 再报 35%）不会让前端进度条回退。

        detail 用于承载结构化进度明细（如跨平台中转的下载/上传字节数），
        落库到 progress_detail，供「转存记录」分两行展示。
        """
        if self._is_cancelled(task_id):
            raise TaskCancelled()
        try:
            p_int = max(0, min(100, int(p)))
        except (TypeError, ValueError):
            p_int = 0
        with self._peak_lock:
            peak = self._peak_progress.get(task_id, 0)
            if p_int < peak:
                p_int = peak
            elif p_int > peak:
                self._peak_progress[task_id] = p_int
        fields = {"progress": p_int, "message": m}
        if isinstance(detail, dict):
            fields["progress_detail"] = detail
        self._safe_update(task_id, **fields)

    def _notify_done(self, task_id: str, share: ShareLink, target: str,
                     result: TransferResult) -> None:
        """任务成功落终态后推送（可选，best-effort）。

        仅当配置 BARK_URL 时启用：GET <bark>/<标题>/<正文> 推送到手机
        （长转存最多 10 分钟，人不在电脑前也能收到）。
        """
        url = getattr(self.settings, "bark_url", "")
        if not url:
            return
        try:
            src = PLATFORM_LABELS.get(share.platform, share.platform)
            dst = PLATFORM_LABELS.get(target, target) if target != "local" else "本机高速"
            title = "转存完成" if result.new_url else "转存完成（无链接）"
            body = f"{src} → {dst}"
            if result.new_url:
                body += f"\n{result.new_url}"
            if result.password:
                body += f"\n提取码：{result.password}"
            push = f"{url.rstrip('/')}/{urlquote(title, safe='')}/{urlquote(body, safe='')}"
            with httpx.Client(trust_env=False, timeout=5) as c:
                c.get(push)
        except Exception:
            pass  # 通知失败不影响任务结果

    def _direct(self, share: ShareLink, target: str, password: str | None,
                progress) -> object:
        """同平台直转。

        每个任务独立 AsyncClient + 独立事件循环（asyncio.run）：
        httpx.AsyncClient 不允许跨事件循环复用，旧实现共享 client
        在多 worker 并发时会污染连接池（审核 P1-2）。
        """
        async def _job():
            async with new_client() as c:
                registry = self._build_registry(c)
                return await registry[target].transfer(share, password, progress)

        return asyncio.run(_job())

    # ---- 跨平台流式中转（阶段 3） ----

    def _relay(self, task_id: str, share: ShareLink, target: str, password: str | None,
               progress) -> object:
        """跨平台中转：源盘 save → OpenList fs/copy → 目标盘 locate → share。

        数据路径：源网盘 CDN → 家里电脑（OpenList 流式，不落盘）→ 目标网盘 CDN。
        目标为 local 时第 ② 步落盘本机、第 ③ 步生成 /dl 下载链接。
        前置条件：OPENLIST_TOKEN 已配置、双端存储已在 OpenList 挂载
        （挂载目录见 OPENLIST_MOUNT_* 配置）。
        """
        mounts = self.settings.openlist_mounts
        missing = [p for p in (share.platform, target) if not mounts.get(p)]
        if not self.settings.openlist_token:
            raise AdapterError(
                "跨平台中转需要配置 OPENLIST_TOKEN（OpenList 管理 token），"
                "并在 OpenList 中挂载双端存储")
        if missing:
            raise AdapterError(
                f"OpenList 未配置 {'/'.join(missing)} 的挂载目录（OPENLIST_MOUNT_*）")

        def scaled(lo: int, hi: int):
            """把适配器 0~100 的内部进度映射到全任务的 [lo, hi] 区间。"""
            def _p(p: int, m: str, detail: dict | None = None):
                progress(lo + (hi - lo) * max(0, min(p, 100)) // 100, m,
                         detail=detail)
            return _p

        # ① 源盘：他人分享 → 自己网盘
        # resume（用户已确认「跳过超限继续」）：源盘文件此前已保存过，复用已落库的
        # src_ref 跳过 save，避免对同一个分享重复转存；否则正常 save 并把 src_ref 落库，
        # 供可能的「超限暂停 → 继续」resume 使用。
        task0 = self.store.get(task_id)
        confirmed = bool(task0.get("confirmed_skip")) if task0 else False
        # reuse_src：组合转存里「组内已有子任务保存过源文件」→ 直接复用，避免同一份
        # 分享在源盘被重复转存 N 份（配额浪费 + 同名冲突）。由 _run_group 广播写库。
        reuse_src = bool(task0.get("reuse_src")) if task0 else False
        saved_src = task0.get("src_ref") if task0 else None

        async def _save():
            async with new_client() as c:
                registry = self._build_registry(c)
                return await registry[share.platform].save(share, password, scaled(2, 30))

        if (confirmed or reuse_src) and saved_src:
            src_ref = SavedRef(platform=share.platform,
                               names=list(saved_src.get("names", [])),
                               mount_dir=saved_src.get("mount_dir", "") or "")
            progress(2, "复用已保存的源文件")
        else:
            progress(2, "转存到源网盘")
            src_ref = asyncio.run(_save())
            self._safe_update(task_id, src_ref={
                "names": list(src_ref.names),
                "mount_dir": src_ref.mount_dir or "",
            })

        # ② OpenList：源盘挂载目录 → 目标盘「转存」目录。
        # OpenList 的跨驱动复制是「Link(源) → SeekableStream → Put(目标)」的**流式管道**；
        # 当目标驱动需要可寻址的整份文件（如分片上传）而源链只给得出顺序流时，
        # 才会退化成「整份落 temp 再上传」——详见 openlist.py 模块说明。
        progress(32, "经 OpenList 中转复制")
        src_dir = (mounts[share.platform].rstrip("/")
                   + (src_ref.mount_dir if src_ref.mount_dir.startswith("/") else
                      ("/" + src_ref.mount_dir if src_ref.mount_dir else "")))
        dst_dir = (mounts[target].rstrip("/") + "/" + self.settings.transfer_dir)
        ol = self._openlist_factory()
        temp_dir = getattr(self.settings, "openlist_temp_dir", "") or ""
        stall_min = float(getattr(self.settings, "relay_stall_min", 15) or 15)
        max_hours = float(getattr(self.settings, "relay_max_hours", 24) or 24)
        with httpx.Client(trust_env=False, timeout=60) as sync_c:
            ol.ensure_dir(sync_c, dst_dir)
            # 幂等预检：重复转存同一分享时跳过目标盘已有的同名文件，
            # 避免 fs/copy 因 file exists 整单失败。返回 {名字: 大小} 以便内容校验。
            existing = ol.existing_names(sync_c, dst_dir, src_ref.names)
            if existing:
                bad = self._existing_size_mismatch(ol, sync_c, src_dir, existing)
                if bad:
                    raise AdapterError(
                        f"目标盘「{self.settings.transfer_dir}」已有同名但大小不同的文件"
                        f"（{'、'.join(sorted(bad)[:3])}）——它很可能是另一个文件。"
                        f"为免分享到错误内容，请先在目标盘删除同名文件后重试")
            to_copy = [n for n in src_ref.names if n not in existing]
            if to_copy:
                # 先枚举源树：拿到总字节 / 最大单文件（进度计量与磁盘校验都要用）。
                # 注意：只统计 to_copy 命中的子树——src_dir 可能只是「包含待复制项的
                # 父目录」（夸克自分享降级场景），整目录递归会把无关文件算进总量。
                meter = RelayMeter(ol, sync_c, src_dir, dst_dir,
                                   temp_dir=temp_dir, names=to_copy,
                                   src_mount=mounts[share.platform],
                                   dst_mount=mounts[target])
                meter.scan_source()
                self._check_relay_disk(meter)
                # 单文件超限：目标盘单文件超过上限时，把超限顶层项挑出来——不整单失败，
                # 而是暂停并就「哪些文件超限」向用户确认；用户「继续」则跳过超限项转存其余。
                over_names = self._oversized_top_names(meter, src_ref.names, target)
                if over_names and not confirmed:
                    cap = self._cap_gb(target)
                    over_files = [
                        {"name": rel, "size": sz}
                        for rel, sz in meter.files.items()
                        if sz > self._cap_bytes(target)
                    ][:50]
                    self.store.update(
                        task_id, status="needs_confirm", progress=30,
                        message=(f"部分文件超过目标盘「{PLATFORM_LABELS.get(target, target)}」"
                                  f"单文件上限约 {cap:g}GB，需确认是否跳过超限继续"),
                        over_files=over_files,
                        cap_gb=cap)
                    raise NeedsConfirm()
                # ⚠️ 必须在**已滤掉 existing 的** to_copy 上再滤超限项。若从 src_ref.names
                # 重算，会把「目标盘已存在」的项重新塞回来 → fs/copy 撞 file exists
                # 整单失败（正是上面幂等预检要防的场景）。
                to_copy = [n for n in to_copy if n not in over_names]
                if not to_copy:
                    # 待复制项被全部跳过。此处只可能是「全被超限跳过」——existing 全命中
                    # 的场景在上面的 else 分支就收尾了。
                    cap = self._cap_gb(target)
                    msg = (f"待转存的 {len(over_names)} 项均超过目标盘「"
                           f"{PLATFORM_LABELS.get(target, target)}」单文件上限"
                           f"（约 {cap:g}GB），已按你的确认跳过转存")
                    self._safe_update(task_id, status="success", progress=100,
                                      message=msg, result_files=[])
                    return TransferResult(new_url="", message=msg, files_saved=0)
                # 登记「本任务进入复制/轮询阶段」：RelayMeter 据此判断 temp 目录能否
                # 归属到单个任务（并发 >1 时不计 temp，见 relay_meter 类注释）。
                RelayMeter.enter()
                try:
                    task_ids = ol.copy(sync_c, src_dir, dst_dir, to_copy)
                    progress(33, "复制任务已提交")
                    try:
                        ol.wait_copy(sync_c, task_ids, src_dir, dst_dir, to_copy,
                                     scaled(33, 93), meter=meter,
                                     abort_check=self._cancel_guard(task_id),
                                     stall_min=stall_min, max_hours=max_hours,
                                     temp_dir=temp_dir,
                                     src_mount=mounts[share.platform],
                                     dst_mount=mounts[target])
                    except Exception:
                        # 失败/取消/超时/停滞：把 OpenList 侧的复制任务一并取消。
                        # 否则会出现「任务记录已停、后台还在跑」并持续吃磁盘（事故根因之一）。
                        n = self._abort_relay(ol, sync_c, dst_dir, task_ids,
                                              mount=mounts[target])
                        if n:
                            print(f"[relay] 已取消 {n} 个残留 OpenList 复制任务")
                        raise
                finally:
                    RelayMeter.leave()
            else:
                progress(38, "目标盘已有同名文件，跳过复制")

        # ③ 目标盘：在「转存」目录定位复制后的文件 → 生成新分享
        async def _finish():
            async with new_client() as c:
                registry = self._build_registry(c)
                ref = await registry[target].locate(src_ref.names,
                                                    self.settings.transfer_dir,
                                                    scaled(93, 97))
                ref.task_id = task_id   # local 适配器据此生成 /dl/ 链接
                # 下载凭证（分享链接只带 dl_token，不泄露 task_id）
                ref.dl_token = (self.store.get(task_id) or {}).get("dl_token", "")
                try:
                    return await registry[target].share(ref, scaled(97, 100))
                except CapabilityError as e:
                    # 目标平台官方接口不支持程序化创建分享（如阿里个人开放 API）。
                    # 文件已实际转存到用户网盘 → 降级为「转存成功、无分享链接」。
                    return TransferResult(
                        new_url="", password=None, files_saved=len(ref.refs),
                        message=str(e))

        return asyncio.run(_finish())
