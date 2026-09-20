"""跨平台中转的计量与完成判定测试（2026-09 大文件事故回归）。

重点回归三件事：
1) 「假成功」：OpenList 对目录的 copy 会拆成每节点一个任务，且**目录级任务在子任务
   入队完成时就置 done**——只看 fs/copy 返回的 task id 会误判成功；
2) 「总量虚高」：src_dir 只是「包含待复制项的父目录」（夸克自分享降级场景），
   整目录递归会把无关文件算进总量——实测把 443MB 的分享算成 2.76TB；
3) 「路径前缀」：任务名里是**挂载内相对路径**（`/微信分享/游戏/…`），而我们的
   src_dir/dst_dir 带挂载前缀（`/quark/微信分享/游戏`）——路径比较必须宽容，
   否则复制完成后 done_files 恒为 0、任务永远卡在 0%。

测试数据刻意保持**生产形态**：src_dir 是「包含待复制项的父目录」，names 只含其中
一项，任务名用挂载内相对路径。
"""
import pytest

from adapters.base import AdapterError
from openlist import OpenListClient, parse_task_name
from relay_meter import RelayMeter

SM, DM = "/quark", "/baidu"                 # 挂载名
SRC = "/quark/微信分享/游戏"                 # 带挂载前缀的源目录（= 父目录）
SP = "/微信分享/游戏"                        # 任务名里的源侧相对路径
DST = "/baidu/转存"
DP = "/转存"                                # 任务名里的目标侧相对路径


# ---------------- 任务名解析 ----------------

def test_parse_task_name():
    n = "copy [/quark](/微信分享/游戏/a/b.txt) to [/baidu](/转存/a/b.txt)"
    d = parse_task_name(n)
    assert d["sm"] == "/quark"
    assert d["sp"] == "/微信分享/游戏/a/b.txt"   # 挂载内相对路径（不含 /quark）
    assert d["dm"] == "/baidu"
    assert d["dp"] == "/转存/a/b.txt"
    assert parse_task_name("not a copy task") is None
    assert parse_task_name("") is None


# ---------------- 计量器 ----------------

def _f(name, size):
    return {"name": name, "is_dir": False, "size": size}


def _d(name):
    return {"name": name, "is_dir": True, "size": 0}


class StubOL:
    """最小 OpenList 替身：按「路径 → 条目列表」返回，任务表可注入。"""

    def __init__(self, trees, undone=(), done=()):
        self.trees = {(k or "/").rstrip("/") or "/": v for k, v in trees.items()}
        self._undone, self._done = list(undone), list(done)

    def list_dir(self, c, path, refresh=False, per_page=500):
        key = (path or "/").rstrip("/") or "/"
        return [dict(x) for x in self.trees.get(key, [])]

    def copy_tasks(self, c, which):
        return self._done if which == "done" else self._undone

    def cancel(self, c, tid):
        return True

    def cancel_under(self, c, dst, mount=""):
        return 0


def _task(sp, dp, state=2, progress=None):
    """真实形态的复制任务名：括号里是挂载内相对路径。"""
    t = {"id": "t-" + sp, "state": state,
         "name": f"copy [{SM}]({sp}) to [{DM}]({dp})"}
    if progress is not None:
        t["progress"] = progress
    return t


def _meter(ol, names=("pkg",)):
    return RelayMeter(ol, None, SRC, DST, names=list(names),
                      src_mount=SM, dst_mount=DM)


TREES = {
    SRC: [_d("pkg"), _f("无关的大游戏.bin", 99_000_000_000)],
    f"{SRC}/pkg": [_f("a.bin", 100), _f("b.bin", 300), _d("sub")],
    f"{SRC}/pkg/sub": [_f("c.bin", 200)],
    DST: [_d("pkg")],
    f"{DST}/pkg": [_f("a.bin", 100), _f("b.bin", 300), _d("sub")],
    f"{DST}/pkg/sub": [_f("c.bin", 200)],
}


def test_scan_source_only_counts_named_items():
    """父目录里的无关文件绝不能被计入总量（回归 443MB → 2.76TB）。"""
    m = _meter(StubOL(TREES))
    m.scan_source()
    assert m.total == 600                     # 100 + 300 + 200
    assert m.max_file == 300
    assert set(m.files) == {"pkg/a.bin", "pkg/b.bin", "pkg/sub/c.bin"}


def test_scan_source_handles_file_item():
    """names 里是「文件」时也要按大小计入（不是只有目录能复制）。"""
    m = _meter(StubOL({SRC: [_f("solo.bin", 42)]}), names=["solo.bin"])
    m.scan_source()
    assert m.files == {"solo.bin": 42} and m.total == 42


def test_poll_matches_mount_relative_task_names():
    """任务名是挂载内相对路径 → 仍要认出「自己的任务」（回归卡死）。"""
    ol = StubOL(TREES,
                undone=[_task(f"{SP}/pkg/b.bin", f"{DP}/pkg", state=0)],
                done=[_task(f"{SP}/pkg/a.bin", f"{DP}/pkg", state=2)])
    m = _meter(ol)
    m.scan_source()
    st = m.poll()
    assert st.total_files == 3
    assert st.done_files == 1 and st.done_bytes == 100
    assert st.pending == 1                    # 未完成的那条也被认领
    assert st.saw_ours


def test_poll_counts_directory_level_tasks_as_ours_but_not_as_files():
    """目录级任务（dp=目标根、sp=待复制项本身）算「自己人」但不计入文件数。"""
    ol = StubOL(TREES, done=[_task(f"{SP}/pkg", DP, state=2)])
    m = _meter(ol)
    m.scan_source()
    st = m.poll()
    assert st.pending == 0 and st.done_files == 0 and st.saw_ours


def test_poll_ignores_unrelated_tasks():
    """同一源目录下复制**别的子项**、或别的挂载的任务，都不得混进来。"""
    ol = StubOL(TREES, undone=[
        _task(f"{SP}/别的游戏/x.bin", f"{DP}/别的游戏", state=0),
        {"id": "z", "state": 0,
         "name": "copy [/uc](/转存/x.bin) to [/baidu](/转存/x.bin)"},
    ])
    m = _meter(ol)
    m.scan_source()
    st = m.poll()
    assert st.pending == 0 and not st.saw_ours


def test_poll_separates_download_and_upload_bytes():
    """下载/上传分两路计量：上传量含在途文件的 progress 折算（回归「假 100%」）。

    旧实现把本机 temp 当成「已传输」→ 下载一结束就显示 100%，上传期间进度冻结。
    现在：下载量 = 已落地 + temp；上传量 = 已落地 + Σ(在途 × progress%)。
    """
    trees = {
        SRC: [_d("pkg")],
        f"{SRC}/pkg": [_f("a.bin", 100), _f("b.bin", 300)],
        DST: [_d("pkg")],
        f"{DST}/pkg": [_f("a.bin", 100)],
    }
    ol = StubOL(trees,
                undone=[_task(f"{SP}/pkg/b.bin", f"{DP}/pkg", state=1, progress=50)],
                done=[_task(f"{SP}/pkg/a.bin", f"{DP}/pkg", state=2)])
    m = _meter(ol)
    m.scan_source()
    st = m.poll()
    assert st.landed_bytes == 100 and st.landed_files == 1     # a.bin 已落地
    assert st.ul_bytes == 100 + 150                            # + b.bin 的一半
    # 下载量 ≥ 上传量（已上传的字节必然已先下载）：100 已落地 + b.bin 至少已下了一半 150
    assert st.dl_bytes == 250
    assert st.pending == 1
    # 进度绝不能因为「在途」就被算成 100%
    assert st.ul_bytes < st.total_bytes


def test_streaming_upload_never_exceeds_download():
    """流式直传（不落 temp）时下载量由上传量反推，杜绝「下载 0% / 上传 23%」。

    2026-09-18 实测：8.1MB 百度→夸克是单分片直传，根本不落 temp，旧公式
    「下载 = 已落地 + temp」算出 0 B，而上传已到 23% —— 反物理。
    现在下载量 = max(已落地 + temp, 上传量)。
    """
    ol = StubOL({SRC: [_f("solo.bin", 1000)], DST: []},
                undone=[_task(f"{SP}/solo.bin", DP, state=1, progress=23)])
    m = _meter(ol, names=["solo.bin"])
    m.scan_source()
    st = m.poll()
    assert st.temp_bytes == 0          # 流式直传：没有 temp 文件可量
    assert st.ul_bytes == 230          # 1000 × 23%
    assert st.dl_bytes == 230          # 被上传量顶起来，不再显示 0 B / 0%
    assert st.dl_bytes >= st.ul_bytes  # 铁律：下载永远 ≥ 上传


def test_verify_destination_detects_missing_and_size_mismatch():
    m = _meter(StubOL({**TREES, f"{DST}/pkg": [_f("a.bin", 99)]}))
    m.scan_source()
    ok, why = m.verify_destination()
    assert not ok and ("大小不符" in why or "缺少" in why)

    m2 = _meter(StubOL({**TREES, f"{DST}/pkg": [], f"{DST}/pkg/sub": []}))
    m2.scan_source()
    ok2, why2 = m2.verify_destination()
    assert not ok2 and "缺少" in why2

    m3 = _meter(StubOL(TREES))
    m3.scan_source()
    assert m3.verify_destination()[0]


# ---------------- wait_copy 完成判定 ----------------

def _run_wait(ol, names, **kw):
    seen = []
    kw.setdefault("poll_interval", 0.01)
    OpenListClient.wait_copy(ol, None, ["t1"], SRC, DST, list(names),
                             lambda p, m, detail=None: seen.append((p, m)),
                             src_mount=SM, dst_mount=DM, **kw)
    return seen


def test_wait_copy_completes_with_mount_relative_names():
    """生产形态端到端：任务名挂载内相对路径 + 目录级任务 + 目的地齐全 → 完成。"""
    done = [_task(f"{SP}/pkg", DP, state=2),
            _task(f"{SP}/pkg/a.bin", f"{DP}/pkg", state=2),
            _task(f"{SP}/pkg/b.bin", f"{DP}/pkg", state=2),
            _task(f"{SP}/pkg/sub/c.bin", f"{DP}/pkg/sub", state=2)]
    seen = _run_wait(StubOL(TREES, done=done), ["pkg"])
    assert seen[-1][0] == 100
    assert "复制完成" in seen[-1][1]


def test_wait_copy_refuses_fake_success_when_file_task_pending():
    """目录级任务已 done，但文件任务仍 pending、目标也确实缺文件 → 绝不放行。

    这正是 2026-09 事故形态：目录级任务「子任务入队完成」即置 done，
    真身还在排队；只看这个 id 会假成功并立即生成**内容不全**的分享链接。
    """
    trees = {**TREES, f"{DST}/pkg": [], f"{DST}/pkg/sub": []}
    done = [_task(f"{SP}/pkg", DP, state=2)]
    undone = [_task(f"{SP}/pkg/a.bin", f"{DP}/pkg", state=0)]
    with pytest.raises(AdapterError, match="停滞"):
        _run_wait(StubOL(trees, undone=undone, done=done), ["pkg"],
                  stall_min=1 / 60)


def test_wait_copy_refuses_when_destination_incomplete():
    """任务全部 done 但目标缺文件 → 校验不过，不得成功。"""
    trees = {**TREES, f"{DST}/pkg": [], f"{DST}/pkg/sub": []}
    done = [_task(f"{SP}/pkg/a.bin", f"{DP}/pkg", state=2),
            _task(f"{SP}/pkg/b.bin", f"{DP}/pkg", state=2),
            _task(f"{SP}/pkg/sub/c.bin", f"{DP}/pkg/sub", state=2)]
    with pytest.raises(AdapterError, match="停滞"):
        _run_wait(StubOL(trees, done=done), ["pkg"], stall_min=1 / 60)


def test_wait_copy_abort_check_propagates():
    class Aborted(Exception):
        pass

    def _abort():
        raise Aborted()

    with pytest.raises(Aborted):
        _run_wait(StubOL(TREES), ["pkg"], abort_check=_abort)


def test_wait_copy_reports_two_lines():
    """进度消息必须是两行：第一行下载、第二行上传（前端 pre-line 渲染）。"""
    done = [_task(f"{SP}/pkg/a.bin", f"{DP}/pkg", state=2),
            _task(f"{SP}/pkg/b.bin", f"{DP}/pkg", state=2),
            _task(f"{SP}/pkg/sub/c.bin", f"{DP}/pkg/sub", state=2)]
    seen = _run_wait(StubOL(TREES, done=done), ["pkg"])
    prog_msgs = [m for _, m in seen if m.startswith("下载")]
    assert prog_msgs, seen          # 必须出现过「下载/上传」两行进度
    line1, line2 = prog_msgs[-1].split("\n", 1)
    assert line1.startswith("下载 ") and "/" in line1      # 形如「下载 600 B/600 B（100%）」
    assert line2.startswith("上传 ") and "/" in line2


class UploadOnlyStub:
    """模拟「下载已结束、上传仍在推进」：progress 每轮递增，到 100% 时文件落地。

    回归 2026-09-17 事故：旧实现的停滞检测只看下载量，上传大文件期间下载静止
    → 15 分钟就误判「停滞」并取消任务（差点毁掉 21.5GB 已下载数据）。
    """

    def __init__(self, size: int, step: int = 20):
        self.size = size
        self.step = step
        self.prog = 0

    def list_dir(self, c, path, refresh=False, per_page=500):
        p = (path or "").rstrip("/")
        if p == SRC:
            return [_f("solo.bin", self.size)]
        if p == DST:
            return [_f("solo.bin", self.size)] if self.prog >= 100 else []
        return []

    def copy_tasks(self, c, which):
        if which == "undone":          # 每轮只推进一步（poll 会先问 undone 再问 done）
            self.prog = min(100, self.prog + self.step)
        t = _task(f"{SP}/solo.bin", DP, state=1, progress=self.prog)
        if self.prog >= 100:
            t["state"] = 2
            return [t] if which == "done" else []
        return [] if which == "done" else [t]

    def cancel(self, c, tid):
        return True

    def cancel_under(self, c, dst, mount=""):
        return 0


def test_wait_copy_survives_stall_when_upload_still_progressing():
    """下载早已静止、上传仍在推进 → 不得判停滞（双通道检测）。"""
    ol = UploadOnlyStub(1000, step=20)
    seen = _run_wait(ol, ["solo.bin"], stall_min=1 / 60, poll_interval=0.01)
    assert seen[-1][0] == 100
    assert "复制完成" in seen[-1][1]
