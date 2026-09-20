"""OpenList 客户端测试：任务表数组/字典双形态 + 数字/字符串 state 枚举 +
「目的地校验式」完成判定（回归 2026-09 假成功 / 总量虚高 / 卡死 三起事故）。

**任务名务必写成真实格式**：括号里是「挂载内相对路径」（`/转存/a.txt`），
而传给 wait_copy 的 src_dir/dst_dir 带挂载前缀（`/quark/转存`、`/baidu/转存`）。
旧测试把完整路径塞进括号，正好掩盖了「路径前缀对不上导致完成判定永远认不出
自己任务」的线上卡死事故——所以这里刻意保持真实形态。

注意：OpenListClient 走同步 httpx.Client（编排器线程内调用）。
"""
import json

import httpx
import pytest

from adapters.base import AdapterError
from openlist import OpenListClient, parse_task_name

SM, DM = "/quark", "/baidu"
SRC = "/quark/转存"        # 带挂载前缀的源目录
DST = "/baidu/转存"


def mock_client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)


def _f(name, size):
    return {"name": name, "is_dir": False, "size": size}


def _d(name):
    return {"name": name, "is_dir": True, "size": 0}


def make_handler(trees, undone=(), done=(), wrap_dict=False):
    """构造假 OpenList：fs/list 按路径查表返回；任务表可注入（数组或字典形态）。"""
    def handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if path == "/api/fs/list":
            body = json.loads(req.content or b"{}")
            key = (body.get("path") or "").rstrip("/")
            return httpx.Response(200, json={"code": 200,
                                             "data": {"content": trees.get(key, [])}})
        if path.endswith("/done") or path.endswith("/undone"):
            rows = list(done) if path.endswith("/done") else list(undone)
            data = {"tasks": rows} if wrap_dict else rows
            return httpx.Response(200, json={"code": 200, "data": data})
        return httpx.Response(200, json={"code": 200, "data": {}})
    return handler


def _task(sp, dp, state=2, **extra):
    """真实形态：括号里是挂载内相对路径（不含挂载名）。"""
    d = {"id": "t-" + sp, "state": state,
         "name": f"copy [{SM}]({sp}) to [{DM}]({dp})"}
    d.update(extra)
    return d


TRANSFER_TREE = {SRC: [_f("a.txt", 10)]}


def test_parse_task_name_roundtrip():
    d = parse_task_name(f"copy [{SM}](/转存/a.txt) to [{DM}](/转存/a.txt)")
    assert d == {"sm": SM, "sp": "/转存/a.txt", "dm": DM, "dp": "/转存/a.txt"}


def test_wait_copy_accepts_array_data_and_numeric_state():
    """任务表 data 直接是数组、state 是数字 2（成功）→ 目的地校验通过即完成。

    回归：线上 quark→baidu 中转因对数组调 .get() 报 'list' object has no
    attribute 'get'，且数字 state 不被识别导致假超时。
    """
    h = make_handler({SRC: [_f("a.txt", 10)], DST: [_f("a.txt", 10)]},
                     done=[_task("/转存/a.txt", "/转存", state=2)])
    ol = OpenListClient("http://x", "tok")
    c = mock_client(h)
    seen = []
    ol.wait_copy(c, ["t"], SRC, DST, ["a.txt"],
                 lambda p, m, detail=None: seen.append((p, m)), poll_interval=0.01,
                 src_mount=SM, dst_mount=DM)
    assert seen[-1][0] == 100
    c.close()


def test_wait_copy_numeric_failed_state_raises():
    """done 表里非成功 state（如 7）且文件未落地 → 立即报错（回归空文件场景）。"""
    h = make_handler({SRC: [_f("a.txt", 10)], DST: []},
                     done=[_task("/转存/a.txt", "/转存", state=7, status="uploading",
                                 error="empty files are not allowed by baidu netdisk")])
    ol = OpenListClient("http://x", "tok")
    c = mock_client(h)
    with pytest.raises(AdapterError, match="不支持保存空文件"):
        ol.wait_copy(c, ["t"], SRC, DST, ["a.txt"],
                     lambda p, m, detail=None: None, poll_interval=0.01,
                     src_mount=SM, dst_mount=DM)
    c.close()


def test_wait_copy_dict_shaped_data_also_supported():
    """字典形态 {"tasks": [...]} + 字符串 state（"succeeded"）同样兼容。"""
    h = make_handler({SRC: [_f("a.txt", 10)], DST: [_f("a.txt", 10)]},
                     done=[_task("/转存/a.txt", "/转存", state="succeeded")],
                     wrap_dict=True)
    ol = OpenListClient("http://x", "tok")
    c = mock_client(h)
    seen = []
    ol.wait_copy(c, ["t"], SRC, DST, ["a.txt"],
                 lambda p, m, detail=None: seen.append((p, m)), poll_interval=0.01,
                 src_mount=SM, dst_mount=DM)
    assert seen[-1][0] == 100
    c.close()


def test_wait_copy_reports_progress_detail():
    """每轮轮询都向 progress 透传结构化明细（relay/done/total），供记录页两行展示。"""
    h = make_handler({SRC: [_f("a.txt", 10)], DST: [_f("a.txt", 10)]},
                     done=[_task("/转存/a.txt", "/转存", state=2)])
    ol = OpenListClient("http://x", "tok")
    c = mock_client(h)
    seen = []
    ol.wait_copy(c, ["t"], SRC, DST, ["a.txt"],
                 lambda p, m, detail=None: seen.append((p, m, detail)),
                 poll_interval=0.01, src_mount=SM, dst_mount=DM)
    # 每轮都带了明细，且完成那轮 dl_done == ul_done == total
    assert seen, "应有进度回调"
    assert all(d["relay"] is True and d["total"] == 10 for _, _, d in seen)
    last = seen[-1][2]
    assert last["dl_done"] == 10 and last["ul_done"] == 10
    c.close()
def test_wait_copy_refuses_when_destination_not_verified():
    """任务全部成功但目标盘缺文件（目录级任务假成功）→ 判停滞并报错，绝不放行。"""
    h = make_handler({SRC: [_f("a.txt", 10)], DST: []},
                     done=[_task("/转存/a.txt", "/转存", state=2)])
    ol = OpenListClient("http://x", "tok")
    c = mock_client(h)
    with pytest.raises(AdapterError, match="停滞|校验"):
        ol.wait_copy(c, ["t"], SRC, DST, ["a.txt"],
                     lambda p, m, detail=None: None, poll_interval=0.01, stall_min=1 / 60,
                     src_mount=SM, dst_mount=DM)
    c.close()


def test_wait_copy_only_requires_the_named_subset():
    """src_dir 是父目录、只复制其中一个子项时，完成判定不得牵扯父目录里的其他文件。

    回归 2026-09「443MB 被算成 2.76TB」：父目录里躺着用户全部游戏，若整目录递归
    统计，总量虚高且完成判定永远等不到父目录的无关文件落地。
    """
    src_tree = [ _d("pkg"), _f("无关的大游戏.bin", 99_000_000_000) ]
    trees = {
        SRC: src_tree,
        f"{SRC}/pkg": [_f("a.txt", 10)],
        DST: [_d("pkg")],
        f"{DST}/pkg": [_f("a.txt", 10)],
    }
    done = [_task("/转存/pkg", "/转存", state=2),                 # 目录级任务
            _task("/转存/pkg/a.txt", "/转存/pkg", state=2)]       # 文件任务
    h = make_handler(trees, done=done)
    ol = OpenListClient("http://x", "tok")
    c = mock_client(h)
    seen = []
    ol.wait_copy(c, ["t"], SRC, DST, ["pkg"],
                 lambda p, m, detail=None: seen.append((p, m)), poll_interval=0.01,
                 src_mount=SM, dst_mount=DM)
    assert seen[-1][0] == 100
    assert "43" in seen[-1][1] or "B" in seen[-1][1]     # 总量是 10 B 级别，不是 TB
    assert "TB" not in "".join(m for _, m in seen)
    c.close()


def test_wait_copy_dead_download_watchdog():
    """回归 2026-09-18 死循环：百度单文件超 8GB 被服务端拒绝后，OpenList 反复
    「整文件重新下载→重新上传」，下载量从峰值断崖回退。累计 3 次回退即判定无限循环并
    主动中止（抛 AdapterError），不再把中转盘写满还永远跑不完。"""
    from openlist import OpenListClient

    ol = OpenListClient("http://x", "tok")

    class Stat:
        def __init__(self, dl):
            self.dl_bytes = dl
            self.ul_bytes = 0
            self.pending = 1
            self.total_files = 1
            self.landed_files = 0
            self.saw_ours = False
            self.fail_reason = None
            self.temp_bytes = 0

    class FakeMeter:
        total = 1000
        _seq = [100, 0, 100, 0, 100, 0, 100]   # 下载量反复从峰值回退到 0
        _i = 0
        def poll(self):
            v = FakeMeter._seq[FakeMeter._i % len(FakeMeter._seq)]
            FakeMeter._i += 1
            return Stat(v)
        def verify_destination(self):
            return (False, "not done")

    c = mock_client(make_handler({SRC: [_f("a.txt", 10)], DST: [_f("a.txt", 10)]},
                                 done=[_task("/转存/a.txt", "/转存", state=2)]))
    with pytest.raises(AdapterError, match="反复从头重试下载"):
        ol.wait_copy(c, ["t"], SRC, DST, ["a.txt"],
                     lambda p, m, detail=None: None, poll_interval=0.01,
                     meter=FakeMeter(), src_mount=SM, dst_mount=DM)
    c.close()


# ---------- 目录列举的翻页与同名内容校验（2026-09-19 修复） ----------

def _paged_handler(pages: dict[int, list]) -> object:
    """按请求里的 page 返回对应页内容的假 OpenList。"""
    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content or b"{}")
        try:
            page = int(body.get("page") or 1)
        except (TypeError, ValueError):
            page = 1
        return httpx.Response(200, json={"code": 200,
                                         "data": {"content": pages.get(page, [])}})
    return handler


def test_existing_names_paginates_and_reports_sizes():
    """幂等预检必须**翻页**：转存目录超过一页时不能漏判。

    旧实现固定 page=1&per_page=200 —— 目录超过 200 条就查不到同名文件，
    幂等预检失效，重复转存会撞 fs/copy 的 file exists 整单失败。
    同时要返回 {名字: 大小}，供上层识别「同名但不同内容」。
    """
    page1 = [_f(f"f{i}.bin", i) for i in range(500)]      # 满页 → 必须继续翻
    page2 = [_f("want.bin", 1234)]
    ol = OpenListClient("http://x", "tok")
    with mock_client(_paged_handler({1: page1, 2: page2})) as c:
        got = ol.existing_names(c, DST, ["want.bin", "missing.bin"])
    assert got == {"want.bin": 1234}          # 第二页的命中没被漏掉


def test_list_dir_paginates_all_pages():
    """list_dir 也必须翻页 —— 漏条目会让「目的地完整性校验」假通过（最危险的一类）。"""
    pages = {1: [_f(f"a{i}", 1) for i in range(500)], 2: [_f("last.bin", 9)]}
    ol = OpenListClient("http://x", "tok")
    with mock_client(_paged_handler(pages)) as c:
        items = ol.list_dir(c, DST)
    assert len(items) == 501
    assert items[-1]["name"] == "last.bin"

