"""编排器测试：用假适配器 + 假 OpenList 客户端驱动完整任务生命周期（无网络）。"""
import dataclasses
import time

import pytest

from adapters.base import (Adapter, AdapterError, AdapterNotConfigured,
                           SavedRef, TransferResult)
from db import TaskStore
from orchestrator import Orchestrator
from parser import ShareLink


@dataclasses.dataclass
class FakeSettings:
    openlist_base: str = "http://127.0.0.1:9"
    openlist_token: str = ""
    transfer_dir: str = "转存"
    local_dir: str = ""
    openlist_mounts: dict = dataclasses.field(
        default_factory=lambda: {"fake": "/fake", "other": "/other"})
    target_file_cap_gb: dict = dataclasses.field(default_factory=dict)


class FakeAdapter(Adapter):
    platform = "fake"
    label = "假盘"

    def __init__(self, behavior="ok"):
        self.behavior = behavior
        self.saved = None
        self.shared = None
        self.located = None

    def configured(self):
        return self.behavior != "unconfigured"

    async def save(self, share, password, progress):
        if self.behavior == "unconfigured":
            raise AdapterNotConfigured("未配置")
        if self.behavior == "boom":
            raise AdapterError("提取码错误")
        progress(50, "半程")
        self.saved = (share, password)
        return SavedRef(platform="fake", names=["a.txt"], mount_dir="saved",
                        refs=[{"fid": "s1"}])

    async def share(self, ref, progress):
        self.shared = ref
        return TransferResult(new_url="https://fake/s/new", password="abcd",
                              files_saved=len(ref.refs), message="完成")

    async def locate(self, names, mount_dir, progress):
        self.located = (names, mount_dir)
        return SavedRef(platform="fake", names=names, mount_dir=mount_dir,
                        refs=[{"fid": "s2"}])


class FakeOpenList:
    """OpenListClient 的替身：记录调用、模拟成功。"""

    def __init__(self, existing=(), files=None, existing_sizes=None):
        self.copied = {}
        self._existing = set(existing)   # 目标盘「转存」目录预置的同名文件
        # 假源树（扁平即可）：相对路径 -> 字节数
        self._files = dict(files) if files is not None else {"a.txt": 4}
        # 目标盘同名条目的「大小」（可与源不同，用于验证同名不同内容的拦截）
        self._existing_sizes = dict(existing_sizes or {})
        self._src = ""
        self._dst = ""

    def ensure_dir(self, c, path):
        self.copied["dst"] = path

    def existing_names(self, c, dst_dir, names, per_page=500, max_pages=20):
        self.copied["checked"] = list(names)
        return {n: int(self._existing_sizes.get(n, self._files.get(n, 0)))
                for n in names if n in self._existing}

    def list_dir(self, c, path, refresh=False, per_page=500, max_pages=20):
        # 扁平假树：任何一层都返回同样的文件列表，足以驱动枚举/校验
        return [{"name": n, "is_dir": False, "size": s} for n, s in self._files.items()]

    def copy_tasks(self, c, which):
        if which != "done":
            return []
        return [{"id": "t1", "state": 2,
                 "name": f"copy [/fake]({self._src}/{rel}) to [/other]({self._dst}/{rel})"}
                for rel in self._files]

    def copy(self, c, src_dir, dst_dir, names):
        self.copied["src_dir"] = src_dir
        self.copied["names"] = names
        self._src, self._dst = src_dir, dst_dir
        return ["t1"]

    def wait_copy(self, c, ids, src_dir, dst_dir, names, progress, **kw):
        self.copied["waited"] = True

    def cancel(self, c, tid):
        self.copied.setdefault("cancelled", []).append(tid)
        return True

    def cancel_under(self, c, dst_dir, mount=""):
        return 0


@pytest.fixture()
def make_orch(tmp_path):
    def _make(**behaviors):
        store = TaskStore(str(tmp_path / "t.db"))
        orch = Orchestrator(store, FakeSettings(), max_workers=1)
        orch._adapters = {name: FakeAdapter(b) for name, b in behaviors.items()}
        # 让每任务构建的注册表同样指向假适配器（与 _adapters 保持一致）
        orch._build_registry = lambda client: orch._adapters
        return store, orch
    return _make


def _wait_for(store, tid, statuses=("success", "failed"), timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = store.get(tid)
        if t["status"] in statuses:
            return t
        time.sleep(0.05)
    raise AssertionError("任务超时未终结")


def test_progress_is_monotonic_per_task(make_orch):
    """进度护栏：百分比只增不减，消息照常更新（回归「40% → 35% 回退」）。"""
    store, orch = make_orch(fake="ok")
    tid = store.create(source="fake", target="fake", share_url="https://x", password=None)
    seen: list[tuple[int, str]] = []
    orch._safe_update = lambda _tid, **kw: seen.append((kw.get("progress"), kw.get("message")))

    orch._progress(tid, 40, "转存中")
    orch._progress(tid, 35, "定位中")     # 适配器回调倒序
    orch._progress(tid, 60, "后半程")
    orch._progress(tid, 60, "后半程（重复）")

    assert [p for p, _ in seen] == [40, 40, 60, 60]
    assert seen[1][1] == "定位中"          # 百分比被夹住，但消息仍要更新


def test_peak_progress_released_with_task(make_orch):
    """任务结束（_clear_cancel）后释放峰值记录，避免长期运行内存泄漏。"""
    store, orch = make_orch(fake="ok")
    tid = store.create(source="fake", target="fake", share_url="https://x", password=None)
    orch._progress(tid, 50, "半程")
    assert orch._peak_progress.get(tid) == 50
    orch._clear_cancel(tid)
    assert tid not in orch._peak_progress
    # 释放后同一 id 重新从低位开始（任务复用场景不会莫名卡在高位）
    orch._safe_update = lambda _tid, **kw: None
    orch._progress(tid, 5, "重新开始")
    assert orch._peak_progress[tid] == 5


def test_progress_detail_passed_to_store(make_orch):
    """_progress(detail=...) 把结构化进度明细写进 store，供记录页两行展示。"""
    store, orch = make_orch(fake="ok")
    tid = store.create(source="fake", target="fake", share_url="https://x", password=None)
    seen: list[dict] = []
    orch._safe_update = lambda _tid, **kw: seen.append(dict(kw))
    detail = {"relay": True, "dl_done": 23_000_000_000,
              "ul_done": 22_000_000_000, "total": 23_080_000_000, "total_files": 1}
    orch._progress(tid, 60, "中转中", detail=detail)
    assert any(d.get("progress_detail") == detail for d in seen)
    # 不带 detail 的回调不应把 progress_detail 抹掉（_safe_update 只传存在的字段）
    orch._progress(tid, 80, "继续")
    assert all("progress_detail" not in d for d in seen[1:])


def test_same_platform_success(make_orch):
    store, orch = make_orch(fake="ok")
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="fake", share_url="https://x", password="pw")
    orch.submit(tid, share, "fake", "pw")
    t = _wait_for(store, tid)
    assert t["status"] == "success"
    assert t["result_url"] == "https://fake/s/new"
    assert t["result_pwd"] == "abcd"
    # save 收到了提取码，share 收到了 save 的产物
    ad = orch._adapters["fake"]
    assert ad.saved[1] == "pw"
    assert ad.shared.refs[0]["fid"] == "s1"


def test_adapter_business_error_marks_failed(make_orch):
    store, orch = make_orch(fake="boom")
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="fake", share_url="https://x", password=None)
    orch.submit(tid, share, "fake", None)
    t = _wait_for(store, tid)
    assert t["status"] == "failed"
    assert "提取码错误" in t["error"]


def test_unconfigured_adapter_marks_failed_with_hint(make_orch):
    store, orch = make_orch(fake="unconfigured")
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="fake", share_url="https://x", password=None)
    orch.submit(tid, share, "fake", None)
    t = _wait_for(store, tid)
    assert t["status"] == "failed"
    assert "未配置" in t["error"]


def test_unknown_target_fails(make_orch):
    store, orch = make_orch(fake="ok")
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="ghost", share_url="https://x", password=None)
    orch.submit(tid, share, "ghost", None)
    t = _wait_for(store, tid)
    assert t["status"] == "failed"
    assert "不支持的目标平台" in t["error"]


def test_cross_platform_without_openlist_token_fails_clearly(make_orch):
    store, orch = make_orch(fake="ok", other="ok")
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x", password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid)
    assert t["status"] == "failed"
    assert "OPENLIST_TOKEN" in t["error"]


def test_cross_platform_missing_mount_fails(make_orch):
    store, orch = make_orch(fake="ok", other="ok")
    orch.settings.openlist_token = "tok"
    orch.settings.openlist_mounts = {"fake": "/fake"}  # other 未配置挂载
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x", password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid)
    assert t["status"] == "failed"
    assert "OPENLIST_MOUNT" in t["error"]


def test_cross_platform_relay_full_flow(tmp_path):
    """跨平台全链路：save → OpenList 复制 → locate → share。"""
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(openlist_token="tok")
    fake_ol = FakeOpenList()

    def builder(client):
        return {"fake": FakeAdapter("ok"), "other": FakeAdapter("ok")}

    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x", password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid)
    assert t["status"] == "success", t
    assert t["result_url"] == "https://fake/s/new"
    # OpenList 调用参数正确：源挂载+保存目录、目标挂载、文件名列表
    assert fake_ol.copied["src_dir"] == "/fake/saved"
    assert fake_ol.copied["dst"] == "/other/转存"
    assert fake_ol.copied["names"] == ["a.txt"]
    assert fake_ol.copied["waited"] is True


def test_cross_platform_relay_skip_existing_files(tmp_path):
    """重复转存：目标盘已有全部同名文件 → 跳过复制，直接 locate + share（幂等）。"""
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(openlist_token="tok")
    fake_ol = FakeOpenList(existing=["a.txt"])

    def builder(client):
        return {"fake": FakeAdapter("ok"), "other": FakeAdapter("ok")}

    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x", password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid)
    assert t["status"] == "success", t
    assert t["result_url"] == "https://fake/s/new"
    assert "names" not in fake_ol.copied        # 未发起复制
    assert "waited" not in fake_ol.copied
    assert fake_ol.copied["checked"] == ["a.txt"]


def test_cross_platform_relay_to_local(tmp_path):
    """夸克等 → 本机高速：OpenList 落盘本机后生成 /dl/<dl_token> 短链。"""
    from adapters.local import LocalAdapter

    store = TaskStore(str(tmp_path / "t.db"))
    work = tmp_path / "downloads" / "转存"
    work.mkdir(parents=True)
    (work / "a.txt").write_text("hello local", encoding="utf-8")

    s = FakeSettings(openlist_token="tok", local_dir=str(tmp_path / "downloads"),
                     openlist_mounts={"fake": "/fake", "local": "/local"})
    fake_ol = FakeOpenList()

    def builder(client):
        return {"fake": FakeAdapter("ok"), "local": LocalAdapter(s)}

    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="local", share_url="https://x", password=None)
    orch.submit(tid, share, "local", None)
    t = _wait_for(store, tid)
    assert t["status"] == "success", t
    # 下载链接用 dl_token（与 task_id 解耦）；单文件走短链
    dl = store.get(tid)["dl_token"]
    assert t["result_url"] == f"/dl/{dl}"
    # 本机目标的复制目的目录 = /local/转存
    assert fake_ol.copied["dst"] == "/local/转存"


def test_cross_platform_relay_source_save_failure(make_orch, tmp_path):
    """跨平台时源盘 save 失败 → 任务失败并带原因。"""
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(openlist_token="tok")
    orch = Orchestrator(store, s, max_workers=1,
                        registry_builder=lambda client: {
                            "fake": FakeAdapter("boom"), "other": FakeAdapter("ok")},
                        openlist_factory=FakeOpenList)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x", password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid)
    assert t["status"] == "failed"
    assert "提取码错误" in t["error"]


def test_cap_helpers(tmp_path):
    """_cap_gb / _cap_bytes / _oversized_top_names 纯函数：
    local 与未配置上限（及 ≤0）视为无限制；能按目标盘上限正确挑出含超限文件的顶层项。"""
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(target_file_cap_gb={"other": 4, "baidu": 8})
    orch = Orchestrator(store, s, max_workers=1)
    assert orch._cap_gb("other") == 4
    assert orch._cap_bytes("baidu") == int(8 * 2 ** 30)
    assert orch._cap_gb("local") == 0.0        # 本机高速不做云盘单文件限制
    assert orch._cap_gb("ghost") == 0.0        # 未配置上限 → 无限制
    assert orch._cap_bytes("ghost") == 0

    # 一个目录里只要有一个文件超限，整项都标记超限（复制阶段整体跳过该顶层项）
    class FakeMeter:
        files = {
            "big.iso": int(9 * 2 ** 30),          # 9GB，超限
            "small.txt": int(1 * 2 ** 30),        # 1GB，未超限
            "folder/huge.mkv": int(15 * 2 ** 30),  # 15GB，超限（在子目录内）
        }
    top = ["big.iso", "small.txt", "folder"]
    assert orch._oversized_top_names(FakeMeter(), top, "baidu") == {"big.iso", "folder"}
    assert orch._oversized_top_names(FakeMeter(), top, "other") == {"big.iso", "folder"}  # 4GB 也超限
    assert orch._oversized_top_names(FakeMeter(), top, "local") == set()      # 无限制
    assert orch._oversized_top_names(FakeMeter(), top, "ghost") == set()      # 未配置


def test_relay_pauses_on_over_size_limit(tmp_path):
    """单文件超限不再「整单失败」，而是暂停为 needs_confirm 并写入超限清单，
    且复制在 ol.copy 之前就被拦下（不会跑到百度 31299 才炸）。"""
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(openlist_token="tok", target_file_cap_gb={"other": 4})
    fake_ol = FakeOpenList(files={"a.txt": 5 * 2 ** 30})  # 5GB > 4GB 上限

    def builder(client):
        return {"fake": FakeAdapter("ok"), "other": FakeAdapter("ok")}

    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x", password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid, ("needs_confirm", "failed", "success"))
    assert t["status"] == "needs_confirm", t
    assert any(f["name"] == "a.txt" and f["size"] == 5 * 2 ** 30 for f in t["over_files"])
    # 关键：超限在 ol.copy 之前就暂停，复制从未发起（无 names/src_dir/waited 写入）
    assert "names" not in fake_ol.copied
    assert "src_dir" not in fake_ol.copied
    assert "waited" not in fake_ol.copied


def test_confirm_continue_skips_over_and_copies_rest(tmp_path, monkeypatch):
    """确认继续后：复用已保存源、跳过超限文件、仅复制未超限部分，最终成功落库。"""
    import orchestrator as orch_mod
    from adapters.base import Adapter, SavedRef, TransferResult

    # 测试环境无真实分享链接：把 share_url 解析/密码校验都接到假盘
    monkeypatch.setattr(orch_mod, "parse_share_text",
                        lambda url: ShareLink(platform="fake", share_id="abc"))
    monkeypatch.setattr(orch_mod, "validate_password", lambda share, pw: None)

    class TwoFileAdapter(Adapter):
        platform = "fake"
        label = "假盘"
        def configured(self):
            return True
        async def save(self, share, password, progress):
            return SavedRef(platform="fake", names=["big.iso", "small.txt"],
                            mount_dir="saved", refs=[{"fid": "s1"}])
        async def share(self, ref, progress):
            return TransferResult(new_url="https://fake/s/done", password="",
                                 files_saved=len(ref.refs), message="完成")
        async def locate(self, names, mount_dir, progress):
            return SavedRef(platform="fake", names=names, mount_dir=mount_dir,
                            refs=[{"fid": "s2"}])

    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(openlist_token="tok", target_file_cap_gb={"other": 4})
    fake_ol = FakeOpenList(files={"big.iso": 5 * 2 ** 30, "small.txt": 1 * 2 ** 30})
    def builder(client):
        return {"fake": TwoFileAdapter(s), "other": TwoFileAdapter(s)}
    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x", password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid, ("needs_confirm", "failed", "success"))
    assert t["status"] == "needs_confirm", t
    # 用户选「继续（跳过超限）」：复用源、跳过 big.iso、复制 small.txt
    orch.confirm_continue(tid)
    t2 = _wait_for(store, tid, ("success", "failed"))
    assert t2["status"] == "success", t2
    assert fake_ol.copied.get("names") == ["small.txt"]
    assert t2["result_url"] == "https://fake/s/done"


def test_relay_local_unlimited_for_huge_file(tmp_path):
    """目标选本机高速：单文件超过云盘上限（如 5GB > 百度 4GB 墙）也不被预检拦下，
    正常走完复制（云盘上限只约束云端目标，local 落本机不受其约束）。"""
    from adapters.local import LocalAdapter

    store = TaskStore(str(tmp_path / "t.db"))
    work = tmp_path / "downloads" / "转存"
    work.mkdir(parents=True)
    # 注意：FakeAdapter.save 硬编码返回 names=["a.txt"]，故落盘与源树都用 a.txt 对齐
    (work / "a.txt").write_text("x" * 1024, encoding="utf-8")

    s = FakeSettings(openlist_token="tok", local_dir=str(tmp_path / "downloads"),
                     openlist_mounts={"fake": "/fake", "local": "/local"},
                     target_file_cap_gb={"other": 4})
    fake_ol = FakeOpenList(files={"a.txt": 5 * 2 ** 30})  # 5GB > 百度 4GB 云盘墙，但本机装得下

    def builder(client):
        return {"fake": FakeAdapter("ok"), "local": LocalAdapter(s)}

    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="local", share_url="https://x", password=None)
    orch.submit(tid, share, "local", None)
    t = _wait_for(store, tid)
    assert t["status"] == "success", t
    assert fake_ol.copied.get("waited") is True


def test_queue_info_flags_waiting(make_orch):
    """并发可视：worker 已满时新单元标记为「排队等待」；起跑/结束计数正确。"""
    class _BlockedPool:
        """只记录不执行 → 永远占不到 worker，用来稳定复现「排队」判定。"""
        def __init__(self):
            self.calls = []

        def submit(self, fn, *a):
            self.calls.append((fn, a))

    store, orch = make_orch()
    orch._pool = _BlockedPool()
    orch._max_workers = 1
    # 第一个：无人在跑且 worker 空闲 → 不必排队
    assert orch._dispatch(lambda: None) is False
    # 第二个：已有单元在排队 → 必然要等
    assert orch._dispatch(lambda: None) is True
    assert orch.queue_info() == {"workers": 1, "running": 0, "waiting": 2}
    # worker 认领一个：排队 -1、执行 +1
    orch._q_enter()
    assert orch.queue_info() == {"workers": 1, "running": 1, "waiting": 1}
    orch._q_enter()
    orch._q_leave()
    orch._q_leave()
    assert orch.queue_info() == {"workers": 1, "running": 0, "waiting": 0}
    assert len(orch._pool.calls) == 2


def test_status_shape(make_orch):
    store, orch = make_orch(fake="ok")
    st = orch.status()
    assert st["adapters"] == {"fake": True}
    assert st["openlist_relay"] is False
    assert st["openlist_mounts"] == {"fake": "/fake", "other": "/other"}


# ---------- 组合转存（一条链接 → 多个目标盘，组内串行） ----------

def test_group_status_any_failure_means_group_failed():
    """整组状态派生：**任一子任务失败 → 整组失败**（用户确认的语义）。"""
    gs = Orchestrator.group_status
    assert gs([{"status": "success"}, {"status": "success"}]) == "success"
    assert gs([{"status": "success"}, {"status": "failed"}]) == "failed"
    assert gs([{"status": "failed"}, {"status": "cancelled"}]) == "failed"
    assert gs([{"status": "cancelled"}, {"status": "cancelled"}]) == "cancelled"
    assert gs([{"status": "success"}, {"status": "cancelled"}]) == "cancelled"
    assert gs([{"status": "success"}, {"status": "running"}]) == "running"
    assert gs([{"status": "pending"}, {"status": "pending"}]) == "running"
    assert gs([{"status": "needs_confirm"}, {"status": "pending"}]) == "needs_confirm"
    assert gs([]) == "pending"


def test_cancel_flags_whole_group(make_orch):
    """取消组内**任一**子任务 = 取消整组（信号覆盖 + 未终态兄弟立即落 cancelled）。"""
    store, orch = make_orch(fake="ok")
    a = store.create(source="fake", target="fake", share_url="https://x",
                     password=None, group_id="g1", group_seq=1)
    b = store.create(source="fake", target="fake", share_url="https://x",
                     password=None, group_id="g1", group_seq=2)
    orch.cancel(a)                       # 只点了其中一个
    assert orch._is_cancelled(b)         # 兄弟也被置了取消信号
    assert orch.cancel_group("g1") == 2
    assert store.get(a)["status"] == "cancelled"
    assert store.get(b)["status"] == "cancelled"


def _group_orch(tmp_path, *, fail_target=None):
    """组合转存测试台：假源盘 + 两个假目标盘 + 假 OpenList。

    返回 (store, orch, saved_calls, exec_order)；saved_calls 记录**源盘 save 次数**
    （验证「源盘只存一次」），exec_order 记录子任务实际执行顺序（验证串行）。
    """
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings()
    s.openlist_token = "tok"
    s.openlist_mounts = {"fake": "/fake", "other": "/other", "third": "/third"}
    orch = Orchestrator(store, s, max_workers=1)
    saved_calls: list[int] = []

    class SrcAdapter(FakeAdapter):
        platform = "fake"

        async def save(self, share, password, progress):
            saved_calls.append(1)
            return await super().save(share, password, progress)

    class TgtAdapter(FakeAdapter):
        def __init__(self, platform, fail=False):
            super().__init__("ok")
            self.platform = platform
            self._fail = fail

        async def locate(self, names, mount_dir, progress):
            if self._fail:
                raise AdapterError(f"{self.platform} 定位失败")
            return await super().locate(names, mount_dir, progress)

    adapters = {"fake": SrcAdapter("ok"),
                "other": TgtAdapter("other", fail=(fail_target == "other")),
                "third": TgtAdapter("third", fail=(fail_target == "third"))}
    orch._adapters = adapters
    orch._build_registry = lambda client: adapters
    orch._openlist_factory = lambda: FakeOpenList()

    exec_order: list[str] = []
    orig_exec = orch._exec_one

    def spy(tid, share, target, pwd, **kw):
        exec_order.append(target)
        return orig_exec(tid, share, target, pwd, **kw)

    orch._exec_one = spy
    return store, orch, saved_calls, exec_order


def test_run_group_serial_and_saves_source_once(tmp_path):
    """组内**串行**执行；跨平台子任务只对源盘 save 一次（src_ref 广播给兄弟复用）。"""
    store, orch, saved_calls, exec_order = _group_orch(tmp_path)
    gid, base = "g1", 100.0
    store.create(source="fake", target="other", share_url="https://x", password=None,
                 owner_id="u", group_id=gid, group_seq=1, created_at=base)
    store.create(source="fake", target="third", share_url="https://x", password=None,
                 owner_id="u", group_id=gid, group_seq=2, created_at=base - 0.001)
    orch._run_group(gid, ShareLink(platform="fake", share_id="abc"), None, 1)

    assert exec_order == ["other", "third"]             # 串行且按 seq 升序
    kids = store.list_group(gid)
    assert [k["status"] for k in kids] == ["success", "success"]
    assert len(saved_calls) == 1                        # 源盘只 save 了一次
    assert kids[1]["reuse_src"] is True                 # 兄弟被标记为复用
    assert kids[1]["src_ref"] == kids[0]["src_ref"]     # 且拿到同一份源盘引用


def test_run_group_continues_after_failure_but_group_is_failed(tmp_path):
    """某目标失败**不中断**其余目标（保住已完成的工作），整组状态派生为「失败」。"""
    store, orch, _saved, exec_order = _group_orch(tmp_path, fail_target="other")
    gid = "g2"
    store.create(source="fake", target="other", share_url="https://x", password=None,
                 owner_id="u", group_id=gid, group_seq=1, created_at=100.0)
    store.create(source="fake", target="third", share_url="https://x", password=None,
                 owner_id="u", group_id=gid, group_seq=2, created_at=99.999)
    orch._run_group(gid, ShareLink(platform="fake", share_id="abc"), None, 1)

    assert exec_order == ["other", "third"]             # 失败后仍然继续跑其余目标
    kids = store.list_group(gid)
    assert [k["status"] for k in kids] == ["failed", "success"]
    assert Orchestrator.group_status(kids) == "failed"  # 任一失败 → 整组失败
    assert "定位失败" in kids[0]["error"]


def test_run_group_rejects_own_share_on_same_platform(tmp_path):
    """组合转存：原链接是**本人**分享 + 同源目标 → 判失败（文件本就在该网盘里）。"""
    store, orch, _saved, _order = _group_orch(tmp_path)

    class SelfShareAdapter(FakeAdapter):
        platform = "fake"

        async def transfer(self, share, password, progress):
            return TransferResult(new_url="", message="已定位盘内文件",
                                  details={"self_share": True})

    orch._adapters = {**orch._adapters, "fake": SelfShareAdapter("ok")}
    orch._build_registry = lambda client: orch._adapters
    tid = store.create(source="fake", target="fake", share_url="https://x",
                       password=None, owner_id="u", group_id="g3", group_seq=1,
                       created_at=100.0)
    orch._run_group("g3", ShareLink(platform="fake", share_id="abc"), None, 1)

    t = store.get(tid)
    assert t["status"] == "failed"
    assert "无需转存到同源目标" in t["error"]


def test_relay_skips_both_existing_and_oversized(tmp_path):
    """回归：超限过滤必须**叠加在已滤掉 existing 的集合上**，不能从全量 names 重算。

    旧实现 `to_copy = [n for n in src_ref.names if n not in over_names]` 会把
    「目标盘已存在」的 a.txt 重新塞回复制清单 → fs/copy 撞 file exists 整单失败，
    正是幂等预检要防的场景。
    """
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(openlist_token="tok", target_file_cap_gb={"other": 4})
    fake_ol = FakeOpenList(existing=["a.txt"],
                           files={"a.txt": 4, "b.txt": 8, "big.iso": 5 * 2 ** 30})

    class MultiAdapter(FakeAdapter):
        async def save(self, share, password, progress):
            progress(50, "半程")
            return SavedRef(platform="fake", names=["a.txt", "b.txt", "big.iso"],
                            mount_dir="saved", refs=[{"fid": "s1"}])

    def builder(client):
        return {"fake": MultiAdapter("ok"), "other": FakeAdapter("ok")}

    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x",
                       password=None)
    store.update(tid, confirmed_skip=True)      # 用户已确认「跳过超限继续」
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid)
    assert t["status"] == "success", t
    # 已存在的 a.txt 与超限的 big.iso 都必须排除，只复制 b.txt
    assert fake_ol.copied["names"] == ["b.txt"]


def test_relay_rejects_same_name_different_size(tmp_path):
    """同名但大小不同 = 目标盘那个是**另一个文件** → 拒绝复用，免得分享错内容。"""
    store = TaskStore(str(tmp_path / "t.db"))
    s = FakeSettings(openlist_token="tok")
    fake_ol = FakeOpenList(existing=["a.txt"], files={"a.txt": 4},
                           existing_sizes={"a.txt": 4096})

    def builder(client):
        return {"fake": FakeAdapter("ok"), "other": FakeAdapter("ok")}

    orch = Orchestrator(store, s, max_workers=1, registry_builder=builder,
                        openlist_factory=lambda: fake_ol)
    share = ShareLink(platform="fake", share_id="abc")
    tid = store.create(source="fake", target="other", share_url="https://x",
                       password=None)
    orch.submit(tid, share, "other", None)
    t = _wait_for(store, tid)
    assert t["status"] == "failed", t
    assert "同名但大小不同" in t["error"]
    assert "names" not in fake_ol.copied        # 直接拒绝，没发起复制
