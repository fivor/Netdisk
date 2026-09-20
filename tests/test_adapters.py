"""适配器测试：httpx.MockTransport 模拟三家接口，验证我方请求逻辑与状态映射。"""
import asyncio
import json
from dataclasses import dataclass
from urllib.parse import parse_qs

import httpx
import pytest

from adapters.ali import AliAdapter
from adapters.baidu import BaiduAdapter
from adapters.base import (AdapterError, AdapterNotConfigured,
                           CapabilityError, SavedRef)
from adapters.quark import QuarkAdapter
from adapters.uc import UCAdapter
from adapters.local import LocalAdapter
from parser import ShareLink


@dataclass
class Settings:
    transfer_dir: str = "转存"
    local_dir: str = ""
    baidu_cookie: str = "BDUSS=abc; STOKEN=def"
    quark_cookie: str = "__pus=1; __puus=2"
    uc_cookie: str = "__pus=uc1; __puus=uc2"
    ali_refresh_token: str = "rt-default"
    # 默认带 client_id/secret → 官方刷新路径；在线续期测试显式置空
    ali_client_id: str = "cid"
    ali_client_secret: str = "cs"
    baidu_share_days: int = 7
    ali_share_days: int = 7
    db_path: str = ""
    ali_online_renew_api: str = "https://api.oplist.org/alicloud/renewapi"
    ali_driver_txt: str = "alicloud_qr"


def mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)


async def run(adapter, share, pwd=None):
    return await adapter.transfer(share, pwd, lambda p, m: None)


async def run_traced(adapter, share, pwd=None):
    """同 run，但把 (进度, 消息) 序列收集起来，供单调性/文案回归断言。"""
    seen: list[tuple[int, str]] = []

    def _cb(p, m):
        seen.append((p, m))

    result = await adapter.transfer(share, pwd, _cb)
    return result, seen


@pytest.fixture()
def fast_sleep(monkeypatch):
    """把适配器里的任务轮询间隔缩到 10ms。"""
    orig = asyncio.sleep

    async def _fast(_delay):
        await orig(0.01)

    monkeypatch.setattr(asyncio, "sleep", _fast)
    yield


# ---------------- 百度 ----------------

class TestBaidu:
    def test_not_configured(self):
        s = Settings(baidu_cookie="")
        a = BaiduAdapter(s, client=mock_client(lambda r: httpx.Response(200, json={})))
        with pytest.raises(AdapterNotConfigured):
            asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd"))

    def test_meta_chain_blocked_translated(self, fast_sleep):
        """三级元数据全失败 → shorturlinfo errno=2 ≠ 分享失效：如实说「请稍后重试」
        并透传百度提示（回归 2026-09-20：同一分享 share/list errno=0 活得好好的，
        旧文案咬定「链接已失效」误导用户）。"""

        def handler(req: httpx.Request) -> httpx.Response:
            if "share/verify" in str(req.url):
                return httpx.Response(200, json={"errno": 0, "randsk": "sk"})
            if "/share/init" in str(req.url) or "/s/1abc123" in str(req.url):
                # 主/备页面都解析不出 shareid（两轮重试都会走到）
                return httpx.Response(200, text="<html>error</html>")
            if "shorturlinfo" in str(req.url):
                return httpx.Response(200, json={
                    "errno": 2, "show_msg": "啊哦，链接出错了"})
            raise AssertionError(f"不应到达: {req.url}")

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="请稍后重试.*链接出错了"):
            asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd1"))

    def test_transfer_blocked_content_translated(self, fast_sleep):
        """transfer 阶段 errno=2「不是分享内的文件」≠ 链接失效：如实报「内容被审核/
        和谐，可稍后重试」（回归 2026-09-20 对照实验：同参数对正常分享返回
        「文件已存在」，证明是内容级拦截而非链接问题）。"""

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "share/verify" in url:
                return httpx.Response(200, json={"errno": 0, "randsk": "sk"})
            if "/share/init" in url:
                return httpx.Response(200, text='share_uk:"222","shareid":111')
            if "share/list" in url:
                return httpx.Response(200, json={"errno": 0, "list": [
                    {"fs_id": 333, "isdir": 0, "server_filename": "a.mp4"}]})
            if "api/create" in url:
                return httpx.Response(200, json={"errno": 0})
            if "share/transfer" in url:
                return httpx.Response(200, json={
                    "errno": 2, "show_msg": "不是分享内的文件"})
            raise AssertionError(f"不应到达: {url}")

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="拒绝转存.*审核或已失效.*稍后重试"):
            asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd1"))

    def test_transfer_censored_md5_detected(self, fast_sleep):
        """和谐文件特征：list 可见但 md5 含非 hex 字符 → transfer errno=2 时
        直接点名「已被百度和谐」，别让用户白等「稍后重试」（2026-09-20 实锤：
        md5=bc3df6344q156ae9... 的文件 list 正常、转存必被拒）。"""

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "share/verify" in url:
                return httpx.Response(200, json={"errno": 0, "randsk": "sk"})
            if "/share/init" in url:
                return httpx.Response(200, text='share_uk:"222","shareid":111')
            if "share/list" in url:
                return httpx.Response(200, json={"errno": 0, "list": [
                    {"fs_id": 333, "isdir": 0, "server_filename": "课程.mp4",
                     "md5": "bc3df6344q156ae97332d2deef70f776"}]})
            if "api/create" in url:
                return httpx.Response(200, json={"errno": 0})
            if "share/transfer" in url:
                return httpx.Response(200, json={
                    "errno": 2, "show_msg": "不是分享内的文件"})
            raise AssertionError(f"不应到达: {url}")

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="已被百度和谐.*更换资源源"):
            asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd1"))

    def test_transfer_file_exists_translated(self, fast_sleep):
        """转存自己分享的文件 → errno=2"文件已存在" → 人话（回归 2026-09-16）。"""

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "share/verify" in url:
                return httpx.Response(200, json={"errno": 0, "randsk": "sk"})
            if "api/create" in url:
                return httpx.Response(200, json={"errno": 0})
            if "/share/init" in url:
                return httpx.Response(200, text='share_uk:"222","shareid":111')
            if "share/list" in url:
                return httpx.Response(200, json={"errno": 0, "list": [
                    {"fs_id": 333, "isdir": 0, "server_filename": "a.apk"}]})
            if "share/transfer" in url:
                return httpx.Response(200, json={
                    "errno": 2, "show_msg": "文件已存在"})
            raise AssertionError(f"不应到达: {req.url}")

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="已存在于你的网盘中"):
            asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd1"))

    def test_wrong_password(self):
        def handler(req: httpx.Request) -> httpx.Response:
            if "share/verify" in str(req.url):
                return httpx.Response(200, json={"errno": -9})
            raise AssertionError(f"不应到达: {req.url}")

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="提取码错误"):
            asyncio.run(run(a, ShareLink("baidu", "abc123"), "bad"))

    def test_non_json_response_risk_control(self):
        """百度风控返回 HTML 页时应给出可读错误而非内部异常。"""

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="<html>百度安全验证</html>",
                                  headers={"content-type": "text/html"})

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="可能触发风控"):
            asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd"))

    def test_happy_path(self):
        seen = {}

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "share/verify" in url:
                assert req.url.params["surl"] == "abc123"
                return httpx.Response(200, json={"errno": 0, "randsk": "sk-xyz"})
            if "api/create" in url:
                form = parse_qs(req.content.decode())
                assert form["path"][0] == "/转存"
                assert form["isdir"][0] == "1"
                return httpx.Response(200, json={"errno": 0})
            if "/share/init" in url:
                # 主路径：落地页内嵌 share_uk / shareid（实测定案）
                assert req.url.params["surl"] == "abc123"
                assert req.url.params["pwd"] == "pwd1"
                html = ('<html>uk:\'0\', share_uk:"222","shareid":111,'
                        'followFlag:-1</html>')
                return httpx.Response(200, text=html)
            if "share/list" in url:
                # 文件列表：shareid+uk+sekey(randsk)
                assert req.url.params["shareid"] == "111"
                assert req.url.params["uk"] == "222"
                assert req.url.params["sekey"] == "sk-xyz"
                return httpx.Response(200, json={"errno": 0, "list": [
                    {"fs_id": 333, "isdir": 0, "from": "uk222",
                     "server_filename": "电影.mkv"}]})
            if "share/transfer" in url:
                form = parse_qs(req.content.decode())
                seen["transfer_files"] = json.loads(form["filelist"][0])
                seen["transfer_path"] = form["path"][0]
                return httpx.Response(200, json={"errno": 0, "extra": {"save_path": "/saved/x"}})
            if "api/list" in url:
                # 转存完成后定位「转存」目录里的实际副本
                assert req.url.params["dir"] == "/转存"
                return httpx.Response(200, json={"errno": 0, "list": [
                    {"fs_id": 777, "path": "/转存/电影.mkv", "isdir": 0,
                     "server_filename": "电影.mkv", "server_mtime": 100}]})
            if "share/set" in url:
                form = parse_qs(req.content.decode())
                assert form["schannel"][0] == "0"  # 私密分享
                # 只分享刚转存的文件（file-level fid_list），不得把整个「转存」目录分享出去
                assert json.loads(form["fid_list"][0]) == [777]
                assert "path" not in form
                assert len(form["pwd"][0]) == 4
                return httpx.Response(200, json={"errno": 0,
                                                 "link": "https://pan.baidu.com/s/1newlink"})
            raise AssertionError(f"未模拟: {url}")

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        result = asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd1"))
        # 提取码拼进链接（?pwd=）：产物链接可直接再转存，无需手动补提取码
        assert result.new_url == "https://pan.baidu.com/s/1newlink?pwd=" + result.password
        assert result.password
        assert result.files_saved == 1
        assert seen["transfer_files"] == [333]
        assert seen["transfer_path"] == "/转存"

    def test_folder_only_share_transfers_whole_dir(self, fast_sleep):
        """源分享根目录只有文件夹：目录 fs_id 直接参与 transfer（服务端整树复制），
        定位按目录名命中 → SavedRef 含目录名（回归 2026-09-20「目录需展开」误杀）。
        后续 share() 用该目录的 fs_id 走 fid_list 分支——只分享这个目录，
        不是整个「转存」文件夹（防历史文件泄漏的安全约束不变）。"""
        seen: dict = {}

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "share/verify" in url:
                return httpx.Response(200, json={"errno": 0, "randsk": "sk"})
            if "/share/init" in url:
                return httpx.Response(200, text='share_uk:"222","shareid":111')
            if "api/create" in url:
                return httpx.Response(200, json={"errno": 0})   # _ensure_work_dir 建目录
            if "share/list" in url:
                return httpx.Response(200, json={"errno": 0, "list": [
                    {"fs_id": 888, "isdir": "1", "server_filename": "闲鱼教程"}]})
            if "share/transfer" in url:
                form = parse_qs(req.content.decode())
                seen["transfer_files"] = json.loads(form["filelist"][0])
                return httpx.Response(200, json={"errno": 0})
            if "api/list" in url:
                assert req.url.params["dir"] == "/转存"
                return httpx.Response(200, json={"errno": 0, "list": [
                    {"fs_id": 888, "path": "/转存/闲鱼教程", "isdir": "1",
                     "server_filename": "闲鱼教程", "server_mtime": 200}]})
            if "share/set" in url:
                form = parse_qs(req.content.decode())
                seen["share_fids"] = json.loads(form["fid_list"][0])
                return httpx.Response(200, json={"errno": 0,
                                                 "link": "https://pan.baidu.com/s/1newlink"})
            raise AssertionError(f"未模拟: {url}")

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        result = asyncio.run(run(a, ShareLink("baidu", "abc123"), "pwd1"))
        assert seen["transfer_files"] == [888]      # 目录 fs_id 进了转存请求
        assert seen["share_fids"] == [888]          # 分享用目录自身 fid（不泄漏兄弟内容）
        assert result.files_saved == 1

    def test_share_directory_uses_path_list_param(self):
        """目录分享的路径参数名必须是 `path_list`——写成 `path` 会被百度拒。

        实测（2026-09-17）：share/set 对目录返回
        `{"errno":2,"show_msg":"path_list or fid_list param need"}`；
        且 schannel=0（私密分享）缺 pwd 会 errno=115「账号异常，禁止分享」。
        本用例用「只有 path、没有 fs_id」的 ref 走目录分支，锁住参数名。
        """
        seen: dict = {}

        def handler(req: httpx.Request) -> httpx.Response:
            assert "share/set" in str(req.url)
            form = parse_qs(req.content.decode())
            seen.update({k: v[0] for k, v in form.items()})
            return httpx.Response(200, json={"errno": 0,
                                             "link": "https://pan.baidu.com/s/1dir"})

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        ref = SavedRef(platform="baidu", names=["合集"], refs=[{"path": "/转存/合集"}])
        res = asyncio.run(a.share(ref, lambda p, m: None))
        assert json.loads(seen["path_list"]) == ["/转存/合集"]
        assert "path" not in seen and "fid_list" not in seen
        assert seen["schannel"] == "0" and len(seen["pwd"]) == 4
        assert res.new_url.startswith("https://pan.baidu.com/s/1dir")

    def test_locate_saved_files_rename_fallback(self):
        """newcopy 重命名副本（"电影(1).mkv"）按去后缀名匹配、mtime 最新优先。"""

        def handler(req: httpx.Request) -> httpx.Response:
            assert req.url.params["dir"] == "/转存"
            return httpx.Response(200, json={"errno": 0, "list": [
                {"fs_id": 1, "path": "/转存/电影.mkv", "isdir": 0,
                 "server_filename": "电影.mkv", "server_mtime": 50},
                {"fs_id": 2, "path": "/转存/电影(1).mkv", "isdir": 0,
                 "server_filename": "电影(1).mkv", "server_mtime": 200},
                {"fs_id": 3, "path": "/转存/其他.txt", "isdir": 0,
                 "server_filename": "其他.txt", "server_mtime": 10}]})

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        ref = asyncio.run(a._locate_saved_files(
            a.client(), ["电影.mkv"], "/转存", {"Cookie": "x"}))
        assert ref.names == ["电影(1).mkv"]   # 实际副本名（供 OpenList 复制）
        assert ref.refs == [{"fs_id": 2, "path": "/转存/电影(1).mkv"}]

    def test_strip_rename_suffix(self):
        from adapters.baidu import _strip_rename_suffix as s
        assert s("abc(1).txt") == "abc.txt"
        assert s("abc.txt") == "abc.txt"
        assert s("a(12)") == "a"
        assert s("abc(1)(2).txt") == "abc(1).txt"

    def test_locate_saved_files_paginates(self):
        """转存目录 >100 文件时按名定位需翻页（旧实现只查第一页会漏名）。"""
        page1 = [{"fs_id": i, "path": f"/转存/f{i}.txt", "isdir": 0,
                  "server_filename": f"f{i}.txt", "server_mtime": 1} for i in range(100)]
        page2 = [{"fs_id": 999, "path": "/转存/target.txt", "isdir": 0,
                  "server_filename": "target.txt", "server_mtime": 2}]

        def handler(req: httpx.Request) -> httpx.Response:
            assert "api/list" in str(req.url)
            if req.url.params["page"] == "1":
                return httpx.Response(200, json={"errno": 0, "list": page1})
            return httpx.Response(200, json={"errno": 0, "list": page2})

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        ref = asyncio.run(a._locate_saved_files(
            a.client(), ["target.txt"], "/转存", {"Cookie": "x"}))
        assert ref.refs == [{"fs_id": 999, "path": "/转存/target.txt"}]

    def test_locate_matches_saved_names(self):
        """跨平台：OpenList 复制完成后按名定位自己盘内文件。

        2026-09-19 审查修复：部分命中不再静默放行（那会生成缺文件的分享），
        任何一个名字缺失都必须显式报错。
        """

        def handler(req: httpx.Request) -> httpx.Response:
            assert "api/list" in str(req.url)
            assert req.url.params["dir"] == "/来自转存站/tag1"
            return httpx.Response(200, json={"errno": 0, "list": [
                {"fs_id": 777, "path": "/来自转存站/tag1/电影.mkv",
                 "server_filename": "电影.mkv"},
                {"fs_id": 888, "path": "/来自转存站/tag1/其他.txt",
                 "server_filename": "其他.txt"}]})

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        # 全部命中 → 正常返回
        ref = asyncio.run(a.locate(["电影.mkv", "其他.txt"], "/来自转存站/tag1"))
        assert len(ref.refs) == 2
        assert ref.refs[0] == {"fs_id": 777, "path": "/来自转存站/tag1/电影.mkv"}
        # 部分缺失 → 显式报错（旧行为：静默返回子集）
        with pytest.raises(AdapterError, match="未找到转存后的文件"):
            asyncio.run(a.locate(["电影.mkv", "不存在的.file"], "/来自转存站/tag1"))

    def test_locate_accepts_directory_name(self):
        """回归 2026-09-19：源分享整体包含文件夹时，names 里是目录名，
        locate 不得过滤目录（曾导致 quark→百度 含文件夹的转存必失败）。"""

        def handler(req: httpx.Request) -> httpx.Response:
            assert "api/list" in str(req.url)
            return httpx.Response(200, json={"errno": 0, "list": [
                {"fs_id": 555, "path": "/转存/复仇者之死（2010）苍井空", "isdir": 1,
                 "server_filename": "复仇者之死（2010）苍井空"}]})

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        ref = asyncio.run(a.locate(["复仇者之死（2010）苍井空"], "/转存"))
        assert ref.refs == [{"fs_id": 555, "path": "/转存/复仇者之死（2010）苍井空"}]

    def test_locate_not_found_raises(self):
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"errno": 0, "list": []})

        a = BaiduAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="未找到转存后的文件"):
            asyncio.run(a.locate(["x.mkv"], "/dir"))


# ---------------- 夸克 ----------------

class TestQuark:
    def test_not_configured(self):
        s = Settings(quark_cookie="")
        a = QuarkAdapter(s, client=mock_client(lambda r: httpx.Response(200, json={})))
        with pytest.raises(AdapterNotConfigured):
            asyncio.run(run(a, ShareLink("quark", "q1"), ""))

    def test_error_41017_self_share_fallback(self, fast_sleep):
        """转存自己的分享（41017）→ 降级为盘内定位，跨平台链路继续。"""

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "sharepage/token" in url:
                return httpx.Response(200, json={"code": 0, "data": {"stoken": "st"}})
            if "sharepage/detail" in url:
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "f1", "share_fid_token": "t1", "file_name": "a.txt"}]}})
            if "sharepage/save" in url:
                body = json.loads(req.content)
                assert body["to_pdir_fid"] == "workdir"  # 转存目录
                return httpx.Response(200, json={
                    "code": 41017, "message": "用户禁止转存自己的分享"})
            if "file/sort" in url:
                # ensure_work_dir 查根目录；BFS 定位时同样从根开始
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "workdir", "file_name": "转存", "dir": True},
                    {"fid": "f1", "file_name": "a.txt", "dir": False}]}})
            if req.url.path.endswith("/clouddrive/file") and req.method == "POST":
                return httpx.Response(200, json={"code": 0, "data": {"fid": "workdir"}})
            if "clouddrive/task" in url:
                return httpx.Response(200, json={"code": 0, "data": {
                    "status": 2, "share_id": "selfshare1"}})
            if req.url.path.endswith("/clouddrive/share") and req.method == "POST":
                body = json.loads(req.content)
                assert body["fid_list"] == ["f1"]  # 用盘内 fid 生成新分享
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk2"}})
            if "share/password" in url:
                return httpx.Response(200, json={"code": 0, "data": {
                    "share_url": "https://pan.quark.cn/s/selfshare1"}})
            raise AssertionError(f"未模拟: {url}")

        a = QuarkAdapter(Settings(), client=mock_client(handler))
        result = asyncio.run(run(a, ShareLink("quark", "q1"), ""))
        assert result.new_url == "https://pan.quark.cn/s/selfshare1"

    def test_save_progress_never_goes_backwards(self, fast_sleep, tmp_path, ali_clean):
        """进度百分比全程单调不减（回归「40% → 35% 进度条回退」）。

        同时覆盖 _wait_task：轮询期的上界必须压在「转存完成」之下，
        否则长时间转存会让进度先冲到 90 再回落到 70/55，视觉上像卡死倒退。
        """
        polls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "sharepage/token" in url:
                return httpx.Response(200, json={"code": 0, "data": {"stoken": "st"}})
            if "sharepage/detail" in url:
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "f1", "share_fid_token": "t1", "file_name": "a.txt"}]}})
            if req.url.path.endswith("/clouddrive/file") and req.method == "POST":
                return httpx.Response(200, json={"code": 0, "data": {"fid": "workdir"}})
            if "file/sort" in url:
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "workdir", "file_name": "转存", "dir": True}]}})
            if "sharepage/save" in url:
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk1"}})
            if "clouddrive/task" in url:
                tid = req.url.params.get("task_id")
                if tid == "tk1":        # save 的转存任务
                    polls["n"] += 1
                    # 前 30 次一直「处理中」，逼迫 _wait_task 逼近旧实现的 90% 上界
                    if polls["n"] <= 30:
                        return httpx.Response(200, json={"code": 0, "data": {"status": 1}})
                    return httpx.Response(200, json={"code": 0, "data": {
                        "status": 2, "save_as_top_fids": ["s1"]}})
                return httpx.Response(200, json={"code": 0, "data": {   # tk2：分享任务
                    "status": 2, "share_id": "mono"}})
            if "clouddrive/share" in url and "sharepage" not in url \
                    and "password" not in url and req.method == "POST":
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk2"}})
            if "share/password" in url:
                return httpx.Response(200, json={"code": 0, "data": {
                    "share_url": "https://pan.quark.cn/s/mono"}})
            raise AssertionError(f"未模拟: {url}")

        a = QuarkAdapter(Settings(), client=mock_client(handler))
        result, seen = asyncio.run(run_traced(a, ShareLink("quark", "q1"), ""))
        assert result.new_url == "https://pan.quark.cn/s/mono"
        assert polls["n"] > 30          # 确实经历了长轮询

        pcts = [p for p, _ in seen]
        assert pcts == sorted(pcts), f"进度出现回退: {pcts}"
        assert max(pcts) <= 100
        # 转存阶段（百分比 < 70）不得越过下游「转存完成」的 70
        assert max(p for p in pcts if p < 70) <= 65

    def test_self_share_dir_path_walk(self):
        """自己的分享：优先用 pdir_fid + /file/info 自底向上拼路径，不再遍历整盘。

        回归 2026-09-18：盘内 3500+ 条目时，旧实现撞 3000 条扫描上限 →
        误报「盘内未定位到 1 个文件（可能已被删除）」，而文件其实好好地躺在盘里。
        """
        calls = {"sort": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "file/info" in url:
                chain = {"d1": ("pkg", "d2"), "d2": ("games", "0")}
                name, pdir = chain[req.url.params["fid"]]
                return httpx.Response(200, json={"code": 0, "data": {
                    "file_name": name, "pdir_fid": pdir}})
            if "file/sort" in url:
                calls["sort"] += 1
                return httpx.Response(200, json={"code": 0, "data": {"list": []}})
            raise AssertionError(f"未模拟: {url}")

        a = QuarkAdapter(Settings(), client=mock_client(handler))
        ref = asyncio.run(a._self_share_ref(
            a.client(), [{"fid": "f1", "pdir_fid": "d1"}], ["f1"], ["a.txt"],
            lambda p, m: None))
        assert ref.mount_dir == "/games/pkg"
        assert ref.refs == [{"fid": "f1"}]
        assert calls["sort"] == 0        # 一次暴力遍历都没做

    def test_self_share_scan_limit_reports_real_reason(self, monkeypatch):
        """兜底 BFS 撞上限时给出真实原因，不得误报「文件可能已被删除」。"""
        import adapters.quark as q

        monkeypatch.setattr(q, "SELF_SHARE_SCAN_LIMIT", 2)

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "file/info" in url:
                raise AssertionError("无 pdir_fid 时不应走 file/info")
            if "file/sort" in url:
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": f"x{i}", "file_name": f"f{i}", "dir": True}
                    for i in range(100)]}})
            raise AssertionError(f"未模拟: {url}")

        a = QuarkAdapter(Settings(), client=mock_client(handler))
        with pytest.raises(AdapterError, match="条目过多"):
            asyncio.run(a._self_share_ref(
                a.client(), [{"fid": "f1"}], ["f1"], ["a.txt"], lambda p, m: None))

    def test_happy_path_with_task_polling(self, fast_sleep, tmp_path, ali_clean):
        """完整链路（对齐 quark-auto-save / QuarkPanTool 实测契约）。"""
        poll_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            method = req.method
            if "sharepage/token" in url:
                body = json.loads(req.content)
                assert body == {"pwd_id": "q1", "passcode": ""}
                return httpx.Response(200, json={"code": 0, "data": {"stoken": "st"}})
            if "sharepage/detail" in url:
                assert req.url.params["pwd_id"] == "q1"
                assert req.url.params["stoken"] == "st"
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "f1", "share_fid_token": "t1", "file_name": "a.txt"},
                    {"fid": "f2", "share_fid_token": "t2", "file_name": "b.txt"}]}})
            if req.url.path.endswith("/clouddrive/file") and req.method == "POST":
                # ensure_work_dir：创建/确认「转存」目录
                return httpx.Response(200, json={"code": 0, "data": {"fid": "workdir"}})
            if "file/sort" in url and req.url.params["pdir_fid"] == "0":
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "workdir", "file_name": "转存", "dir": True}]}})
            if "sharepage/save" in url:
                body = json.loads(req.content)
                assert body["fid_list"] == ["f1", "f2"]
                assert body["fid_token_list"] == ["t1", "t2"]
                assert body["to_pdir_fid"] == "workdir"  # 转存目录
                assert body["pwd_id"] == "q1"
                assert body["scene"] == "link"
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk1"}})
            if "clouddrive/task" in url:
                poll_count["n"] += 1
                n = poll_count["n"]
                if n == 1:
                    return httpx.Response(200, json={"code": 0, "data": {"status": 1}})
                if n == 2:
                    return httpx.Response(200, json={"code": 0, "data": {
                        "status": 2, "save_as_top_fids": ["s1", "s2"]}})
                if n == 3:
                    return httpx.Response(200, json={"code": 0, "data": {"status": 1}})
                return httpx.Response(200, json={"code": 0, "data": {
                    "status": 2, "share_id": "newshare"}})
            if "clouddrive/share" in url and "sharepage" not in url \
                    and "password" not in url and method == "POST":
                body = json.loads(req.content)
                assert body["fid_list"] == ["s1", "s2"]
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk2"}})
            if "share/password" in url:
                body = json.loads(req.content)
                assert body["share_id"] == "newshare"
                return httpx.Response(200, json={"code": 0, "data": {
                    "share_url": "https://pan.quark.cn/s/newshare"}})
            raise AssertionError(f"未模拟: {url}")

        a = QuarkAdapter(Settings(), client=mock_client(handler))
        result = asyncio.run(run(a, ShareLink("quark", "q1"), ""))
        assert result.new_url == "https://pan.quark.cn/s/newshare"
        assert result.files_saved == 2
        assert poll_count["n"] >= 4

    def test_save_falls_back_to_locate(self, fast_sleep, tmp_path, ali_clean):
        """save 响应缺 save_as_top_fids 时按名定位自己盘（而非报错）。"""
        polls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "sharepage/token" in url:
                return httpx.Response(200, json={"code": 0, "data": {"stoken": "st"}})
            if "sharepage/detail" in url:
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "f1", "share_fid_token": "t1", "file_name": "a.txt"}]}})
            if "sharepage/save" in url:
                body = json.loads(req.content)
                assert body["to_pdir_fid"] == "workdir"
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk1"}})
            if "clouddrive/task" in url:
                polls["n"] += 1
                if polls["n"] == 1:
                    return httpx.Response(200, json={"code": 0, "data": {"status": 2}})
                return httpx.Response(200, json={"code": 0, "data": {
                    "status": 2, "share_id": "fb-share"}})
            if "file/sort" in url:
                pdir = req.url.params.get("pdir_fid", "0")
                if pdir == "0":
                    return httpx.Response(200, json={"code": 0, "data": {"list": [
                        {"fid": "workdir", "file_name": "转存", "dir": True}]}})
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "saved1", "file_name": "a.txt"}]}})
            if "clouddrive/share" in url and "sharepage" not in url \
                    and "password" not in url and req.method == "POST":
                body = json.loads(req.content)
                assert body["fid_list"] == ["saved1"]
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk2"}})
            if "share/password" in url:
                return httpx.Response(200, json={"code": 0, "data": {
                    "share_url": "https://pan.quark.cn/s/fallback"}})
            raise AssertionError(f"未模拟: {url}")

        a = QuarkAdapter(Settings(), client=mock_client(handler))
        result = asyncio.run(run(a, ShareLink("quark", "q1"), ""))
        assert result.new_url == "https://pan.quark.cn/s/fallback"

    def test_locate_matches_saved_names(self):
        def handler(req: httpx.Request) -> httpx.Response:
            assert "file/sort" in str(req.url)
            assert req.url.params["pdir_fid"] == "0"
            return httpx.Response(200, json={"code": 0, "data": {"list": [
                {"fid": "f9", "file_name": "电影.mkv"},
                {"fid": "f8", "file_name": "其他.txt"}]}})

        a = QuarkAdapter(Settings(), client=mock_client(handler))
        ref = asyncio.run(a.locate(["电影.mkv"], ""))
        assert ref.refs == [{"fid": "f9"}]

    def test_save_without_fid_list_raises(self, fast_sleep, tmp_path, ali_clean):
        """转存产物与按名定位都拿不到 fid 时应显式报错，不得用源文件 fid 兜底。"""
        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "sharepage/token" in url:
                return httpx.Response(200, json={"code": 0, "data": {"stoken": "st"}})
            if "sharepage/detail" in url:
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "f1", "share_fid_token": "t1", "file_name": "a.txt"}]}})
            if "sharepage/save" in url:
                return httpx.Response(200, json={"code": 0, "data": {"task_id": "tk1"}})
            if "clouddrive/task" in url:
                return httpx.Response(200, json={"code": 0, "data": {"status": 2}})
            if "file/sort" in url:
                pdir = req.url.params.get("pdir_fid", "0")
                if pdir == "0":
                    return httpx.Response(200, json={"code": 0, "data": {"list": [
                        {"fid": "workdir", "file_name": "转存", "dir": True}]}})
                return httpx.Response(200, json={"code": 0, "data": {"list": []}})
            raise AssertionError(f"未模拟: {url}")

        s = Settings(quark_cookie=Settings.quark_cookie,
                     db_path=str(tmp_path / "tasks.db"))
        a = QuarkAdapter(s, client=mock_client(handler))
        with pytest.raises(AdapterError, match="未返回转存文件列表"):
            asyncio.run(run(a, ShareLink("quark", "q1"), ""))


# ---------------- UC ----------------

class TestUC:
    def test_not_configured(self):
        s = Settings(uc_cookie="")
        a = UCAdapter(s, client=mock_client(lambda r: httpx.Response(200, json={})))
        with pytest.raises(AdapterNotConfigured):
            asyncio.run(run(a, ShareLink("uc", "u1"), ""))

    def test_uses_uc_domain_and_params(self):
        """UC 复用夸克实现，但请求必须打到 UC 域名、带 UCBrowser 参数与 UC Referer。"""

        def handler(req: httpx.Request) -> httpx.Response:
            assert "pc-api.uc.cn" in str(req.url)
            assert req.url.params["pr"] == "UCBrowser"
            assert "drive.uc.cn" in req.headers.get("referer", "")
            if "file/sort" in str(req.url):
                return httpx.Response(200, json={"code": 0, "data": {"list": [
                    {"fid": "f9", "file_name": "x.txt"}]}})
            raise AssertionError(f"未模拟: {req.url}")

        a = UCAdapter(Settings(), client=mock_client(handler))
        ref = asyncio.run(a.locate(["x.txt"], ""))
        assert ref.refs == [{"fid": "f9"}]

    def test_registry_contains_uc(self):
        from adapters.base import build_registry
        reg = build_registry(Settings())
        assert set(reg) == {"baidu", "quark", "ali", "uc", "local"}
        assert reg["uc"].label == "UC 网盘"


# ---------------- 本机高速 ----------------

class TestLocal:
    def test_save_unsupported(self):
        a = LocalAdapter(Settings(), client=mock_client(lambda r: httpx.Response(200, json={})))
        with pytest.raises(CapabilityError):
            asyncio.run(run(a, ShareLink("local", "x"), ""))

    def test_locate_and_share_single(self, tmp_path):
        work = tmp_path / "downloads" / "转存"
        work.mkdir(parents=True)
        (work / "a.txt").write_text("data", encoding="utf-8")
        s = Settings(local_dir=str(tmp_path / "downloads"))
        a = LocalAdapter(s)
        ref = asyncio.run(a.locate(["a.txt"], "转存"))
        assert ref.refs[0]["size"] == 4
        ref.task_id = "tid123"
        res = asyncio.run(a.share(ref, lambda p, m: None))
        # 单文件短链：/dl/<token>（文件名由服务端 Content-Disposition 提供）
        assert res.new_url == "/dl/tid123"
        assert res.files_saved == 1

    def test_locate_expands_directory(self, tmp_path):
        """源分享整体含文件夹：locate 目录名 → 递归展开为其中的文件（相对路径）。

        回归 2026-09-20：quark→local 任务因 locate 只认文件、不认目录而失败。
        """
        work = tmp_path / "downloads" / "转存"
        (work / "MOD").mkdir(parents=True)
        (work / "MOD" / "a.bin").write_bytes(b"AAAA")
        (work / "MOD" / "b.bin").write_bytes(b"BBBB")
        s = Settings(local_dir=str(tmp_path / "downloads"))
        a = LocalAdapter(s)
        ref = asyncio.run(a.locate(["MOD"], "转存"))
        assert sorted(rr["name"] for rr in ref.refs) == ["MOD/a.bin", "MOD/b.bin"]
        ref.task_id = "tid77"
        res = asyncio.run(a.share(ref, lambda p, m: None))
        assert res.new_url == "/dl/tid77/"
        assert sorted(res.files) == ["MOD/a.bin", "MOD/b.bin"]

    def test_share_multi_files_uses_index(self, tmp_path):
        work = tmp_path / "downloads" / "转存"
        work.mkdir(parents=True)
        (work / "a.txt").write_text("1", encoding="utf-8")
        (work / "b.bin").write_text("2", encoding="utf-8")
        s = Settings(local_dir=str(tmp_path / "downloads"))
        a = LocalAdapter(s)
        ref = asyncio.run(a.locate(["a.txt", "b.bin"], "转存"))
        ref.task_id = "tid9"
        res = asyncio.run(a.share(ref, lambda p, m: None))
        assert res.new_url == "/dl/tid9/"

    def test_share_requires_task_id(self, tmp_path):
        work = tmp_path / "downloads" / "转存"
        work.mkdir(parents=True)
        (work / "a.txt").write_text("1", encoding="utf-8")
        s = Settings(local_dir=str(tmp_path / "downloads"))
        a = LocalAdapter(s)
        ref = asyncio.run(a.locate(["a.txt"], "转存"))
        with pytest.raises(AdapterError, match="任务标识"):
            asyncio.run(a.share(ref, lambda p, m: None))

    def test_locate_missing_raises(self, tmp_path):
        (tmp_path / "downloads" / "转存").mkdir(parents=True)
        s = Settings(local_dir=str(tmp_path / "downloads"))
        a = LocalAdapter(s)
        with pytest.raises(AdapterError, match="未找到"):
            asyncio.run(a.locate(["ghost.txt"], "转存"))

    def test_registry_contains_local(self):
        from adapters.base import build_registry
        reg = build_registry(Settings())
        assert set(reg) == {"baidu", "quark", "ali", "uc", "local"}
        assert reg["local"].label == "本机高速"


# ---------------- 阿里 ----------------

@pytest.fixture()
def ali_clean():
    """清空类级 token 缓存，隔离各测试。"""
    AliAdapter._cache.clear()
    yield
    AliAdapter._cache.clear()


def _ali_settings(tmp_path, rt="rt-x", cid="cid", cs="cs") -> Settings:
    return Settings(ali_refresh_token=rt, ali_client_id=cid, ali_client_secret=cs,
                    db_path=str(tmp_path / "tasks.db"))


class TestAli:
    def test_not_configured(self, tmp_path, ali_clean):
        s = _ali_settings(tmp_path, rt="")
        a = AliAdapter(s, client=mock_client(lambda r: httpx.Response(200, json={})))
        with pytest.raises(AdapterNotConfigured):
            asyncio.run(a.locate(["x"], ""))

    def test_save_unsupported(self, tmp_path, ali_clean):
        """阿里个人开放 API 无分享转存接口 → 明确的能力错误。"""
        a = AliAdapter(_ali_settings(tmp_path), client=mock_client(
            lambda r: httpx.Response(200, json={})))
        with pytest.raises(CapabilityError, match="不提供「保存他人分享」"):
            asyncio.run(a.save(ShareLink("ali", "a1"), "", lambda p, m: None))

    def test_share_unsupported(self, tmp_path, ali_clean):
        a = AliAdapter(_ali_settings(tmp_path), client=mock_client(
            lambda r: httpx.Response(200, json={})))
        with pytest.raises(CapabilityError, match="不提供「创建分享」"):
            asyncio.run(a.share(SavedRef("ali", ["x"], "", [{"file_id": "f"}]),
                                lambda p, m: None))

    def test_official_token_cache(self, tmp_path, ali_clean):
        """企业 client_id/secret 配置时走官方直连刷新，且命中进程内缓存。"""
        calls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            if "oauth/access_token" in str(req.url):
                calls["n"] += 1
                body = json.loads(req.content)
                assert body["client_id"] == "cid"
                assert body["grant_type"] == "refresh_token"
                return httpx.Response(200, json={
                    "accessToken": "at", "refreshToken": "rt-new", "expireIn": 7200})
            return httpx.Response(200, json={})

        a = AliAdapter(_ali_settings(tmp_path, rt="rt-official-1"),
                       client=mock_client(handler))
        c = a.client()

        async def twice():
            t1 = await a._token(c)
            t2 = await a._token(c)
            return t1, t2

        t1, t2 = asyncio.run(twice())
        assert t1 == t2 == "at"
        assert calls["n"] == 1  # 第二次命中缓存

    def test_online_renew_flow(self, tmp_path, ali_clean):
        """个人用户：无 client_id/secret → OpenList 官方在线续期，
        refresh_token 轮换并持久化到状态文件；locate 走真实 drive_id。"""
        calls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "renewapi" in url:
                calls["n"] += 1
                assert req.url.params["refresh_ui"] == "rt-online-1"
                assert req.url.params["server_use"] == "true"
                assert req.url.params["driver_txt"] == "alicloud_qr"
                return httpx.Response(200, json={
                    "access_token": "at2", "refresh_token": "rt-online-2"})
            if "getDriveInfo" in url:
                return httpx.Response(200, json={"resource_drive_id": "rid1"})
            if "openFile/list" in url:
                assert req.headers["authorization"] == "Bearer at2"
                body = json.loads(req.content)
                assert body["drive_id"] == "rid1"
                return httpx.Response(200, json={"items": [
                    {"file_id": "f9", "name": "x.txt"}]})
            raise AssertionError(f"未模拟: {url}")

        s = _ali_settings(tmp_path, rt="rt-online-1", cid="", cs="")
        a = AliAdapter(s, client=mock_client(handler))
        c = a.client()
        asyncio.run(a._token(c))
        assert calls["n"] == 1
        # 轮换后的 refresh_token 已持久化
        import json as _json
        state = _json.load(open(str(tmp_path / "ali_token_state.json"), encoding="utf-8"))
        assert state == {"base": "rt-online-1", "current_rt": "rt-online-2"}
        # 第二次调用命中缓存（且续期接口不再被调）
        asyncio.run(a._token(c))
        assert calls["n"] == 1
        # locate 用真实 drive_id 走 openFile/list
        ref = asyncio.run(a.locate(["x.txt"], ""))
        assert ref.refs == [{"file_id": "f9"}]

    def test_online_renew_env_reset(self, tmp_path, ali_clean):
        """用户更新 .env 的 refresh_token 后，应以 env 为准重置轮换状态。"""
        state_file = tmp_path / "ali_token_state.json"
        state_file.write_text('{"base": "rt-old", "current_rt": "rt-rotated"}',
                              encoding="utf-8")

        def handler(req: httpx.Request) -> httpx.Response:
            if "renewapi" in str(req.url):
                # 必须用 env 新值去续期，而不是状态文件里的旧轮换值
                assert req.url.params["refresh_ui"] == "rt-fresh"
                return httpx.Response(200, json={
                    "access_token": "at3", "refresh_token": "rt-fresh-2"})
            return httpx.Response(200, json={})

        s = _ali_settings(tmp_path, rt="rt-fresh", cid="", cs="")
        a = AliAdapter(s, client=mock_client(handler))
        asyncio.run(a._token(a.client()))

    def test_auth_expired(self, tmp_path, ali_clean):
        def handler(req: httpx.Request) -> httpx.Response:
            url = str(req.url)
            if "oauth/access_token" in url:
                return httpx.Response(200, json={
                    "accessToken": "at", "refreshToken": "rt2", "expireIn": 7200})
            if "getDriveInfo" in url:
                return httpx.Response(200, json={"resource_drive_id": "rid1"})
            return httpx.Response(401, json={"code": "Unauthorized"})

        a = AliAdapter(_ali_settings(tmp_path, rt="rt-exp-1"),
                       client=mock_client(handler))
        with pytest.raises(AdapterError, match="授权失效"):
            asyncio.run(a.locate(["x"], ""))


# ---------- 本机高速：配置判定的边界（2026-09-19） ----------

class _LocalSettings:
    def __init__(self, local_dir):
        self.local_dir = local_dir


def test_local_adapter_configured_requires_nonempty_local_dir():
    """LOCAL_DIR 为空时必须报「未就绪」。

    回归：旧实现 `bool(self._base())` 里 `Path("")` 就是 `Path('.')`，**恒为真** ——
    「LOCAL_DIR 未配置」会被误判成已就绪，等真跑起来才炸。
    """
    from adapters.local import LocalAdapter

    assert LocalAdapter(_LocalSettings("")).configured() is False
    assert LocalAdapter(_LocalSettings("   ")).configured() is False
    assert LocalAdapter(_LocalSettings("/data/downloads")).configured() is True


def test_local_adapter_locate_raises_when_unconfigured():
    """未配置时 locate 要报「未配置」的人话错误，而不是拿 Path('.') 去乱找文件。"""
    from adapters.base import AdapterNotConfigured
    from adapters.local import LocalAdapter

    with pytest.raises(AdapterNotConfigured):
        asyncio.run(LocalAdapter(_LocalSettings("")).locate(["a.txt"], ""))
