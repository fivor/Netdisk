"""适配器抽象与注册表。

设计要点：
- 每个平台一个 Adapter，凭据未配置时 `configured()` 返回 False，
  编排器据此直接失败并给出人话提示，而不是半路报一堆 4xx；
- 转存能力拆为两段（阶段 3 跨平台的基础）：
    save()   他人分享 → 转存到自己网盘，返回 SavedRef
    share()  已保存文件 → 生成新分享链接
  同平台 transfer() = save + share（组合方法，保持旧行为）；
  跨平台 = 源盘 save → OpenList fs/copy → 目标盘 locate + share。
- 所有网络调用使用传入的 httpx.AsyncClient（必须 trust_env=False）。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

import httpx

from parser import ShareLink


class AdapterError(Exception):
    """平台侧业务失败（如提取码错误、风控、频控）。"""


class AdapterNotConfigured(AdapterError):
    """该平台凭据未配置。"""


class CapabilityError(AdapterError):
    """官方接口不提供该能力（非业务失败，非配置问题）。

    例如：阿里个人开放 API 不提供「保存他人分享」「创建分享」接口
    （2025-07 起个人开发者申请也已暂停）。编排器可据此做降级处理
    （如：文件已转存成功、仅分享链接无法生成）。
    """


@dataclass
class SavedRef:
    """save() 的产物：文件已在「自己」的网盘里。

    names     顶层文件/目录名（与 OpenList 挂载视图中可见名一致，
              跨平台复制时作为 fs/copy 的 names）
    mount_dir 保存在自己盘内的目录（OpenList 挂载路径的相对部分；
              空串表示根目录）
    refs      平台内部文件引用列表：
              baidu {"fs_id", "path"} / quark {"fid"} / ali {"file_id"}
    task_id   所属任务 id（编排器在 locate 后回填；local 适配器
              借此生成 /dl/<task_id>/... 下载链接，其余适配器忽略）
    dl_token  任务下载凭证（编排器回填；local 适配器优先用它生成
              /dl/<dl_token>/... 链接，使分享链接不泄露 task_id）
    """
    platform: str
    names: list[str]
    mount_dir: str = ""
    refs: list[dict] = field(default_factory=list)
    task_id: str = ""
    dl_token: str = ""
    # 「自分享」标记：save() 发现该分享是**用户本人**创建的（文件本就在盘内），
    # 走的是「定位盘内文件」降级路径、并未真正转存。组合转存模式下编排器据此
    # 拒绝「本人链接 + 同源目标」这种无意义组合（见 orchestrator._exec_one）。
    self_share: bool = False


@dataclass
class TransferResult:
    new_url: str
    password: str | None = None
    files_saved: int = 0
    message: str = ""
    details: dict = field(default_factory=dict)
    # 本机高速下载：实际落盘文件名列表（供 /dl 端点按任务隔离，防跨任务串档）
    files: list[str] = field(default_factory=list)


ProgressFn = Callable[[int, str], None]


def _noop_progress(_p: int, _m: str) -> None:
    pass


class Adapter(ABC):
    platform: str = ""
    label: str = ""

    def __init__(self, settings, client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings
        self._client = client  # None 时按需创建（生产）；测试注入 MockTransport 客户端

    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            from http_client import new_client
            self._client = new_client()
        return self._client

    @abstractmethod
    def configured(self) -> bool:
        """凭据是否已配置。"""

    @abstractmethod
    async def save(self, share: ShareLink, password: str | None,
                   progress: ProgressFn = _noop_progress) -> SavedRef:
        """把他人分享转存到自己网盘。"""

    @abstractmethod
    async def share(self, ref: SavedRef, progress: ProgressFn = _noop_progress) -> TransferResult:
        """对自己网盘内的文件生成新分享。"""

    @abstractmethod
    async def locate(self, names: list[str], mount_dir: str = "",
                     progress: ProgressFn = _noop_progress) -> SavedRef:
        """在自己网盘的 mount_dir 下按名定位文件（跨平台复制后的目标盘侧）。"""

    async def transfer(self, share: ShareLink, password: str | None,
                       progress: ProgressFn = _noop_progress) -> TransferResult:
        """同平台直转 = 转存 + 分享（组合方法）。

        把 SavedRef.self_share 透传到 TransferResult.details，供编排器在
        「组合转存」模式下识别并拒绝「本人链接 + 同源目标」的组合。
        """
        ref = await self.save(share, password, progress)
        res = await self.share(ref, progress)
        if getattr(ref, "self_share", False):
            res.details = {**(res.details or {}), "self_share": True}
        return res

    # ---- 公共小工具 ----

    @staticmethod
    def parse_cookie(raw: str) -> dict[str, str]:
        jar: dict[str, str] = {}
        for part in raw.split(";"):
            if "=" in part:
                k, _, v = part.strip().partition("=")
                if k:
                    jar[k] = v
        return jar


def build_registry(settings, client: httpx.AsyncClient | None = None) -> dict[str, Adapter]:
    """构建平台 -> 适配器 注册表。惰性 import 避免循环依赖。"""
    from adapters.ali import AliAdapter
    from adapters.baidu import BaiduAdapter
    from adapters.local import LocalAdapter
    from adapters.quark import QuarkAdapter
    from adapters.uc import UCAdapter

    adapters: dict[str, Adapter] = {}
    for cls in (BaiduAdapter, QuarkAdapter, AliAdapter, UCAdapter, LocalAdapter):
        adapters[cls.platform] = cls(settings, client=client)
    return adapters
