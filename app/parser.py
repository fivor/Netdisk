"""分享链接解析：从任意粘贴文本中识别网盘平台、分享 ID 与提取码。"""
from __future__ import annotations

import re
from dataclasses import dataclass


class ParseError(ValueError):
    """无法从文本中识别出分享链接。"""


@dataclass
class ShareLink:
    platform: str          # baidu / quark / ali / uc
    share_id: str          # 平台内分享标识（百度为去掉前导 1 的 surl）
    password: str | None = None
    raw: str = ""


# 平台 URL 模式（每组可含多个正则；捕获组 = 分享 ID）
_PATTERNS: list[tuple[str, list[re.Pattern[str]]]] = [
    ("baidu", [
        # https://pan.baidu.com/s/1AbCdEf  → surl = "AbCdEf"（去前导 1）
        re.compile(r"pan\.baidu\.com/s/1?([\w\-]{6,})"),
        # https://pan.baidu.com/share/init?surl=AbCdEf
        re.compile(r"pan\.baidu\.com/share/init\?surl=1?([\w\-]{6,})"),
    ]),
    ("quark", [re.compile(r"pan\.quark\.cn/s/([\w]+)")]),
    ("ali", [re.compile(r"(?:www\.)?(?:alipan|aliyundrive)\.com/s/([\w]+)")]),
    ("uc", [re.compile(r"drive\.uc\.cn/s/([\w]+)")]),
]

# 提取码：常见前缀 + 4 位字母数字（「码」前缀排除「验证码/优惠码」误报）
# 分隔符含 "="：兼容百度 App 分享文案的 "?pwd=xxxx" 形态
# pwd/passcode/code 三个英文备择要求**词首边界**（前面不能是字母）——否则 URL 查询串里
# 的 source=abcd / encode=xxxx 会把 "code" 从 "source"/"encode" 里抠出来，提取码张冠李戴。
_PASSWORD = re.compile(
    r"(?:提取码|访问码|密\s?码|(?<!证)(?<!惠)码|(?<![a-zA-Z])pwd|(?<![a-zA-Z])passcode|(?<![a-zA-Z])code)"
    r"\s*[:：\s=]\s*([a-zA-Z0-9]{4})\b",
    re.IGNORECASE,
)

PLATFORM_LABELS = {
    "baidu": "百度网盘",
    "quark": "夸克网盘",
    "ali": "阿里云盘",
    "uc": "UC 网盘",
}


def parse_share_text(text: str) -> ShareLink:
    """从用户粘贴的文本中解析分享链接与提取码。

    规则：
    - 多个链接时取第一个；
    - 提取码在整个文本中寻找（很多分享把提取码放在链接外）。
    """
    if not text or not text.strip():
        raise ParseError("内容为空，请粘贴分享链接")

    for platform, patterns in _PATTERNS:
        m = None
        for pattern in patterns:
            m = pattern.search(text)
            if m:
                break
        if not m:
            continue
        pwd_m = _PASSWORD.search(text)
        return ShareLink(
            platform=platform,
            share_id=m.group(1).strip(),
            password=pwd_m.group(1) if pwd_m else None,
            raw=text.strip(),
        )

    raise ParseError("未识别到支持的网盘链接（支持百度 / 夸克 / 阿里云盘 / UC）")


def validate_password(share: ShareLink, provided: str | None) -> str | None:
    """合并用户补充输入的提取码（用户输入优先于文本解析结果）。

    provided 先 strip：纯空白的输入视同未提供，回退到文本解析结果，
    而不是把空串当成「用户确认无提取码」。
    """
    p = (provided or "").strip()
    if p:
        return p
    return share.password
