"""UC 网盘适配器（Cookie 方案）。

UC 网盘（drive.uc.cn）与夸克同属阿里系，开放接口同构：
pc-api.uc.cn/1/clouddrive/* 的端点、参数、响应结构与夸克完全一致
（OpenList quark_uc 驱动即以同一套实现支持两家，仅切换
pr 参数：夸克=ucpro / UC=UCBrowser）。
因此直接继承 QuarkAdapter，覆写域名、品牌参数与凭据来源。

凭据：登录 drive.uc.cn 后从浏览器开发者工具复制整串 Cookie
（必须包含 __pus / __puus），填入 .env 的 UC_COOKIE。
注意：UC 要求账号完成实名认证后才能创建分享（code=32011）。
"""
from __future__ import annotations

from adapters.quark import QuarkAdapter


class UCAdapter(QuarkAdapter):
    platform = "uc"
    label = "UC 网盘"
    BASE = "https://pc-api.uc.cn/1/clouddrive"
    PR = {"pr": "UCBrowser", "fr": "pc"}
    WEB_BASE = "https://drive.uc.cn"        # UC 分享链接域名
    COOKIE_KEY = "uc_cookie"
    # _ERRNO_MSG 继承 QuarkAdapter（含 41017 自转存 / 32011 实名认证等人话映射）
