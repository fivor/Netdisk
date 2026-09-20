"""共享 HTTP 客户端。

⚠️ 代理隔离铁律（方案 5.3 / 环境体检报告）：
所有对外 HTTP 必须用这里的工厂创建客户端 —— `trust_env=False`，
保证绝不读取 HTTP_PROXY 等环境变量，网盘 API 全部走家宽直连，
避免阿里/百度风控把代理出口 IP 当成异常环境。
"""
from __future__ import annotations

import httpx

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def new_client(**kwargs) -> httpx.AsyncClient:
    """创建一个「永不走代理」的异步客户端。测试可注入 transport。"""
    kwargs.setdefault("trust_env", False)
    kwargs.setdefault("timeout", httpx.Timeout(30.0, connect=10.0))
    kwargs.setdefault("follow_redirects", True)
    kwargs.setdefault("headers", {"User-Agent": USER_AGENT})
    return httpx.AsyncClient(**kwargs)
