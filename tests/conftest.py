"""pytest 共享配置：把 app 目录加入 sys.path（扁平布局），并固定测试环境变量。"""
import os
import sys
import tempfile
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent.parent / "app"
sys.path.insert(0, str(APP_DIR))

_TMP = tempfile.mkdtemp(prefix="pan-transfer-test-")
# ⚠️ 与开发机的 .env 彻底隔离：config._load_dotenv() 会读 cwd 下的 .env，一旦那里配了
# APP_PASSWORD，整套 API 用例都会被 require_token 打回 401（2026-09-19 实测踩到）。
# 把 ENV_FILE 指向不存在的路径 = 不加载任何 .env，环境完全由下面这些 setdefault 决定。
os.environ.setdefault("ENV_FILE", os.path.join(_TMP, "no-such.env"))
os.environ.setdefault("DB_PATH", os.path.join(_TMP, "tasks.db"))
os.environ.setdefault("CACHE_DIR", os.path.join(_TMP, "cache"))
os.environ.setdefault("LOCAL_DIR", os.path.join(_TMP, "downloads"))
os.environ.setdefault("OPENLIST_BASE", "http://127.0.0.1:9")  # 不可达端口
# 提交即校验（fail-fast）会检查目标平台 configured() 与 OpenList 中继就绪，
# 故测试环境提供「全部已配置」的假凭据（真实探测仍在编排器内进行）。
os.environ.setdefault("OPENLIST_TOKEN", "test-openlist-token")
# 测试环境关掉启动期的 OpenList 孤儿任务回收（会尝试连不可达端口，拖慢每个 TestClient）
os.environ.setdefault("RELAY_REAP_ON_BOOT", "0")
os.environ.setdefault("BAIDU_COOKIE", "BDUSS=test-bduss")
os.environ.setdefault("QUARK_COOKIE", "__puus=test-quark")
os.environ.setdefault("UC_COOKIE", "__puus=test-uc")
os.environ.setdefault("ALI_REFRESH_TOKEN", "test-ali-rt")
# 双角色测试前提：管理员口令默认为空（本机 TestClient 自动视为管理员）
os.environ.setdefault("ADMIN_PASSWORD", "")
# 访问口令留空：非空时 require_token 会让所有 API 用例 401
os.environ.setdefault("APP_PASSWORD", "")
