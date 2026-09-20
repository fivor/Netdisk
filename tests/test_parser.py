import pytest

from parser import ParseError, parse_share_text, validate_password


class TestParse:
    def test_baidu_with_password_inline(self):
        text = "链接：https://pan.baidu.com/s/1AbCdEfGh 提取码：x9K2"
        s = parse_share_text(text)
        assert s.platform == "baidu"
        assert s.share_id == "AbCdEfGh"  # 去前导 1
        assert s.password == "x9K2"

    def test_baidu_share_init_url(self):
        s = parse_share_text("https://pan.baidu.com/share/init?surl=QwErTy12 (码: abcd)")
        assert s.platform == "baidu"
        assert s.share_id == "QwErTy12"
        assert s.password == "abcd"

    def test_baidu_without_leading_one(self):
        s = parse_share_text("pan.baidu.com/s/abcdef1234")
        assert s.platform == "baidu"
        assert s.share_id == "abcdef1234"

    def test_quark_no_password(self):
        s = parse_share_text("快来看看 https://pan.quark.cn/s/88f1a2b3c4d5 好东西")
        assert s.platform == "quark"
        assert s.share_id == "88f1a2b3c4d5"
        assert s.password is None

    def test_quark_with_password_label(self):
        s = parse_share_text("https://pan.quark.cn/s/88f1a2b3c4d5\n提取码: Qz7W")
        assert s.password == "Qz7W"

    def test_ali_alipan(self):
        s = parse_share_text("https://www.alipan.com/s/gCqXyz123ab 提取码：88nx")
        assert s.platform == "ali"
        assert s.share_id == "gCqXyz123ab"
        assert s.password == "88nx"

    def test_ali_legacy_domain(self):
        s = parse_share_text("https://www.aliyundrive.com/s/9mLdQwEr")
        assert s.platform == "ali"
        assert s.share_id == "9mLdQwEr"

    def test_uc(self):
        s = parse_share_text("https://drive.uc.cn/s/fid12345678 密码：kk9p")
        assert s.platform == "uc"
        assert s.password == "kk9p"

    def test_first_link_wins(self):
        text = "第一个 https://pan.baidu.com/s/1aaaaaa 第二个 https://pan.quark.cn/s/bbbbbb"
        s = parse_share_text(text)
        assert s.platform == "baidu"

    def test_password_without_url_ignored_for_wrong_platform_format(self):
        s = parse_share_text("https://pan.quark.cn/s/abc123 提取码：a1b2")
        assert s.password == "a1b2"

    def test_empty_raises(self):
        with pytest.raises(ParseError):
            parse_share_text("   ")

    def test_no_link_raises(self):
        with pytest.raises(ParseError):
            parse_share_text("今天天气不错")

    def test_validate_password_user_input_wins(self):
        s = parse_share_text("https://pan.quark.cn/s/abc123 提取码：a1b2")
        assert validate_password(s, "zzzz") == "zzzz"
        assert validate_password(s, None) == "a1b2"

    def test_code_prefix_lookbehind(self):
        """「码」前缀不应误吞「验证码/优惠码」。"""
        s = parse_share_text("https://pan.quark.cn/s/abc123 验证码: 8z8z")
        assert s.password is None
        s2 = parse_share_text("https://pan.quark.cn/s/abc123 (码: 9k9k)")
        assert s2.password == "9k9k"

    def test_baidu_pwd_in_query(self):
        """百度 App 分享文案的 "?pwd=xxxx" 形态应自动识别提取码。"""
        s = parse_share_text("https://pan.baidu.com/s/13FwMevJykUtaEY7pKxQtnw?pwd=ussq")
        assert s.platform == "baidu"
        assert s.share_id == "3FwMevJykUtaEY7pKxQtnw"
        assert s.password == "ussq"
