"""财报查询 webapp 的 API 测试。直连本机 astock 库(只读)。"""
from fastapi.testclient import TestClient

from webapp.app import app

client = TestClient(app)


def test_search_by_name():
    r = client.get("/api/search", params={"q": "旭创"})
    assert r.status_code == 200
    items = r.json()
    assert any(i["code"] == "300308.SZ" and i["market"] == "cn" for i in items)
    hit = next(i for i in items if i["code"] == "300308.SZ")
    assert hit["name"] == "中际旭创"
    assert hit["has_statements"] is True


def test_search_by_code():
    r = client.get("/api/search", params={"q": "00700"})
    assert r.status_code == 200
    assert any(i["code"] == "00700.HK" and i["market"] == "hk" for i in r.json())


def test_search_no_match():
    r = client.get("/api/search", params={"q": "zzz不存在的股票zzz"})
    assert r.status_code == 200
    assert r.json() == []


def test_search_limit_10():
    r = client.get("/api/search", params={"q": "银行"})
    assert len(r.json()) <= 10


def test_statements_cn_annual_default():
    r = client.get("/api/statements", params={"market": "cn", "code": "300308.SZ"})
    assert r.status_code == 200
    d = r.json()
    assert d["name"] == "中际旭创"
    assert d["currency"] == "CNY"
    # 默认年度:最近 5 个年报,不含季报
    assert d["periods"] == ["2021-12-31", "2022-12-31", "2023-12-31",
                            "2024-12-31", "2025-12-31"]
    for stmt in ("income", "balance", "cashflow"):
        block = d["statements"][stmt]
        assert block["key_items"], stmt
        assert block["rows"], stmt
        # key_items 都真实存在于 rows
        row_names = {row["item"] for row in block["rows"]}
        assert set(block["key_items"]) <= row_names
    # 利润表前三行:营收、净利润、归母净利润
    assert d["statements"]["income"]["key_items"][:3] == [
        "营业总收入", "净利润", "归属于母公司所有者的净利润"]
    # 摘要取最新期(2025 年报):营收 382 亿左右
    assert d["latest_period"] == "2025-12-31"
    rev = next(s for s in d["summary"] if s["label"] == "营业收入")
    assert 3.7e10 < rev["value"] < 3.9e10
    assert any(s["label"] == "资产负债率" for s in d["summary"])


def test_statements_cn_quarterly():
    r = client.get("/api/statements",
                   params={"market": "cn", "code": "300308.SZ", "freq": "quarterly"})
    assert r.status_code == 200
    d = r.json()
    # 季度:近 5 个自然年(含今年)的全部报告期
    assert "2026-03-31" in d["periods"]
    assert "2025-09-30" in d["periods"]
    assert "2022-03-31" in d["periods"]
    assert "2021-12-31" not in d["periods"]
    assert d["latest_period"] == "2026-03-31"
    # 摘要:2026Q1 营收 195 亿左右
    rev = next(s for s in d["summary"] if s["label"] == "营业收入")
    assert 1.9e10 < rev["value"] < 2.0e10


def test_statements_bad_freq():
    r = client.get("/api/statements",
                   params={"market": "cn", "code": "300308.SZ", "freq": "monthly"})
    assert r.status_code == 422


def test_statements_hk():
    # 00001.HK(长和)是 hk_fin_statement 覆盖的少数港股之一
    r = client.get("/api/statements", params={"market": "hk", "code": "00001.HK"})
    assert r.status_code == 200
    d = r.json()
    assert d["currency"]  # 港股有币种列
    assert d["statements"]["income"]["rows"]


def test_statements_no_data():
    r = client.get("/api/statements", params={"market": "hk", "code": "99999.HK"})
    assert r.status_code == 404
    assert r.json()["detail"] == "该股票暂无财报数据"


def test_statements_bad_market():
    r = client.get("/api/statements", params={"market": "xx", "code": "300308.SZ"})
    assert r.status_code == 422
