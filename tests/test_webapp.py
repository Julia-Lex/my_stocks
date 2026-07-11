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


def test_search_hk_by_chinese_name():
    # 港股中文名(name_cn,东财/A+H 对照口径):搜"胜宏"应同时出 A股和 H股
    r = client.get("/api/search", params={"q": "胜宏"})
    assert r.status_code == 200
    codes = {i["code"] for i in r.json()}
    assert {"300476.SZ", "02476.HK"} <= codes
    hk = next(i for i in r.json() if i["code"] == "02476.HK")
    assert hk["name"] == "胜宏科技"   # 有中文名时展示中文


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


def test_fundamental_cn():
    r = client.get("/api/fundamental", params={"market": "cn", "code": "300308.SZ"})
    assert r.status_code == 200
    d = r.json()
    assert d["name"] == "中际旭创"
    assert d["industry"] == "通信设备"
    v = d["valuation"]
    assert v and v["pe_ttm"] > 0 and v["pb"] > 0 and v["total_mv"] > 1e11
    i = d["indicator"]
    assert i and i["roe"] is not None and i["report_date"] >= "2026-03-31"


def test_fundamental_hk():
    r = client.get("/api/fundamental", params={"market": "hk", "code": "00001.HK"})
    assert r.status_code == 200
    d = r.json()
    assert d["valuation"] is None          # 估值表仅 A股
    assert d["indicator"] and d["indicator"]["eps"] is not None


def test_fundamental_not_found():
    r = client.get("/api/fundamental", params={"market": "cn", "code": "999999.SZ"})
    assert r.status_code == 404


def test_kline_cn():
    r = client.get("/api/kline", params={"market": "cn", "code": "300308.SZ", "days": 250})
    assert r.status_code == 200
    d = r.json()
    assert d["adjusted"] is True           # 有复权因子
    bars = d["bars"]
    assert len(bars) == 250
    assert bars == sorted(bars, key=lambda b: b["d"])   # 升序
    for b in (bars[0], bars[-1]):
        assert b["l"] <= b["o"] <= b["h"] and b["l"] <= b["c"] <= b["h"]
        assert b["v"] > 0
    # 最新一根不晚于今天,且在 2026 年
    assert bars[-1]["d"].startswith("2026")


def test_kline_not_found():
    r = client.get("/api/kline", params={"market": "cn", "code": "999999.SZ"})
    assert r.status_code == 404


def test_kline_week_month():
    from datetime import date as _date

    day = client.get("/api/kline",
                     params={"market": "cn", "code": "300308.SZ", "days": 60}).json()
    for period, key in (("week", lambda d: d.isocalendar()[:2]),   # 每根 bar 一个自然周
                        ("month", lambda d: (d.year, d.month))):   # 每根 bar 一个自然月
        r = client.get("/api/kline", params={"market": "cn", "code": "300308.SZ",
                                             "days": 260, "period": period})
        assert r.status_code == 200
        bars = r.json()["bars"]
        assert 100 < len(bars) <= 260
        assert bars == sorted(bars, key=lambda b: b["d"])
        keys = [key(_date.fromisoformat(b["d"])) for b in bars]
        assert len(keys) == len(set(keys)), f"{period} 粒度不对(仍是日线?)"
        last = bars[-1]
        assert last["l"] <= last["o"] <= last["h"] and last["l"] <= last["c"] <= last["h"]
        assert last["v"] > 0
        # 周/月线经 hfq→qfq 换算后,最新收盘应与日线最新收盘同量级(同一周/月内相等)
        assert abs(last["c"] - day["bars"][-1]["c"]) / day["bars"][-1]["c"] < 0.05


def test_kline_penny_stock_not_rounded_to_zero():
    # 00661.HK 复权后价格 <0.001,3 位小数舍入会变 0(2026-07-11 实测缺陷)
    r = client.get("/api/kline", params={"market": "hk", "code": "00661.HK",
                                         "period": "week", "days": 260})
    assert r.status_code == 200
    assert all(b["c"] > 0 and b["o"] > 0 for b in r.json()["bars"])


def test_kline_bad_period():
    r = client.get("/api/kline", params={"market": "cn", "code": "300308.SZ",
                                         "period": "hour"})
    assert r.status_code == 422
