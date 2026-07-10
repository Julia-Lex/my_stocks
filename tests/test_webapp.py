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
