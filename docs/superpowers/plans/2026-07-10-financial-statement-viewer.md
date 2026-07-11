# 财报查询页面 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 本机网页,输入股票名字/代码搜索(A股/港股/美股),展示近 3 年年报 + 今年各期的三大财务报表(摘要卡片 + 关键科目透视表,可展开全部科目)。

**Architecture:** 单文件 FastAPI 后端(`webapp/app.py`,复用根目录 `common.py` 的 `get_conn()` 直连 astock 库,只读)+ 无构建原生 JS 单页(`webapp/static/index.html`)。两个 JSON API(search / statements)+ 静态页。

**Tech Stack:** Python 3 + FastAPI + psycopg2(经 common.py)+ 原生 HTML/JS/CSS。测试:pytest + fastapi TestClient(httpx)。

**Spec:** `docs/superpowers/specs/2026-07-10-financial-statement-viewer-design.md`

## Global Constraints

- 数据库**只读**:所有 SQL 仅 SELECT,不写任何表
- 连接一律走 `common.get_conn()`(遵守 ASTOCK_DB_* 环境变量约定),用完 close
- venv 是 uv 管理的 `.venv`;安装用 `uv pip install ...`,运行用 `.venv/bin/...`
- 端口固定 8500;启动命令 `.venv/bin/uvicorn webapp.app:app --port 8500`
- 财报 jsonb 中**数值科目是 JSON number,元数据键(币种/类型/数据源/公告日期/是否审计/更新日期)是 JSON string** —— 用类型区分,不要维护元数据键黑名单
- jsonb 不保留插入序:全部科目视图的行序 = 关键科目在前(配置序),其余科目按 Python dict 遍历序
- 报告期口径:近 3 个自然年的年报(12-31)+ 当前自然年所有已披露报告期;"当前年"取 `date.today().year`
- 测试直连真实库,只读,无 mock

---

### Task 1: 依赖 + FastAPI 骨架 + /api/search

**Files:**
- Modify: `requirements.txt`
- Create: `webapp/app.py`
- Test: `tests/test_webapp.py`

**Interfaces:**
- Consumes: `common.get_conn()`(根目录 common.py,返回 psycopg2 连接)
- Produces: FastAPI 实例 `webapp.app:app`;`GET /api/search?q=` 返回 `[{code, name, market, has_statements}]`(Task 2 的测试与 Task 3 前端依赖此形状);`MARKETS` 常量(market → 表名映射,Task 2 复用)

- [ ] **Step 1: 安装依赖并登记**

```bash
uv pip install fastapi 'uvicorn[standard]' httpx pytest
```

在 `requirements.txt` 末尾追加:

```
fastapi>=0.110
uvicorn[standard]>=0.29
httpx>=0.27          # fastapi TestClient 依赖
pytest>=8.0
```

- [ ] **Step 2: 写失败测试**

创建 `tests/test_webapp.py`:

```python
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
```

- [ ] **Step 3: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/test_webapp.py -v`
Expected: 收集阶段即 FAIL —— `ModuleNotFoundError: No module named 'webapp'`

- [ ] **Step 4: 实现 webapp/app.py(骨架 + search)**

创建 `webapp/app.py`:

```python
"""财报查询页面 — FastAPI 后端。

只读查询本机 astock 库(经 common.get_conn(),遵守 ASTOCK_DB_* 环境变量)。
启动:.venv/bin/uvicorn webapp.app:app --port 8500
"""
from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

# 根目录的 common.py(uvicorn 从仓库根启动时 cwd 在 sys.path,这里再保险一层)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import get_conn  # noqa: E402

app = FastAPI(title="财报查询")

# market → 表名映射。currency=None 表示无币种列(A股恒为 CNY)
MARKETS = {
    "cn": {"basic": "stock_basic", "stmt": "fin_statement", "has_currency": False},
    "hk": {"basic": "hk_stock_basic", "stmt": "hk_fin_statement", "has_currency": True},
    "us": {"basic": "us_stock_basic", "stmt": "us_fin_statement", "has_currency": True},
}


@app.get("/api/search")
def search(q: str = Query(..., min_length=1, max_length=32)):
    """按名字/代码模糊搜索三个市场,A股优先,最多 10 条。"""
    pat = f"%{q.strip()}%"
    sql = " UNION ALL ".join(
        f"SELECT stock_code, name, '{m}' AS market, "
        f"EXISTS (SELECT 1 FROM {cfg['stmt']} f WHERE f.stock_code = s.stock_code) AS has_statements "
        f"FROM {cfg['basic']} s WHERE s.name ILIKE %s OR s.stock_code ILIKE %s"
        for m, cfg in MARKETS.items()
    )
    # A股在前,有财报的在前,名字短的在前(更可能是精确命中)
    sql = (f"SELECT * FROM ({sql}) t "
           f"ORDER BY (market <> 'cn'), (NOT has_statements), length(name), stock_code LIMIT 10")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [pat, pat] * len(MARKETS))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [{"code": r[0], "name": r[1], "market": r[2], "has_statements": r[3]} for r in rows]
```

- [ ] **Step 5: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/test_webapp.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add requirements.txt webapp/app.py tests/test_webapp.py
git commit -m "feat: 财报查询 webapp 骨架与股票搜索 API"
```

---

### Task 2: /api/statements(三大报表 + 摘要)

**Files:**
- Modify: `webapp/app.py`(追加常量与端点)
- Test: `tests/test_webapp.py`(追加测试)

**Interfaces:**
- Consumes: Task 1 的 `MARKETS`、`get_conn()`
- Produces: `GET /api/statements?market=&code=` 返回
  `{code, name, market, currency, periods: [ISO日期...],`
  ` summary: [{label, value}...],`
  ` statements: {income|balance|cashflow: {key_items: [科目...], rows: [{item, values: {期: 数}}...]}}}`
  (Task 3 前端依赖此形状;无财报 → 404 `{"detail": "该股票暂无财报数据"}`)

- [ ] **Step 1: 写失败测试(追加到 tests/test_webapp.py)**

```python
def test_statements_cn():
    r = client.get("/api/statements", params={"market": "cn", "code": "300308.SZ"})
    assert r.status_code == 200
    d = r.json()
    assert d["name"] == "中际旭创"
    assert d["currency"] == "CNY"
    # 近 3 年年报 + 今年各期
    assert "2025-12-31" in d["periods"] and "2026-03-31" in d["periods"]
    assert "2022-12-31" not in d["periods"]
    for stmt in ("income", "balance", "cashflow"):
        block = d["statements"][stmt]
        assert block["key_items"], stmt
        assert block["rows"], stmt
        # key_items 都真实存在于 rows
        row_names = {row["item"] for row in block["rows"]}
        assert set(block["key_items"]) <= row_names
    # 摘要:2026Q1 营收 195 亿左右
    rev = next(s for s in d["summary"] if s["label"] == "营业收入")
    assert 1.9e10 < rev["value"] < 2.0e10
    assert any(s["label"] == "资产负债率" for s in d["summary"])


def test_statements_hk():
    r = client.get("/api/statements", params={"market": "hk", "code": "00700.HK"})
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
```

注意:`test_statements_hk` 依赖 00700.HK 在库中有财报(hk_fin_statement 仅覆盖约 10 只)。先验证:
`psql -U zhu -d astock -c "SELECT count(*) FROM hk_fin_statement WHERE stock_code='00700.HK'"` —— 若为 0,换用
`psql -U zhu -d astock -c "SELECT DISTINCT stock_code FROM hk_fin_statement LIMIT 5"` 里任一代码。

- [ ] **Step 2: 跑测试确认失败**

Run: `.venv/bin/python -m pytest tests/test_webapp.py -v`
Expected: 新增 4 个 FAIL(404 Not Found —— 端点不存在;bad_market 一条可能 404 而非 422,同样算失败),原 4 个仍 PASS

- [ ] **Step 3: 实现(追加到 webapp/app.py 末尾)**

```python
from datetime import date  # 放到文件顶部 import 区
from typing import Literal  # 同上

# ---------------------------------------------------------------------------
# 关键科目配置:market → stmt_type → 有序候选键元组列表(取每组第一个存在的键)。
# 键名来自各市场 jsonb 实测(2026-07-10 抽样),三市场科目名不同,不要合并。
# ---------------------------------------------------------------------------
KEY_ITEMS = {
    "cn": {
        "income": [("营业总收入",), ("营业成本",), ("销售费用",), ("管理费用",),
                   ("研发费用",), ("财务费用",), ("营业利润",), ("利润总额",), ("净利润",),
                   ("归属于母公司所有者的净利润",), ("基本每股收益",)],
        "balance": [("货币资金",), ("应收票据及应收账款",), ("存货",), ("流动资产合计",),
                    ("固定资产净额", "固定资产"), ("资产总计",), ("短期借款",),
                    ("应付票据及应付账款",), ("流动负债合计",), ("长期借款",), ("负债合计",),
                    ("归属于母公司股东权益合计",), ("所有者权益(或股东权益)合计",)],
        "cashflow": [("经营活动产生的现金流量净额",), ("投资活动产生的现金流量净额",),
                     ("筹资活动产生的现金流量净额",), ("现金及现金等价物净增加额",),
                     ("期末现金及现金等价物余额",)],
    },
    "hk": {
        "income": [("营业总收入", "营业额"), ("销售成本",), ("毛利",), ("营业利润",),
                   ("融资成本",), ("税前利润",), ("所得税",), ("净利润",),
                   ("归属母公司净利润",), ("基本每股收益",)],
        "balance": [("现金及等价物",), ("存货",), ("应收账款",), ("流动资产合计",),
                    ("资产合计",), ("流动负债合计",), ("负债合计",),
                    ("归属于母公司股东权益合计",), ("少数股东权益",), ("股东权益合计",)],
        "cashflow": [("经营活动现金流量净额",), ("投资活动现金流量净额",),
                     ("融资活动现金流量净额",), ("现金及现金等价物净增加额",),
                     ("现金及现金等价物期末余额",)],
    },
    "us": {
        "income": [("总收入", "营业总收入"), ("毛利",), ("营业利润",), ("税前利润",),
                   ("所得税",), ("净利润",), ("归属于母公司股东净利润",),
                   ("基本每股收益",), ("稀释每股收益",)],
        "balance": [("现金和现金等价物", "现金及现金等价物和短期投资"), ("存货",),
                    ("流动资产合计",), ("资产合计",), ("流动负债合计",), ("负债合计",),
                    ("归属于母公司股东权益合计",), ("少数股东权益",), ("股东权益合计",)],
        "cashflow": [("经营活动现金流量净额",), ("投资活动现金流量净额",),
                     ("融资活动现金流量净额",), ("资本支出", "资本性支出"), ("自由现金流",),
                     ("现金及现金等价物净增加额",), ("现金及现金等价物期末余额",)],
    },
}

# 摘要卡片:显示名 → market → 候选键(在"最新报告期三表合并 dict"里查找)
SUMMARY_ITEMS = [
    ("营业收入", {"cn": ("营业总收入", "营业收入"), "hk": ("营业总收入", "营业额"),
                  "us": ("总收入", "营业总收入")}),
    ("归母净利润", {"cn": ("归属于母公司所有者的净利润", "净利润"),
                    "hk": ("归属母公司净利润", "净利润"),
                    "us": ("归属于母公司股东净利润", "净利润")}),
    ("基本每股收益", {"cn": ("基本每股收益",), "hk": ("基本每股收益",), "us": ("基本每股收益",)}),
    ("经营现金流净额", {"cn": ("经营活动产生的现金流量净额",),
                        "hk": ("经营活动现金流量净额",), "us": ("经营活动现金流量净额",)}),
    ("总资产", {"cn": ("资产总计",), "hk": ("资产合计",), "us": ("资产合计",)}),
]
_DEBT_KEYS = {"cn": ("负债合计", "资产总计"), "hk": ("负债合计", "资产合计"),
              "us": ("负债合计", "资产合计")}  # 资产负债率 = 前者/后者


def _pick(d: dict, candidates) -> float | None:
    for k in candidates:
        if d.get(k) is not None:
            return d[k]
    return None


@app.get("/api/statements")
def statements(market: Literal["cn", "hk", "us"], code: str = Query(..., max_length=16)):
    cfg = MARKETS[market]
    year = date.today().year
    cur_cols = ", currency" if cfg["has_currency"] else ""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 近 3 个自然年的年报 + 今年所有报告期
            cur.execute(
                f"SELECT report_date, stmt_type, data{cur_cols} FROM {cfg['stmt']} "
                f"WHERE stock_code = %s AND ("
                f"  (EXTRACT(MONTH FROM report_date) = 12 AND EXTRACT(DAY FROM report_date) = 31"
                f"   AND report_date >= %s AND report_date < %s)"
                f"  OR report_date >= %s) "
                f"ORDER BY report_date",
                (code, date(year - 3, 12, 31), date(year, 1, 1), date(year, 1, 1)),
            )
            rows = cur.fetchall()
            cur.execute(f"SELECT name FROM {cfg['basic']} WHERE stock_code = %s", (code,))
            name_row = cur.fetchone()
    finally:
        conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="该股票暂无财报数据")

    periods = sorted({r[0].isoformat() for r in rows})
    currency = "CNY" if not cfg["has_currency"] else next(
        (r[3] for r in reversed(rows) if r[3]), None)

    # stmt_type → {科目: {期: 值}}(只留 JSON number;string 是元数据)
    by_stmt: dict[str, dict[str, dict[str, float]]] = {}
    for r in rows:
        period, stmt, data = r[0].isoformat(), r[1], r[2]
        items = by_stmt.setdefault(stmt, {})
        for k, v in data.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                items.setdefault(k, {})[period] = v

    statements_out = {}
    for stmt in ("income", "balance", "cashflow"):
        items = by_stmt.get(stmt, {})
        key_items = [next((k for k in group if k in items), None)
                     for group in KEY_ITEMS[market][stmt]]
        key_items = [k for k in key_items if k]
        ordered = key_items + [k for k in items if k not in key_items]
        statements_out[stmt] = {
            "key_items": key_items,
            "rows": [{"item": k, "values": items[k]} for k in ordered],
        }

    # 摘要:最新报告期,三表值合并后按配置取数
    latest = periods[-1]
    merged = {k: pv[latest]
              for items in by_stmt.values() for k, pv in items.items() if latest in pv}
    summary = [{"label": label, "value": _pick(merged, keys[market])}
               for label, keys in SUMMARY_ITEMS]
    debt, asset = (_pick(merged, (k,)) for k in _DEBT_KEYS[market])
    ratio = round(debt / asset * 100, 2) if debt is not None and asset else None
    summary.append({"label": "资产负债率", "value": ratio})

    return {"code": code, "name": name_row[0] if name_row else code, "market": market,
            "currency": currency, "latest_period": latest, "periods": periods,
            "summary": summary, "statements": statements_out}
```

注意:`from datetime import date` 与 `from typing import Literal` 要放到文件顶部 import 区,不要留在类体中间。psycopg2 对 jsonb 列默认返回已解析的 Python dict,无需手动 json.loads。

- [ ] **Step 4: 跑测试确认通过**

Run: `.venv/bin/python -m pytest tests/test_webapp.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add webapp/app.py tests/test_webapp.py
git commit -m "feat: 财报三大报表查询 API(近3年年报+今年各期,关键科目+全科目)"
```

---

### Task 3: 前端页面 + 静态服务

**Files:**
- Modify: `webapp/app.py`(挂静态目录与首页路由)
- Create: `webapp/static/index.html`

**Interfaces:**
- Consumes: Task 1/2 的两个 API(形状见各 task 的 Produces)
- Produces: `GET /` 返回单页应用

- [ ] **Step 1: 挂静态页(webapp/app.py)**

顶部 import 区补:

```python
from fastapi.responses import FileResponse
```

文件末尾追加:

```python
_STATIC = Path(__file__).resolve().parent / "static"


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(_STATIC / "index.html")
```

- [ ] **Step 2: 创建 webapp/static/index.html**

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>财报查询</title>
<style>
  :root { --border:#e2e2e2; --muted:#888; --neg:#c0392b; --accent:#2563eb; }
  * { box-sizing:border-box; }
  body { font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
         margin:0; background:#f7f7f8; color:#222; }
  .wrap { max-width:1100px; margin:0 auto; padding:24px 16px 60px; }
  h1 { font-size:20px; margin:0 0 16px; }
  #err { display:none; background:#fdecea; color:var(--neg); padding:10px 14px;
         border-radius:8px; margin-bottom:12px; }
  /* 搜索 */
  .searchbox { position:relative; max-width:480px; }
  #q { width:100%; padding:10px 14px; font-size:16px; border:1px solid var(--border);
       border-radius:10px; outline:none; }
  #q:focus { border-color:var(--accent); }
  #sug { position:absolute; top:100%; left:0; right:0; z-index:10; background:#fff;
         border:1px solid var(--border); border-radius:10px; margin-top:4px;
         box-shadow:0 4px 16px rgba(0,0,0,.08); display:none; overflow:hidden; }
  .sug-item { padding:9px 14px; cursor:pointer; display:flex; gap:8px; align-items:center; }
  .sug-item:hover { background:#f0f4ff; }
  .sug-item.dim { color:var(--muted); }
  .badge { font-size:11px; padding:1px 7px; border-radius:99px; background:#eef;
           color:var(--accent); flex:none; }
  .sug-item .code { color:var(--muted); font-size:13px; margin-left:auto; }
  /* 摘要卡片 */
  #head { margin:22px 0 10px; font-size:17px; font-weight:600; }
  #head .sub { font-weight:400; color:var(--muted); font-size:13px; margin-left:8px; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
           gap:10px; margin-bottom:22px; }
  .card { background:#fff; border:1px solid var(--border); border-radius:12px; padding:12px 14px; }
  .card .lbl { font-size:12px; color:var(--muted); }
  .card .val { font-size:19px; font-weight:650; margin-top:4px; }
  /* 报表 tab + 表格 */
  .tabs { display:flex; gap:6px; margin-bottom:-1px; }
  .tab { padding:8px 18px; border:1px solid var(--border); border-bottom:none;
         border-radius:10px 10px 0 0; background:#efefef; cursor:pointer; font-size:14px; }
  .tab.on { background:#fff; font-weight:600; }
  .panel { background:#fff; border:1px solid var(--border); border-radius:0 12px 12px 12px;
           padding:14px; overflow-x:auto; }
  table { border-collapse:collapse; width:100%; font-size:13.5px; }
  th, td { padding:7px 12px; border-bottom:1px solid #f0f0f0; text-align:right;
           white-space:nowrap; }
  th:first-child, td:first-child { text-align:left; position:sticky; left:0; background:#fff; }
  thead th { color:var(--muted); font-weight:500; border-bottom:1px solid var(--border); }
  td.neg { color:var(--neg); }
  #expand { margin-top:10px; background:none; border:none; color:var(--accent);
            cursor:pointer; font-size:13.5px; padding:4px 0; }
  .note { color:var(--muted); font-size:13px; padding:30px 0; text-align:center; }
</style>
</head>
<body>
<div class="wrap">
  <h1>财报查询 <span class="sub" style="font-weight:400;font-size:13px;color:#888">A股 · 港股 · 美股</span></h1>
  <div id="err"></div>
  <div class="searchbox">
    <input id="q" placeholder="输入股票名字或代码,如:旭创 / 300308" autocomplete="off">
    <div id="sug"></div>
  </div>
  <div id="content"></div>
</div>
<script>
const $ = s => document.querySelector(s);
const MK = { cn:'A股', hk:'港股', us:'美股' };
const CUR = { CNY:'人民币', HKD:'港元', USD:'美元' };
let cur = null;          // 当前 statements 响应
let tab = 'income';
let expanded = false;

function err(msg){ const e=$('#err'); e.textContent=msg; e.style.display=msg?'block':'none'; }

/* ---------- 搜索 ---------- */
let timer=null;
$('#q').addEventListener('input', e => {
  clearTimeout(timer);
  const q = e.target.value.trim();
  if(!q){ $('#sug').style.display='none'; return; }
  timer = setTimeout(()=>doSearch(q), 300);
});
async function doSearch(q){
  try{
    const r = await fetch('/api/search?q='+encodeURIComponent(q));
    if(!r.ok) throw new Error('搜索失败 HTTP '+r.status);
    render_sug(await r.json());
  }catch(ex){ err(ex.message); }
}
function render_sug(items){
  const box = $('#sug');
  box.innerHTML = '';
  if(!items.length){ box.innerHTML='<div class="sug-item dim">无匹配</div>'; }
  items.forEach(it=>{
    const d = document.createElement('div');
    d.className = 'sug-item' + (it.has_statements?'':' dim');
    d.innerHTML = `<span class="badge">${MK[it.market]}</span><span>${it.name}</span>` +
      (it.has_statements?'':'<span style="font-size:12px">(无财报)</span>') +
      `<span class="code">${it.code}</span>`;
    d.onclick = ()=>{ box.style.display='none'; $('#q').value=`${it.name} ${it.code}`; load(it); };
    box.appendChild(d);
  });
  box.style.display='block';
}
document.addEventListener('click', e=>{ if(!e.target.closest('.searchbox')) $('#sug').style.display='none'; });

/* ---------- 加载与渲染 ---------- */
async function load(it){
  err('');
  $('#content').innerHTML = '<div class="note">加载中…</div>';
  try{
    const r = await fetch(`/api/statements?market=${it.market}&code=${encodeURIComponent(it.code)}`);
    if(r.status===404){
      $('#content').innerHTML = '<div class="note">该股票暂无财报数据(数据库目前仅覆盖部分港/美股财报)</div>';
      return;
    }
    if(!r.ok) throw new Error('查询失败 HTTP '+r.status);
    cur = await r.json(); tab='income'; expanded=false;
    render();
  }catch(ex){ err(ex.message); $('#content').innerHTML=''; }
}

const isPerShare = k => k.includes('每股') || k.includes('股息') || k.includes('派息');
function fmt(v, item){
  if(v===null || v===undefined) return '—';
  if(item==='资产负债率') return v.toFixed(2)+'%';
  if(isPerShare(item)) return v.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');
  return (v/1e8).toLocaleString('zh-CN',{maximumFractionDigits:2});  // 亿
}

function render(){
  const unit = CUR[cur.currency] || cur.currency || '';
  const cards = cur.summary.map(s=>{
    const v = fmt(s.value, s.label);
    const suffix = (s.label==='资产负债率'||isPerShare(s.label)||s.value==null) ? '' : ' 亿';
    return `<div class="card"><div class="lbl">${s.label}</div>
      <div class="val${s.value<0?' neg':''}">${v}${suffix}</div></div>`;
  }).join('');
  $('#content').innerHTML = `
    <div id="head">${cur.name} <span class="sub">${cur.code} · ${MK[cur.market]}
      · 币种:${unit} · 最新报告期:${cur.latest_period}</span></div>
    <div class="cards">${cards}</div>
    <div class="tabs">
      ${[['income','利润表'],['balance','资产负债表'],['cashflow','现金流量表']]
        .map(([k,l])=>`<div class="tab${k===tab?' on':''}" data-t="${k}">${l}</div>`).join('')}
    </div>
    <div class="panel"><div id="tbl"></div>
      <button id="expand"></button></div>`;
  document.querySelectorAll('.tab').forEach(t=>t.onclick=()=>{ tab=t.dataset.t; render(); });
  renderTable();
}

function renderTable(){
  const block = cur.statements[tab];
  const rows = expanded ? block.rows
                        : block.rows.filter(r=>block.key_items.includes(r.item));
  const head = `<tr><th>科目(单位:亿,每股类除外)</th>` +
    cur.periods.map(p=>`<th>${p}</th>`).join('') + '</tr>';
  const body = rows.map(r=>'<tr><td>'+r.item+'</td>'+cur.periods.map(p=>{
      const v = r.values[p];
      return `<td${v<0?' class="neg"':''}>${fmt(v, r.item)}</td>`;
    }).join('')+'</tr>').join('');
  $('#tbl').innerHTML = rows.length
    ? `<table><thead>${head}</thead><tbody>${body}</tbody></table>`
    : '<div class="note">本报表无数据</div>';
  const btn = $('#expand');
  btn.textContent = expanded ? '收起,只看关键科目' : `展开全部科目(共 ${block.rows.length} 项)`;
  btn.onclick = ()=>{ expanded=!expanded; renderTable(); };
}
</script>
</body>
</html>
```

- [ ] **Step 3: 回归 API 测试**

Run: `.venv/bin/python -m pytest tests/test_webapp.py -v`
Expected: 8 passed(静态路由不影响 API)

- [ ] **Step 4: 手工验证(启动 + curl + 浏览器)**

```bash
.venv/bin/uvicorn webapp.app:app --port 8500 &
sleep 2
curl -s localhost:8500/ | head -5                      # 应输出 <!DOCTYPE html>
curl -s 'localhost:8500/api/search?q=旭创'              # 应含 300308.SZ
```

浏览器打开 `http://localhost:8500`,核对:
1. 输入"旭创"→ 下拉出现"A股 中际旭创 300308.SZ"→ 点击后摘要卡片与三表渲染
2. 切换三个 tab;点"展开全部科目"行数变多、再点收起
3. 输入"腾讯"选 00700.HK(港股徽标、币种 HKD);输入乱码显示"无匹配"
4. 选一只无财报的港股 → 显示"该股票暂无财报数据"提示
验证完 `kill %1` 停掉服务。

- [ ] **Step 5: Commit**

```bash
git add webapp/app.py webapp/static/index.html
git commit -m "feat: 财报查询前端页面(搜索下拉+摘要卡片+三表透视/展开)"
```

---

## Self-Review 记录

- 覆盖检查:spec 的搜索/摘要/透视表/展开/币种/负数红/空态/错误条 → Task 1-3 全覆盖;"科目按 jsonb 原序"因 jsonb 不保序调整为"关键科目在前、其余按返回序"(已写入 Global Constraints)
- 占位符:无 TBD/伪代码;所有步骤含完整代码与命令
- 接口一致性:`/api/statements` 响应字段(periods/summary/statements.rows[].item/values)在 Task 2 Produces、Task 2 实现、Task 3 前端三处一致;`latest_period` 已在实现与前端同时出现
