"""财报查询页面 — FastAPI 后端。

只读查询本机 astock 库(经 common.get_conn(),遵守 ASTOCK_DB_* 环境变量)。
启动:.venv/bin/uvicorn webapp.app:app --port 8500
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

# 根目录的 common.py(uvicorn 从仓库根启动时 cwd 在 sys.path,这里再保险一层)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import get_conn  # noqa: E402

app = FastAPI(title="财报查询")

# market → 表名映射。has_currency=False 表示无币种列(A股恒为 CNY)
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


_STATIC = Path(__file__).resolve().parent / "static"


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(_STATIC / "index.html")
