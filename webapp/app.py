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

# market → 表名映射。has_currency=False 表示无币种列(A股恒为 CNY)。
# name_expr:展示名——港股优先中文简称(name_cn,可能为空),回退英文简称
MARKETS = {
    "cn": {"basic": "stock_basic", "stmt": "fin_statement", "has_currency": False,
           "name_expr": "name", "search_cols": ["name", "stock_code"]},
    "hk": {"basic": "hk_stock_basic", "stmt": "hk_fin_statement", "has_currency": True,
           "name_expr": "COALESCE(name_cn, name)",
           "search_cols": ["name", "name_cn", "stock_code"]},
    "us": {"basic": "us_stock_basic", "stmt": "us_fin_statement", "has_currency": True,
           "name_expr": "name", "search_cols": ["name", "stock_code"]},
}


@app.get("/api/search")
def search(q: str = Query(..., min_length=1, max_length=32)):
    """按名字/代码模糊搜索三个市场(港股含中文名),A股优先,最多 10 条。"""
    pat = f"%{q.strip()}%"
    parts, params = [], []
    for m, cfg in MARKETS.items():
        cond = " OR ".join(f"s.{col} ILIKE %s" for col in cfg["search_cols"])
        parts.append(
            f"SELECT stock_code, {cfg['name_expr']} AS name, '{m}' AS market, "
            f"EXISTS (SELECT 1 FROM {cfg['stmt']} f WHERE f.stock_code = s.stock_code) AS has_statements "
            f"FROM {cfg['basic']} s WHERE {cond}")
        params += [pat] * len(cfg["search_cols"])
    # A股在前,有财报的在前,名字短的在前(更可能是精确命中)
    sql = (f"SELECT * FROM ({' UNION ALL '.join(parts)}) t "
           f"ORDER BY (market <> 'cn'), (NOT has_statements), length(name), stock_code LIMIT 10")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
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
        "income": [("营业总收入",), ("净利润",), ("归属于母公司所有者的净利润",),
                   ("营业成本",), ("销售费用",), ("管理费用",), ("研发费用",),
                   ("财务费用",), ("营业利润",), ("利润总额",), ("基本每股收益",)],
        "balance": [("货币资金",), ("应收票据及应收账款",), ("存货",), ("流动资产合计",),
                    ("固定资产净额", "固定资产"), ("资产总计",), ("短期借款",),
                    ("应付票据及应付账款",), ("流动负债合计",), ("长期借款",), ("负债合计",),
                    ("归属于母公司股东权益合计",), ("所有者权益(或股东权益)合计",)],
        "cashflow": [("经营活动产生的现金流量净额",), ("投资活动产生的现金流量净额",),
                     ("筹资活动产生的现金流量净额",), ("现金及现金等价物净增加额",),
                     ("期末现金及现金等价物余额",)],
    },
    "hk": {
        "income": [("营业总收入", "营业额"), ("净利润",), ("归属母公司净利润",),
                   ("销售成本",), ("毛利",), ("营业利润",), ("融资成本",),
                   ("税前利润",), ("所得税",), ("基本每股收益",)],
        "balance": [("现金及等价物",), ("存货",), ("应收账款",), ("流动资产合计",),
                    ("资产合计",), ("流动负债合计",), ("负债合计",),
                    ("归属于母公司股东权益合计",), ("少数股东权益",), ("股东权益合计",)],
        "cashflow": [("经营活动现金流量净额",), ("投资活动现金流量净额",),
                     ("融资活动现金流量净额",), ("现金及现金等价物净增加额",),
                     ("现金及现金等价物期末余额",)],
    },
    "us": {
        "income": [("总收入", "营业总收入"), ("净利润",), ("归属于母公司股东净利润",),
                   ("毛利",), ("营业利润",), ("税前利润",), ("所得税",),
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
def statements(market: Literal["cn", "hk", "us"], code: str = Query(..., max_length=16),
               freq: Literal["annual", "quarterly"] = "annual"):
    cfg = MARKETS[market]
    year = date.today().year
    cur_cols = ", currency" if cfg["has_currency"] else ""
    if freq == "annual":
        # 年度:最近 5 个年报(12-31)
        cond = ("EXTRACT(MONTH FROM report_date) = 12 AND EXTRACT(DAY FROM report_date) = 31"
                " AND report_date >= %s")
        since = date(year - 5, 12, 31)
    else:
        # 季度:近 5 个自然年(含今年)的全部报告期
        cond = "report_date >= %s"
        since = date(year - 4, 1, 1)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT report_date, stmt_type, data{cur_cols} FROM {cfg['stmt']} "
                f"WHERE stock_code = %s AND ({cond}) ORDER BY report_date",
                (code, since),
            )
            rows = cur.fetchall()
            cur.execute(f"SELECT {cfg['name_expr']} FROM {cfg['basic']} WHERE stock_code = %s",
                        (code,))
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


# ---------------------------------------------------------------------------
# 基本面 + K线
# ---------------------------------------------------------------------------
# 各市场行情/指标表。fin_indicator 三市场列基本一致(cn 多 industry,hk/us 多 currency)
_QUOTE_TABLES = {
    "cn": {"price": "daily_price", "adj": "adj_factor", "ind": "fin_indicator", "mv_prefix": ""},
    "hk": {"price": "hk_daily_price", "adj": "hk_adj_factor", "ind": "hk_fin_indicator", "mv_prefix": "hk_"},
    "us": {"price": "us_daily_price", "adj": "us_adj_factor", "ind": "us_fin_indicator", "mv_prefix": "us_"},
}

_IND_COLS = ["report_date", "eps", "bps", "roe", "roa", "gross_margin", "net_margin",
             "revenue", "revenue_yoy", "net_profit", "net_profit_yoy",
             "debt_ratio", "current_ratio"]
_VAL_COLS = ["trade_date", "pe", "pe_ttm", "pb", "ps_ttm", "dv_ratio", "total_mv"]

# 腾讯实时行情(qt.gtimg.cn)~ 分隔字段下标,2026-07-11 用 00700/00005/AAPL/MSFT 实测:
# 港股: f3=最新价 f39=PE(TTM) f58=PB f47=股息率% f44=总市值(亿) f75=币种
# 美股: f3=最新价 f39=PE(TTM) f47=EPS f44=总市值(亿) f35=币种(无 PB,用 价格/BPS 现算)
_TX_QUOTE_URL = "https://qt.gtimg.cn/q="


def _parse_tx_quote(market: str, text: str) -> dict | None:
    """解析腾讯行情响应为估值 dict(纯函数,便于测试)。字段缺失记 None。"""
    f = text.split("~")
    if len(f) < 60:
        return None

    def num(i):
        try:
            v = float(f[i])
            return v if v != 0 else None
        except (ValueError, IndexError):
            return None

    mv = num(44)
    out = {"trade_date": "实时", "pe": None, "pe_ttm": num(39), "ps_ttm": None,
           "total_mv": mv * 1e8 if mv else None, "price": num(3)}
    if market == "hk":
        out.update({"pb": num(58), "dv_ratio": num(47)})
    else:
        out.update({"pb": None, "dv_ratio": None})
    return out


def _fetch_live_valuation(market: str, code: str) -> dict | None:
    """港/美股实时估值(库里无估值表,取腾讯实时行情)。失败返回 None,页面留空。"""
    import requests

    sym = code.split(".")[0]
    tx_code = ("hk" if market == "hk" else "us") + sym
    try:
        resp = requests.get(_TX_QUOTE_URL + tx_code, timeout=5)
        resp.encoding = "gbk"
        return _parse_tx_quote(market, resp.text)
    except Exception:  # noqa: BLE001 — 实时源不可用不阻塞页面
        return None


def _row_dict(cols, row):
    if row is None:
        return None
    out = {}
    for c, v in zip(cols, row):
        out[c] = v.isoformat() if hasattr(v, "isoformat") else (float(v) if v is not None else None)
    return out


@app.get("/api/fundamental")
def fundamental(market: Literal["cn", "hk", "us"], code: str = Query(..., max_length=16)):
    """最新估值(仅 A股)+ 最新财务指标。"""
    cfg, qt = MARKETS[market], _QUOTE_TABLES[market]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {cfg['name_expr']} FROM {cfg['basic']} WHERE stock_code = %s",
                        (code,))
            name_row = cur.fetchone()
            if name_row is None:
                raise HTTPException(status_code=404, detail="未找到该股票")
            industry = None
            valuation = None
            if market == "cn":
                cur.execute(
                    f"SELECT {', '.join(_VAL_COLS)} FROM daily_valuation "
                    f"WHERE stock_code = %s ORDER BY trade_date DESC LIMIT 1", (code,))
                valuation = _row_dict(_VAL_COLS, cur.fetchone())
                cur.execute("SELECT industry FROM fin_indicator WHERE stock_code = %s "
                            "AND industry IS NOT NULL ORDER BY report_date DESC LIMIT 1", (code,))
                ind_row = cur.fetchone()
                industry = ind_row[0] if ind_row else None
            cur.execute(
                f"SELECT {', '.join(_IND_COLS)} FROM {qt['ind']} "
                f"WHERE stock_code = %s ORDER BY report_date DESC LIMIT 1", (code,))
            indicator = _row_dict(_IND_COLS, cur.fetchone())
    finally:
        conn.close()
    if market != "cn":
        # 港/美股无估值表,取腾讯实时行情;美股接口无 PB,用 价格/每股净资产 现算(同为美元)
        valuation = _fetch_live_valuation(market, code)
        if valuation:
            price = valuation.pop("price", None)
            if (market == "us" and valuation["pb"] is None and price
                    and indicator and indicator.get("bps")):
                valuation["pb"] = round(price / indicator["bps"], 2)
    return {"code": code, "name": name_row[0], "market": market,
            "industry": industry, "valuation": valuation, "indicator": indicator}


@app.get("/api/kline")
def kline(market: Literal["cn", "hk", "us"], code: str = Query(..., max_length=16),
          days: int = Query(250, ge=20, le=1000),
          period: Literal["day", "week", "month"] = "day"):
    """最近 days 根 K线(前复权,若无复权因子则原始价)。

    day 从日线表现算 qfq;week/month 走 *_weekly/monthly_price_hfq 物化视图
    (后复权),统一除以该股最新复权因子换算成前复权,与日线口径一致。
    """
    qt = _QUOTE_TABLES[market]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if period == "day":
                cur.execute(
                    f"SELECT trade_date, open, high, low, close, volume FROM {qt['price']} "
                    f"WHERE stock_code = %s ORDER BY trade_date DESC LIMIT %s", (code, days))
            else:
                mv = f"{qt['mv_prefix']}{'weekly' if period == 'week' else 'monthly'}_price_hfq"
                cur.execute(
                    f"SELECT trade_date, open, high, low, close, volume FROM {mv} "
                    f"WHERE stock_code = %s ORDER BY period_start DESC LIMIT %s", (code, days))
            price_rows = cur.fetchall()[::-1]   # 转升序
            if not price_rows:
                raise HTTPException(status_code=404, detail="未找到该股票的日线数据")
            cur.execute(
                f"SELECT trade_date, adj_factor FROM {qt['adj']} "
                f"WHERE stock_code = %s ORDER BY trade_date", (code,))
            factors = cur.fetchall()
    finally:
        conn.close()

    adjusted = bool(factors)
    latest_f = float(factors[-1][1]) if factors else 1.0
    bars = []
    if period == "day":
        # 日线是不复权原始价。因子只在变动日有记录:按日期前向填充;
        # qfq = 价格 × 当日因子 ÷ 最新因子
        fi = -1  # factors 中 ≤ 当前交易日的最后下标
        for d, o, h, l, c, v in price_rows:
            while fi + 1 < len(factors) and factors[fi + 1][0] <= d:
                fi += 1
            f = (float(factors[fi][1]) / latest_f) if (adjusted and fi >= 0) else 1.0
            bars.append({"d": d.isoformat(),
                         "o": round(float(o) * f, 6), "h": round(float(h) * f, 6),
                         "l": round(float(l) * f, 6), "c": round(float(c) * f, 6),
                         "v": int(v) if v is not None else 0})
    else:
        # 物化视图已是后复权价:除以最新因子即前复权
        f = 1.0 / latest_f
        for d, o, h, l, c, v in price_rows:
            bars.append({"d": d.isoformat(),
                         "o": round(float(o) * f, 6), "h": round(float(h) * f, 6),
                         "l": round(float(l) * f, 6), "c": round(float(c) * f, 6),
                         "v": int(v) if v is not None else 0})
    return {"code": code, "market": market, "adjusted": adjusted,
            "period": period, "bars": bars}


_STATIC = Path(__file__).resolve().parent / "static"


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(_STATIC / "index.html")
