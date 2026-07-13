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
            # 最新日行情(开/收/涨跌幅),date 字段即数据日期
            cur.execute(
                f"SELECT trade_date, open, close, pct_chg, high, low FROM {qt['price']} "
                f"WHERE stock_code = %s ORDER BY trade_date DESC LIMIT 1", (code,))
            dr = cur.fetchone()
            daily = None if dr is None else {
                "date": dr[0].isoformat(),
                "open": float(dr[1]) if dr[1] is not None else None,
                "close": float(dr[2]) if dr[2] is not None else None,
                "pct_chg": float(dr[3]) if dr[3] is not None else None,
                "high": float(dr[4]) if dr[4] is not None else None,
                "low": float(dr[5]) if dr[5] is not None else None}
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
            "industry": industry, "valuation": valuation, "indicator": indicator,
            "daily": daily}


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


# ---------------------------------------------------------------------------
# 自选股(webapp 应用数据)
# ---------------------------------------------------------------------------
from pydantic import BaseModel  # noqa: E402


class WatchItem(BaseModel):
    market: Literal["cn", "hk", "us"]
    code: str
    grp: str = "默认分组"


class WatchGroup(BaseModel):
    name: str


class WatchOrderItem(BaseModel):
    market: Literal["cn", "hk", "us"]
    code: str


class WatchOrderGroup(BaseModel):
    name: str
    items: list[WatchOrderItem]


class WatchOrder(BaseModel):
    groups: list[WatchOrderGroup]


@app.get("/api/watchlist")
def watchlist_get():
    """自选股(按分组),带名称与最新收盘/涨跌幅。空分组也返回(供拖入)。"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM watchlist_group ORDER BY sort_order, name")
            groups = {r[0]: [] for r in cur.fetchall()}
            cur.execute("SELECT market, stock_code, grp FROM watchlist "
                        "ORDER BY sort_order, added_at")
            for market, code, grp in cur.fetchall():
                cfg, qt = MARKETS[market], _QUOTE_TABLES[market]
                cur.execute(
                    f"SELECT {cfg['name_expr']} FROM {cfg['basic']} WHERE stock_code = %s",
                    (code,))
                nr = cur.fetchone()
                cur.execute(
                    f"SELECT close, pct_chg FROM {qt['price']} "
                    f"WHERE stock_code = %s ORDER BY trade_date DESC LIMIT 1", (code,))
                pr = cur.fetchone()
                groups.setdefault(grp, []).append(
                    {"market": market, "code": code,
                     "name": nr[0] if nr else code,
                     "close": float(pr[0]) if pr and pr[0] is not None else None,
                     "pct_chg": float(pr[1]) if pr and pr[1] is not None else None})
    finally:
        conn.close()
    return {"groups": [{"name": g, "items": items} for g, items in groups.items()]}


@app.post("/api/watchlist")
def watchlist_add(item: WatchItem):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO watchlist_group (name, sort_order) "
                        "VALUES (%s, 999) ON CONFLICT DO NOTHING", (item.grp[:32],))
            cur.execute("SELECT coalesce(max(sort_order), 0) + 1 FROM watchlist WHERE grp = %s",
                        (item.grp[:32],))
            nxt = cur.fetchone()[0]
            cur.execute("INSERT INTO watchlist (market, stock_code, grp, sort_order) "
                        "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (item.market, item.code[:16], item.grp[:32], nxt))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/watchlist/groups")
def watchlist_group_add(g: WatchGroup):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT coalesce(max(sort_order), 0) + 1 FROM watchlist_group")
            cur.execute("INSERT INTO watchlist_group (name, sort_order) "
                        "SELECT %s, coalesce(max(sort_order), 0) + 1 FROM watchlist_group "
                        "ON CONFLICT DO NOTHING", (g.name[:32],))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.delete("/api/watchlist/groups/{name}")
def watchlist_group_del(name: str):
    if name == "默认分组":
        raise HTTPException(status_code=422, detail="默认分组不可删除")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # 成员并入默认分组(排到末尾)
            cur.execute("SELECT coalesce(max(sort_order), 0) FROM watchlist WHERE grp = '默认分组'")
            base = cur.fetchone()[0]
            cur.execute("UPDATE watchlist SET grp = '默认分组', sort_order = sort_order + %s "
                        "WHERE grp = %s", (base + 1000, name))
            cur.execute("DELETE FROM watchlist_group WHERE name = %s", (name,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.put("/api/watchlist/order")
def watchlist_order(order: WatchOrder):
    """拖拽后的批量重排:按前端给出的分组顺序与组内顺序整体重写。"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for gi, g in enumerate(order.groups):
                cur.execute("UPDATE watchlist_group SET sort_order = %s WHERE name = %s",
                            (gi, g.name[:32]))
                for si, it in enumerate(g.items):
                    cur.execute("UPDATE watchlist SET grp = %s, sort_order = %s "
                                "WHERE market = %s AND stock_code = %s",
                                (g.name[:32], si, it.market, it.code))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.delete("/api/watchlist/{market}/{code}")
def watchlist_del(market: Literal["cn", "hk", "us"], code: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM watchlist WHERE market = %s AND stock_code = %s",
                        (market, code))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


# ---------------------------------------------------------------------------
# 待办事项(webapp 应用数据:分析想法清单)
# ---------------------------------------------------------------------------
class TodoNew(BaseModel):
    content: str


class TodoPatch(BaseModel):
    done: bool | None = None
    report: str | None = None    # docs/analysis/ 下的报告文件名


from datetime import datetime  # noqa: E402


class ScheduleNew(BaseModel):
    content: str
    due_at: datetime             # 到期时刻(datetime-local 的本地时间)


class SchedulePatch(BaseModel):
    done: bool | None = None
    report: str | None = None
    due_at: datetime | None = None
    content: str | None = None


@app.get("/api/todos")
def todos_get():
    """未完成在前(新→旧),已完成沉底(完成时间新→旧);嵌套定时验证任务(按到期日)。"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, content, done, created_at, done_at, report FROM todo "
                        "ORDER BY done, CASE WHEN done THEN done_at END DESC, created_at DESC")
            rows = cur.fetchall()
            cur.execute("SELECT id, todo_id, content, due_at, done, done_at, report "
                        "FROM todo_schedule ORDER BY due_at, id")
            sched: dict[int, list] = {}
            for s in cur.fetchall():
                sched.setdefault(s[1], []).append(
                    {"id": s[0], "content": s[2],
                     "due_at": s[3].isoformat()[:16].replace("T", " "),
                     "done": s[4],
                     "done_at": s[5].isoformat()[:16].replace("T", " ") if s[5] else None,
                     "report": s[6]})
    finally:
        conn.close()
    return {"items": [{"id": r[0], "content": r[1], "done": r[2],
                       "created_at": r[3].isoformat()[:16].replace("T", " "),
                       "done_at": r[4].isoformat()[:16].replace("T", " ") if r[4] else None,
                       "report": r[5], "schedules": sched.get(r[0], [])}
                      for r in rows]}


@app.post("/api/todos/{tid}/schedules")
def schedule_add(tid: int, s: ScheduleNew):
    content = s.content.strip()
    if not content:
        raise HTTPException(status_code=422, detail="内容不能为空")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM todo WHERE id = %s", (tid,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="待办不存在")
            cur.execute("INSERT INTO todo_schedule (todo_id, content, due_at) "
                        "VALUES (%s, %s, %s) RETURNING id", (tid, content[:500], s.due_at))
            sid = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "id": sid}


@app.patch("/api/schedules/{sid}")
def schedule_patch(sid: int, p: SchedulePatch):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if p.done is not None:
                cur.execute("UPDATE todo_schedule SET done = %s, "
                            "done_at = CASE WHEN %s THEN now() END WHERE id = %s",
                            (p.done, p.done, sid))
            if p.report is not None:
                cur.execute("UPDATE todo_schedule SET report = NULLIF(%s, '') WHERE id = %s",
                            (p.report.strip()[:200], sid))
            if p.due_at is not None:
                cur.execute("UPDATE todo_schedule SET due_at = %s WHERE id = %s",
                            (p.due_at, sid))
            if p.content is not None and p.content.strip():
                cur.execute("UPDATE todo_schedule SET content = %s WHERE id = %s",
                            (p.content.strip()[:500], sid))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.delete("/api/schedules/{sid}")
def schedule_del(sid: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM todo_schedule WHERE id = %s", (sid,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/todos")
def todos_add(t: TodoNew):
    content = t.content.strip()
    if not content:
        raise HTTPException(status_code=422, detail="内容不能为空")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO todo (content) VALUES (%s) RETURNING id", (content[:500],))
            tid = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "id": tid}


@app.patch("/api/todos/{tid}")
def todos_patch(tid: int, p: TodoPatch):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if p.done is not None:
                cur.execute("UPDATE todo SET done = %s, done_at = CASE WHEN %s THEN now() END "
                            "WHERE id = %s", (p.done, p.done, tid))
            if p.report is not None:
                cur.execute("UPDATE todo SET report = NULLIF(%s, '') WHERE id = %s",
                            (p.report.strip()[:200], tid))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


_REPORTS_DIR = Path(__file__).resolve().parent.parent / "docs" / "analysis"


@app.get("/reports/{name}", include_in_schema=False)
def report_file(name: str):
    """分析报告(docs/analysis/ 下的自包含 HTML)。仅允许纯文件名,防路径穿越。"""
    if "/" in name or "\\" in name or name.startswith("."):
        raise HTTPException(status_code=422, detail="非法文件名")
    f = _REPORTS_DIR / name
    if not f.is_file():
        raise HTTPException(status_code=404, detail="报告不存在")
    return FileResponse(f)


@app.delete("/api/todos/{tid}")
def todos_del(tid: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM todo WHERE id = %s", (tid,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


# ---------------------------------------------------------------------------
# 每日公告(现有事件层:业绩预告/业绩快报/龙虎榜;全量交易所公告待数据层补源)
# ---------------------------------------------------------------------------
@app.get("/api/announcements")
def announcements(date_: date = Query(..., alias="date")):
    """某日的公告级事件。forecast 按股去重(优先归母净利润口径);
    nearest = 三个来源中 ≤ 所查日的最近有数据日(空日跳转用)。"""
    fnum = lambda v: float(v) if v is not None else None  # noqa: E731
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (f.stock_code) f.stock_code, s.name, f.report_date, "
                "f.forecast_type, f.change_pct, f.change_desc "
                "FROM fin_forecast f JOIN stock_basic s USING (stock_code) "
                "WHERE f.ann_date = %s "
                "ORDER BY f.stock_code, (f.forecast_type <> '归属于上市公司股东的净利润')",
                (date_,))
            forecast = [{"code": r[0], "name": r[1], "report_date": r[2].isoformat(),
                         "type": r[3], "change_pct": fnum(r[4]), "desc": r[5]}
                        for r in cur.fetchall()]
            forecast.sort(key=lambda x: -(x["change_pct"] if x["change_pct"] is not None
                                          else float("-inf")))
            cur.execute(
                "SELECT e.stock_code, s.name, e.report_date, e.revenue, e.revenue_yoy, "
                "e.net_profit, e.net_profit_yoy "
                "FROM fin_express e JOIN stock_basic s USING (stock_code) "
                "WHERE e.ann_date = %s ORDER BY e.net_profit_yoy DESC NULLS LAST", (date_,))
            express = [{"code": r[0], "name": r[1], "report_date": r[2].isoformat(),
                        "revenue": fnum(r[3]), "revenue_yoy": fnum(r[4]),
                        "net_profit": fnum(r[5]), "net_profit_yoy": fnum(r[6])}
                       for r in cur.fetchall()]
            cur.execute(
                "SELECT l.stock_code, s.name, l.reason, l.close, l.pct_chg, l.net_buy "
                "FROM lhb_detail l JOIN stock_basic s USING (stock_code) "
                "WHERE l.trade_date = %s ORDER BY l.net_buy DESC NULLS LAST", (date_,))
            lhb = [{"code": r[0], "name": r[1], "reason": r[2], "close": fnum(r[3]),
                    "pct_chg": fnum(r[4]), "net_buy": fnum(r[5])} for r in cur.fetchall()]
            cur.execute(
                "SELECT max(d) FROM ("
                "  SELECT max(ann_date) d FROM fin_forecast WHERE ann_date <= %s"
                "  UNION ALL SELECT max(ann_date) FROM fin_express WHERE ann_date <= %s"
                "  UNION ALL SELECT max(trade_date) FROM lhb_detail WHERE trade_date <= %s) t",
                (date_, date_, date_))
            nr = cur.fetchone()[0]
    finally:
        conn.close()
    return {"date": date_.isoformat(), "nearest": nr.isoformat() if nr else None,
            "forecast": forecast, "express": express, "lhb": lhb}


# ---------------------------------------------------------------------------
# 市场指数
# ---------------------------------------------------------------------------
_INDEX_TABLES = {
    "cn": {"index": "index_daily", "price": "daily_price"},
    "hk": {"index": "hk_index_daily", "price": "hk_daily_price"},
    "us": {"index": "us_index_daily", "price": "us_daily_price"},
}


@app.get("/api/index/kline")
def index_kline(market: Literal["cn", "hk", "us"],
                code: str = Query(..., max_length=16),
                days: int = Query(250, ge=20, le=1000)):
    """指数日线,volume 替换为该市场全部个股当日成交量之和(市场总量口径;
    美股为精选池 ~550 只,总量偏小)。"""
    t = _INDEX_TABLES[market]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT trade_date, open, high, low, close FROM {t['index']} "
                f"WHERE index_code = %s ORDER BY trade_date DESC LIMIT %s", (code, days))
            rows = cur.fetchall()[::-1]
            if not rows:
                raise HTTPException(status_code=404, detail="未找到该指数")
            cur.execute(
                f"SELECT trade_date, sum(volume) FROM {t['price']} "
                f"WHERE trade_date >= %s GROUP BY trade_date", (rows[0][0],))
            mvol = {r[0]: int(r[1]) for r in cur.fetchall() if r[1] is not None}
    finally:
        conn.close()
    bars = [{"d": r[0].isoformat(),
             "o": float(r[1]), "h": float(r[2]), "l": float(r[3]), "c": float(r[4]),
             "v": mvol.get(r[0], 0)} for r in rows]
    return {"code": code, "market": market, "bars": bars}


# ---------------------------------------------------------------------------
# A股板块(board/board_daily/board_member,2018 年起;board_fund_flow 尚空)
# ---------------------------------------------------------------------------

def _board_latest_date(cur, before=None):
    cur.execute("SELECT max(trade_date) FROM board_daily" +
                (" WHERE trade_date <= %s" if before else ""),
                (before,) if before else ())
    return cur.fetchone()[0]


# 泛概念过滤:融资融券/MSCI/深股通这类全市场属性标签成员数千只,不是真主题,
# 会在热力图/日历里淹没真正的概念板块。按现役成员数阈值剔除(存储器/CPO 约 180 只);
# 市值/价格风格标签成员数不高但同样无主题信息,按名单剔除。
_GENERIC_CONCEPT_MAX_MEMBERS = 400
_STYLE_LABEL_BOARDS = ("大盘股", "中盘股", "小盘股", "微盘股", "高价股", "低价股")
_CONCEPT_FILTER_SQL = (
    " AND (SELECT count(*) FROM board_member m "
    "      WHERE m.board_code = b.board_code AND m.valid_to IS NULL) <= %s"
    " AND b.board_name <> ALL(%s)")


_PERIOD_DAYS = {"5d": 5, "10d": 10, "20d": 20, "60d": 60, "120d": 120, "250d": 250}

# 港/美股板块:无板块指数日线、个股 amount 为空(腾讯源)、无估值表。
# 指标从成员股现算:涨跌幅=成员等权平均,成交额≈Σ(收盘×成交量),市值缺省。
_INTL_BOARD = {
    "hk": {"board": "hk_board", "member": "hk_board_member",
           "price": "hk_daily_price", "basic": "hk_stock_basic",
           "name_expr": "COALESCE(s.name_cn, s.name)"},
    "us": {"board": "us_board", "member": "us_board_member",
           "price": "us_daily_price", "basic": "us_stock_basic",
           "name_expr": "s.name"},
}


def _intl_latest_date(cur, price_table, before=None):
    cur.execute(f"SELECT max(trade_date) FROM {price_table}" +
                (" WHERE trade_date <= %s" if before else ""),
                (before,) if before else ())
    return cur.fetchone()[0]


def _intl_base_date(cur, price_table, d, period):
    if period == "ytd":
        cur.execute(f"SELECT max(trade_date) FROM {price_table} "
                    f"WHERE trade_date < date_trunc('year', %s::date)", (d,))
    else:
        cur.execute(f"SELECT trade_date FROM (SELECT DISTINCT trade_date "
                    f"FROM {price_table} WHERE trade_date <= %s "
                    f"ORDER BY trade_date DESC LIMIT %s) t "
                    f"ORDER BY trade_date LIMIT 1", (d, _PERIOD_DAYS[period] + 1))
    row = cur.fetchone()
    return row[0] if row else None


@app.get("/api/boards/snapshot")
def boards_snapshot(btype: Literal["industry", "concept"],
                    date_: str | None = Query(None, alias="date", max_length=10),
                    period: Literal["today", "5d", "10d", "20d",
                                    "60d", "120d", "250d", "ytd"] = "today",
                    market: Literal["cn", "hk", "us"] = "cn"):
    """某交易日(默认最新)全部板块快照,按所选周期涨跌幅降序。

    A股:period=today 用板块指数当日 pct_chg;其余用 收盘/基期收盘-1(基期=N 个
    交易日前或上年末)。mktcap=现役成员股总市值之和(估值表最新日)。
    港/美股:从成员股现算(见 _INTL_BOARD 注释)。
    """
    if market != "cn":
        return _intl_boards_snapshot(market, btype, date_, period)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            d = _board_latest_date(cur, date_)
            # 基期日
            base_d = None
            if period == "ytd":
                cur.execute("SELECT max(trade_date) FROM board_daily "
                            "WHERE trade_date < date_trunc('year', %s::date)", (d,))
                base_d = cur.fetchone()[0]
            elif period != "today":
                cur.execute("SELECT trade_date FROM (SELECT DISTINCT trade_date "
                            "FROM board_daily WHERE trade_date <= %s "
                            "ORDER BY trade_date DESC LIMIT %s) t "
                            "ORDER BY trade_date LIMIT 1", (d, _PERIOD_DAYS[period] + 1))
                base_d = cur.fetchone()[0]
            pct_expr = ("cur.pct_chg" if period == "today" else
                        "CASE WHEN base.close > 0 THEN (cur.close/base.close - 1)*100 END")
            base_join = ("" if period == "today" else
                         "LEFT JOIN board_daily base ON base.board_code = cur.board_code "
                         "AND base.trade_date = %s ")
            cur.execute("SELECT max(trade_date) FROM daily_valuation WHERE trade_date <= %s", (d,))
            val_d = cur.fetchone()[0]
            sql = (f"SELECT b.board_code, b.board_name, {pct_expr} AS pct, "
                   f"cur.amount, cur.turnover, cur.close, mv.mktcap "
                   f"FROM board_daily cur JOIN board b USING (board_code) "
                   f"{base_join}"
                   f"LEFT JOIN (SELECT m.board_code, sum(v.total_mv) AS mktcap "
                   f"           FROM board_member m JOIN daily_valuation v "
                   f"             ON v.stock_code = m.stock_code AND v.trade_date = %s "
                   f"           WHERE m.valid_to IS NULL GROUP BY 1) mv "
                   f"  ON mv.board_code = cur.board_code "
                   f"WHERE cur.trade_date = %s AND b.board_type = %s")
            params: list = ([base_d] if base_join else []) + [val_d, d, btype]
            if btype == "concept":
                sql += _CONCEPT_FILTER_SQL
                params += [_GENERIC_CONCEPT_MAX_MEMBERS, list(_STYLE_LABEL_BOARDS)]
            cur.execute(sql + " ORDER BY 3 DESC NULLS LAST", params)
            rows = cur.fetchall()
    finally:
        conn.close()
    fnum = lambda v: float(v) if v is not None else None  # noqa: E731
    return {"date": d.isoformat() if d else None, "period": period,
            "base_date": base_d.isoformat() if base_d else None,
            "items": [{"code": r[0], "name": r[1], "pct_chg": fnum(r[2]),
                       "amount": fnum(r[3]), "turnover": fnum(r[4]),
                       "close": fnum(r[5]), "mktcap": fnum(r[6])} for r in rows]}


def _intl_boards_snapshot(market: str, btype: str, date_, period: str):
    t = _INTL_BOARD[market]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            d = _intl_latest_date(cur, t["price"], date_)
            base_d = None
            if period == "today":
                pct_expr, base_join, base_params = "avg(p.pct_chg)", "", []
            else:
                base_d = _intl_base_date(cur, t["price"], d, period)
                pct_expr = "avg(CASE WHEN p0.close > 0 THEN (p.close/p0.close - 1)*100 END)"
                base_join = (f"JOIN {t['price']} p0 ON p0.stock_code = m.stock_code "
                             f"AND p0.trade_date = %s ")
                base_params = [base_d]
            cur.execute(
                f"SELECT b.board_code, b.board_name, {pct_expr} AS pct, "
                f"sum(p.close * p.volume) AS amount "
                f"FROM {t['board']} b "
                f"JOIN {t['member']} m ON m.board_code = b.board_code AND m.out_date IS NULL "
                f"JOIN {t['price']} p ON p.stock_code = m.stock_code AND p.trade_date = %s "
                f"{base_join}"
                f"WHERE b.board_type = %s "
                f"GROUP BY 1, 2 ORDER BY 3 DESC NULLS LAST",
                [d] + base_params + [btype])
            rows = cur.fetchall()
    finally:
        conn.close()
    fnum = lambda v: float(v) if v is not None else None  # noqa: E731
    return {"date": d.isoformat() if d else None, "period": period,
            "base_date": base_d.isoformat() if base_d else None,
            "items": [{"code": r[0], "name": r[1], "pct_chg": fnum(r[2]),
                       "amount": fnum(r[3]), "turnover": None,
                       "close": None, "mktcap": None} for r in rows]}


@app.get("/api/boards/calendar")
def boards_calendar(btype: Literal["industry", "concept"],
                    days: int = Query(20, ge=5, le=60),
                    market: Literal["cn", "hk", "us"] = "cn"):
    if market != "cn":
        return _intl_boards_calendar(market, btype, days)
    """板块 × 最近 N 个交易日的涨跌幅矩阵(轮动热力日历)。行按近 5 日累计涨幅降序。"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM board_daily "
                        "ORDER BY trade_date DESC LIMIT %s", (days,))
            dates = sorted(r[0] for r in cur.fetchall())
            if not dates:
                return {"dates": [], "rows": []}
            sql = ("SELECT d.board_code, b.board_name, d.trade_date, d.pct_chg "
                   "FROM board_daily d JOIN board b USING (board_code) "
                   "WHERE b.board_type = %s AND d.trade_date >= %s")
            params: list = [btype, dates[0]]
            if btype == "concept":
                sql += _CONCEPT_FILTER_SQL
                params += [_GENERIC_CONCEPT_MAX_MEMBERS, list(_STYLE_LABEL_BOARDS)]
            cur.execute(sql, params)
            data = cur.fetchall()
    finally:
        conn.close()
    idx = {dt: i for i, dt in enumerate(dates)}
    boards: dict[str, dict] = {}
    for code, name, dt, pct in data:
        row = boards.setdefault(code, {"code": code, "name": name,
                                       "values": [None] * len(dates)})
        if dt in idx:
            row["values"][idx[dt]] = float(pct) if pct is not None else None
    rows = list(boards.values())
    rows.sort(key=lambda r: -sum(v for v in r["values"][-5:] if v is not None))
    return {"dates": [dt.isoformat() for dt in dates], "rows": rows}


def _intl_boards_calendar(market: str, btype: str, days: int):
    t = _INTL_BOARD[market]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT trade_date FROM {t['price']} "
                        f"ORDER BY trade_date DESC LIMIT %s", (days,))
            dates = sorted(r[0] for r in cur.fetchall())
            if not dates:
                return {"dates": [], "rows": []}
            cur.execute(
                f"SELECT m.board_code, b.board_name, p.trade_date, avg(p.pct_chg) "
                f"FROM {t['member']} m JOIN {t['board']} b ON b.board_code = m.board_code "
                f"JOIN {t['price']} p ON p.stock_code = m.stock_code "
                f"  AND p.trade_date >= %s AND p.trade_date <= %s "
                f"WHERE m.out_date IS NULL AND b.board_type = %s "
                f"GROUP BY 1, 2, 3", (dates[0], dates[-1], btype))
            data = cur.fetchall()
    finally:
        conn.close()
    idx = {dt: i for i, dt in enumerate(dates)}
    boards: dict[str, dict] = {}
    for code, name, dt, pct in data:
        row = boards.setdefault(code, {"code": code, "name": name,
                                       "values": [None] * len(dates)})
        if dt in idx:
            row["values"][idx[dt]] = float(pct) if pct is not None else None
    rows = list(boards.values())
    rows.sort(key=lambda r: -sum(v for v in r["values"][-5:] if v is not None))
    return {"dates": [dt.isoformat() for dt in dates], "rows": rows}


@app.get("/api/boards/kline")
def boards_kline(code: str = Query(..., max_length=24),
                 days: int = Query(250, ge=20, le=1000),
                 period: Literal["day", "week", "month"] = "day"):
    """板块指数 K线。周/月为日线现聚合(板块无复权概念,数据 2018 年起)。"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if period == "day":
                cur.execute(
                    "SELECT trade_date, open, high, low, close, volume FROM board_daily "
                    "WHERE board_code = %s ORDER BY trade_date DESC LIMIT %s", (code, days))
            else:
                unit = "week" if period == "week" else "month"
                cur.execute(
                    f"SELECT max(trade_date) AS d,"
                    f" (array_agg(open ORDER BY trade_date))[1],"
                    f" max(high), min(low),"
                    f" (array_agg(close ORDER BY trade_date DESC))[1], sum(volume) "
                    f"FROM board_daily WHERE board_code = %s "
                    f"GROUP BY date_trunc('{unit}', trade_date) "
                    f"ORDER BY d DESC LIMIT %s", (code, days))
            rows = cur.fetchall()[::-1]
            cur.execute("SELECT board_name FROM board WHERE board_code = %s", (code,))
            name_row = cur.fetchone()
    finally:
        conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="未找到该板块的日线数据")
    bars = [{"d": r[0].isoformat(),
             "o": float(r[1]), "h": float(r[2]), "l": float(r[3]), "c": float(r[4]),
             "v": int(r[5]) if r[5] is not None else 0} for r in rows]
    return {"code": code, "name": name_row[0] if name_row else code,
            "period": period, "adjusted": False, "bars": bars}


@app.get("/api/boards/members")
def boards_members(code: str = Query(..., max_length=24),
                   date_: str | None = Query(None, alias="date", max_length=10),
                   market: Literal["cn", "hk", "us"] = "cn"):
    """板块现役成员股及其当日表现,按涨跌幅降序。港/美股 amount 用 收盘×量 近似。"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if market == "cn":
                d = _board_latest_date(cur, date_)
                cur.execute(
                    "SELECT m.stock_code, s.name, p.pct_chg, p.amount, p.close "
                    "FROM board_member m JOIN stock_basic s ON s.stock_code = m.stock_code "
                    "LEFT JOIN daily_price p ON p.stock_code = m.stock_code AND p.trade_date = %s "
                    "WHERE m.board_code = %s AND m.valid_to IS NULL "
                    "ORDER BY p.pct_chg DESC NULLS LAST", (d, code))
                rows = cur.fetchall()
                cur.execute("SELECT board_name FROM board WHERE board_code = %s", (code,))
            else:
                t = _INTL_BOARD[market]
                d = _intl_latest_date(cur, t["price"], date_)
                cur.execute(
                    f"SELECT m.stock_code, {t['name_expr']}, p.pct_chg, "
                    f"p.close * p.volume AS amount, p.close "
                    f"FROM {t['member']} m "
                    f"JOIN {t['basic']} s ON s.stock_code = m.stock_code "
                    f"LEFT JOIN {t['price']} p ON p.stock_code = m.stock_code AND p.trade_date = %s "
                    f"WHERE m.board_code = %s AND m.out_date IS NULL "
                    f"ORDER BY p.pct_chg DESC NULLS LAST", (d, code))
                rows = cur.fetchall()
                cur.execute(f"SELECT board_name FROM {t['board']} WHERE board_code = %s", (code,))
            name_row = cur.fetchone()
    finally:
        conn.close()
    return {"date": d.isoformat() if d else None,
            "board_name": name_row[0] if name_row else code,
            "items": [{"code": r[0], "name": r[1],
                       "pct_chg": float(r[2]) if r[2] is not None else None,
                       "amount": float(r[3]) if r[3] is not None else None,
                       "close": float(r[4]) if r[4] is not None else None} for r in rows]}


@app.get("/api/boards/fundflow")
def boards_fundflow(btype: Literal["industry", "concept"],
                    period: Literal["today", "5d", "20d"] = "today",
                    market: Literal["cn", "hk", "us"] = "cn"):
    """板块主力资金流:现役成员个股 capital_flow(富途源)按板块求和。

    日期基准按该市场成员在 capital_flow 中的实际交易日取(三市场日历不同,
    不能混用)。covered/members 标注成员覆盖度(回填期间可能不完整)。
    金额单位随市场币种(cn=元 / hk=港元 / us=美元)。
    """
    n_days = {"today": 1, "5d": 5, "20d": 20}[period]
    if market == "cn":
        board_t, member_t = "board", "board_member"
        open_cond = "valid_to IS NULL"
    else:
        t = _INTL_BOARD[market]
        board_t, member_t = t["board"], t["member"]
        open_cond = "out_date IS NULL"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT cf.trade_date FROM capital_flow cf "
                f"WHERE EXISTS (SELECT 1 FROM {member_t} mm "
                f"              WHERE mm.stock_code = cf.stock_code AND mm.{open_cond}) "
                f"ORDER BY cf.trade_date DESC LIMIT %s", (n_days,))
            dates = sorted(r[0] for r in cur.fetchall())
            if not dates:
                return {"dates": [], "items": []}
            sql = (f"SELECT b.board_code, b.board_name, sum(cf.main_net) AS main_net, "
                   f"count(DISTINCT cf.stock_code) AS covered, "
                   f"(SELECT count(*) FROM {member_t} m2 "
                   f" WHERE m2.board_code = b.board_code AND m2.{open_cond}) AS members "
                   f"FROM {board_t} b "
                   f"JOIN {member_t} m ON m.board_code = b.board_code AND m.{open_cond} "
                   f"JOIN capital_flow cf ON cf.stock_code = m.stock_code "
                   f"  AND cf.trade_date >= %s AND cf.trade_date <= %s "
                   f"WHERE b.board_type = %s")
            params: list = [dates[0], dates[-1], btype]
            if market == "cn" and btype == "concept":
                sql += _CONCEPT_FILTER_SQL
                params += [_GENERIC_CONCEPT_MAX_MEMBERS, list(_STYLE_LABEL_BOARDS)]
            cur.execute(sql + " GROUP BY 1, 2 ORDER BY 3 DESC NULLS LAST", params)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"dates": [d.isoformat() for d in dates],
            "items": [{"code": r[0], "name": r[1],
                       "main_net": float(r[2]) if r[2] is not None else None,
                       "covered": r[3], "members": r[4]} for r in rows]}


@app.get("/api/boards/compare")
def boards_compare(codes: str = Query(..., max_length=200),
                   days: int = Query(250, ge=20, le=1000)):
    """多板块收盘价序列(最多 6 个),供前端归一化画净值对比。"""
    code_list = [c.strip() for c in codes.split(",") if c.strip()][:6]
    if not code_list:
        raise HTTPException(status_code=422, detail="codes 不能为空")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            series = []
            for c in code_list:
                cur.execute(
                    "SELECT trade_date, close FROM board_daily "
                    "WHERE board_code = %s ORDER BY trade_date DESC LIMIT %s", (c, days))
                rows = cur.fetchall()[::-1]
                cur.execute("SELECT board_name FROM board WHERE board_code = %s", (c,))
                nr = cur.fetchone()
                series.append({"code": c, "name": nr[0] if nr else c,
                               "dates": [r[0].isoformat() for r in rows],
                               "closes": [float(r[1]) for r in rows]})
    finally:
        conn.close()
    return {"series": series}


_STATIC = Path(__file__).resolve().parent / "static"


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(_STATIC / "index.html")
