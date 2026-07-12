# -*- coding: utf-8 -*-
"""长鑫科技上市对 A 股的影响预判(todo#8):中芯 2020/华虹 2023 科创板巨型 IPO 事件研究。

口径:
- T0 = 新股上市首日;窗口 T-40 ~ T+60;指数/板块归一化到 T-1 收盘。
- "抽血"观察:全市场日成交额(sum(daily_price.amount),元)在事件窗内的变化;
  以及募资额 ÷ 事件前 20 日日均成交额。
- 映射股:长鑫概念(股东/供应链/同赛道)最近走势,量化"抢跑"程度。
"""
import json
import os
import numpy as np
import pandas as pd
import psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")

EVENTS = {
    "688981.SH": dict(name="中芯国际", list_date="2020-07-16", issue_price=27.46, raise_yi=532.3),
    "688347.SH": dict(name="华虹公司", list_date="2023-08-07", issue_price=52.04, raise_yi=212.03),
}
PRE, POST = 40, 60

def series(sql, params):
    return pd.read_sql(sql, CONN, params=params, parse_dates=["trade_date"])

out = {"events": {}}
for code, ev in EVENTS.items():
    t0 = pd.Timestamp(ev["list_date"])
    lo, hi = t0 - pd.Timedelta(days=90), t0 + pd.Timedelta(days=120)

    idx = series("select trade_date, close from index_daily where index_code='sh000001' "
                 "and trade_date between %s and %s order by 1", (lo.date(), hi.date()))
    brd = series("select trade_date, close from board_daily where board_code='SH.LIST0002' "
                 "and trade_date between %s and %s order by 1", (lo.date(), hi.date()))
    amt = series("select trade_date, sum(amount)/1e12 amt_wanyi from daily_price "
                 "where trade_date between %s and %s group by 1 order by 1", (lo.date(), hi.date()))
    stk = series("select trade_date, close, volume from daily_price where stock_code=%s "
                 "and trade_date between %s and %s order by 1", (code, lo.date(), hi.date()))
    hk = series("select trade_date, close from hk_daily_price where stock_code='00981.HK' "
                "and trade_date between %s and %s order by 1", (lo.date(), hi.date())) \
        if code == "688981.SH" else pd.DataFrame(columns=["trade_date", "close"])

    pos = idx.trade_date.searchsorted(t0)
    win = idx.iloc[max(pos - PRE, 0):pos + POST + 1].copy()
    win["k"] = range(win.index[0] - pos, win.index[0] - pos + len(win))
    anchor_i = float(idx.close.iloc[pos - 1])
    kmap = dict(zip(win.trade_date, win.k))

    def norm(df, valcol="close"):
        d = df[df.trade_date.isin(kmap)].copy()
        d["k"] = d.trade_date.map(kmap)
        base = d[d.k == -1][valcol]
        b = float(base.iloc[0]) if len(base) else float(d[valcol].iloc[0])
        return dict(k=d.k.tolist(), v=[round(float(x) / b - 1, 4) for x in d[valcol]])

    # 板块/指数/H股相对路径;成交额为绝对值(万亿)
    e = dict(
        name=ev["name"], t0=ev["list_date"], raise_yi=ev["raise_yi"],
        idx=norm(idx), board=norm(brd),
        hk=norm(hk) if len(hk) else None,
        amt=dict(k=[kmap[d] for d in amt.trade_date if d in kmap],
                 v=[round(float(v), 3) for d, v in zip(amt.trade_date, amt.amt_wanyi) if d in kmap]),
    )
    # 新股自身:相对发行价
    s = stk[stk.trade_date >= t0].copy()
    s["k"] = range(len(s))
    e["stock"] = dict(k=s.k.tolist(), v=[round(float(c) / ev["issue_price"] - 1, 4) for c in s.close])
    # 摘要
    amt_pre = amt[amt.trade_date < t0].amt_wanyi.tail(20).mean() * 1e4  # 亿
    idx_t0 = float(idx.close.iloc[pos]) / anchor_i - 1
    def stat(d, k):
        try: return d["v"][d["k"].index(k)]
        except (ValueError, IndexError): return None
    e["summary"] = dict(
        amt_pre20_yi=round(float(amt_pre), 0), raise_pct_of_amt=round(ev["raise_yi"] / amt_pre, 4),
        idx_t0=round(idx_t0, 4), idx_20d=stat(e["idx"], 20), idx_60d=stat(e["idx"], 60),
        board_runup=stat(e["board"], -1) and round(stat(e["board"], -1) - (stat(e["board"], -PRE) or 0), 4),
        board_20d=stat(e["board"], 20), board_60d=stat(e["board"], 60),
        stock_d0=e["stock"]["v"][0] if e["stock"]["v"] else None,
        stock_20d=stat(e["stock"], 20), stock_60d=stat(e["stock"], 60),
        hk_t0=stat(e["hk"], 0) if e["hk"] else None, hk_20d=stat(e["hk"], 20) if e["hk"] else None,
    )
    out["events"][code] = e
    print(ev["name"], json.dumps(e["summary"], ensure_ascii=False))

# ---- 长鑫映射股近期表现(抢跑度)+ 当前市场成交额 ----
MAPPED = {"603986.SH": "兆易创新(股东/合作)", "688008.SH": "澜起科技(接口芯片)",
          "000021.SZ": "深科技(封测合作)", "688525.SH": "佰维存储(模组)",
          "002409.SZ": "雅克科技(材料)", "688981.SH": "中芯国际(代工对标)"}
mp = []
for c, nm in MAPPED.items():
    d = series("select trade_date, close from daily_price where stock_code=%s "
               "and trade_date >= current_date - interval '60 days' order by 1", (c,))
    if len(d) < 21:
        continue
    mp.append(dict(code=c, name=nm, ret20=round(float(d.close.iloc[-1] / d.close.iloc[-21] - 1), 4),
                   ret5=round(float(d.close.iloc[-1] / d.close.iloc[-6] - 1), 4)))
idx_now = series("select trade_date, close from index_daily where index_code='sh000001' "
                 "order by trade_date desc limit 21", ())
mp.append(dict(code="sh000001", name="上证指数(基准)",
               ret20=round(float(idx_now.close.iloc[0] / idx_now.close.iloc[-1] - 1), 4),
               ret5=round(float(idx_now.close.iloc[0] / idx_now.close.iloc[5] - 1), 4)))
amt_now = series("select trade_date, sum(amount)/1e12 w from daily_price "
                 "where trade_date >= current_date - interval '20 days' group by 1 order by 1", ())
out["mapped"] = mp
out["amt_now_wanyi"] = round(float(amt_now.w.mean()), 2)
print("映射股:", json.dumps(mp, ensure_ascii=False))
print("近期市场日成交(万亿):", out["amt_now_wanyi"])

path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cxmt_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False)
print("saved:", path)
