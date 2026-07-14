# -*- coding: utf-8 -*-
"""todo#49:亚联机械(001395.SZ)最近两周涨跌 + 财报后反应(全库内数据)。"""
import json
import os
import pandas as pd
import psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")
CODE = "001395.SZ"

px = pd.read_sql(
    "select trade_date, open, high, low, close, pre_close, pct_chg, volume, amount, turnover "
    "from daily_price where stock_code=%s and trade_date >= '2026-06-24' order by trade_date",
    CONN, params=(CODE,), parse_dates=["trade_date"])
val = pd.read_sql(
    "select trade_date, pe_ttm, pb, total_mv/1e8 mv_yi from daily_valuation "
    "where stock_code=%s and trade_date >= '2026-06-24' order by trade_date",
    CONN, params=(CODE,), parse_dates=["trade_date"])
lhb = pd.read_sql(
    "select trade_date, reason, buy_amount/1e4 buy_w, sell_amount/1e4 sell_w "
    "from lhb_detail where stock_code=%s and trade_date >= '2026-07-01' order by trade_date",
    CONN, params=(CODE,), parse_dates=["trade_date"])
ann = pd.read_sql(
    "select publish_time, category, title from announcement where stock_code=%s "
    "and publish_time >= '2026-07-06' order by publish_time",
    CONN, params=(CODE,))
fc = pd.read_sql(
    "select ann_date, forecast_type, change_pct, change_desc from fin_forecast "
    "where stock_code=%s and report_date='2026-06-30'", CONN, params=(CODE,))
ex = pd.read_sql(
    "select ann_date, net_profit/1e8 np, net_profit_yoy npy, revenue/1e8 rev, revenue_yoy revy, eps "
    "from fin_express where stock_code=%s and report_date='2026-06-30'", CONN, params=(CODE,))

m = px.merge(val, on="trade_date", how="left")
base = float(px.loc[px.trade_date == "2026-07-08", "close"].iloc[0])   # 预告前一日收盘
last = float(px.close.iloc[-1])
print(f"预告前(7-08)收盘 {base} → 最新(7-13)收盘 {last}  三连板累计 {(last/base-1)*100:.1f}%")
print("\n最近两周:")
print(m[["trade_date","close","pct_chg","turnover","amount"]].to_string(index=False))
print("\n龙虎榜:")
print(lhb.to_string(index=False))
print("\n业绩快报:", ex.to_dict("records"))

lhb_map = {str(r.trade_date.date()): dict(reason=r.reason, buy=round(r.buy_w,0), sell=round(r.sell_w,0))
           for r in lhb.itertuples()}
# 第一板(2026-07-09)分时:一字涨停成交结构
mv = pd.read_sql("select to_char(trade_time,'HH24:MI') t, volume v from minute_price "
                 "where stock_code=%s and trade_time::date='2026-07-09' order by trade_time",
                 CONN, params=(CODE,))
sc = pd.read_sql("select float_shares from share_capital where stock_code=%s "
                 "order by change_date desc limit 1", CONN, params=(CODE,))
float_sh = int(sc.float_shares.iloc[0]) if len(sc) else None
intra = dict(
    date="2026-07-09", limit_price=26.03, pre_close=23.66,
    vols=[int(x) for x in mv.v], times=[str(t) for t in mv.t],
    day_vol=int(mv.v.sum()), first_min=int(mv.v.iloc[0]),
    first_min_pct=round(float(mv.v.iloc[0]/mv.v.sum()), 4),
    float_shares=float_sh,
    day_vol_pct_float=round(float(mv.v.sum()/float_sh), 4) if float_sh else None,
)

out = dict(
    code=CODE, name="亚联机械",
    base_date="2026-07-08", base_close=base, last_close=last,
    run_pct=round(last/base-1, 4),
    daily=[dict(d=str(r.trade_date.date()), o=float(r.open), h=float(r.high), l=float(r.low),
                c=float(r.close), pct=float(r.pct_chg) if pd.notna(r.pct_chg) else None,
                vol=int(r.volume), amt=float(r.amount) if pd.notna(r.amount) else None,
                to=float(r.turnover) if pd.notna(r.turnover) else None,
                pe=float(r.pe_ttm) if pd.notna(r.pe_ttm) else None,
                pb=float(r.pb) if pd.notna(r.pb) else None,
                mv=float(r.mv_yi) if pd.notna(r.mv_yi) else None,
                lhb=lhb_map.get(str(r.trade_date.date())))
           for r in m.itertuples()],
    express=(lambda r: dict(ann_date=str(r["ann_date"]), np=float(r["np"]), npy=float(r["npy"]),
                            rev=float(r["rev"]), revy=float(r["revy"]), eps=float(r["eps"])))(ex.iloc[0]) if len(ex) else None,
    forecast=[dict(t=r.forecast_type, pct=float(r.change_pct) if pd.notna(r.change_pct) else None,
                   desc=r.change_desc) for r in fc.itertuples()],
    ann=[dict(t=str(r.publish_time), cat=r.category, title=r.title) for r in ann.itertuples()],
    intra=intra,
)
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "yalian_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False, default=str)
print("\nsaved:", path)
