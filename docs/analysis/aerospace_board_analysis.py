# -*- coding: utf-8 -*-
"""商业航天板块盘点(全库内数据)。

局限(库内固有,已登记 project-notes 移交清单#7):
- 无运营面数据(发射次数/在手订单/星座组网进度)——商业航天真正的基本面驱动;
- 龙头(蓝箭/星际荣耀/银河航天等)在一级市场未上市,A股只有二级"概念"公司;
故本报告只回答"二级市场层面:板块贵不贵、资金进出、谁在领涨、纯度如何",
不回答"行业景气/公司订单"。
"""
import json
import os
import numpy as np
import pandas as pd
import psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")
BOARD = "SH.LIST23196"      # 商业航天(富途概念)
VAL_DATE = "2026-07-13"
SUB = {"SH.LIST23040": "卫星互联网", "SH.LIST0035": "航天装备", "SH.LIST0958": "军工电子"}

members = pd.read_sql(
    "select m.stock_code, b.name from board_member m join stock_basic b using(stock_code) "
    "where m.board_code=%s and m.valid_to is null", CONN, params=(BOARD,))
codes = members.stock_code.tolist()

# 交叉成员=纯度代理(同时属于卫星/航天装备核心子板 = 更纯正)
cross = pd.read_sql(
    "select stock_code, board_code from board_member "
    "where board_code = any(%s) and valid_to is null", CONN, params=(list(SUB),))
pure = cross.groupby("stock_code").board_code.apply(lambda s: [SUB[b] for b in s]).to_dict()

# 当前估值 + PS/PB 分位(2023 起,板块历史起点)
val = pd.read_sql(
    "select stock_code, pe_ttm, pb, ps, total_mv/1e8 mv_yi from daily_valuation "
    "where trade_date=%s and stock_code = any(%s)", CONN, params=(VAL_DATE, codes))
histps = pd.read_sql(
    "select stock_code, ps from daily_valuation where stock_code = any(%s) "
    "and ps is not null and ps>0", CONN, params=(codes,))
ps_pct = {}
for c, g in histps.groupby("stock_code"):
    cur = val.loc[val.stock_code == c, "ps"]
    if len(cur) and pd.notna(cur.iloc[0]):
        ps_pct[c] = float((g.ps <= cur.iloc[0]).mean())

# 近60日涨幅 + 资金流(主力净额,现役成员求和)
px = pd.read_sql(
    "select stock_code, trade_date, close from daily_price where stock_code = any(%s) "
    "and trade_date >= %s::date - interval '110 days' order by 1,2", CONN, params=(codes, VAL_DATE))
ret = {}
for c, g in px.groupby("stock_code"):
    if len(g) >= 21:
        ret[c] = dict(r20=float(g.close.iloc[-1]/g.close.iloc[-21]-1),
                      r60=float(g.close.iloc[-1]/g.close.iloc[0]-1) if len(g) >= 60 else None)

# 板块资金流:主力净额(元)近60日按日汇总
flow = pd.read_sql("""
select c.trade_date, sum(c.main_net)/1e8 main_yi
from capital_flow c join board_member m using(stock_code)
where m.board_code=%s and m.valid_to is null and c.trade_date >= %s::date - interval '70 days'
group by c.trade_date order by c.trade_date""", CONN, params=(BOARD, VAL_DATE))

# 板块日线 + 沪深300(2年,归一)
bd = pd.read_sql("select trade_date, close from board_daily where board_code=%s "
                 "and trade_date >= %s::date - interval '2 years' order by 1",
                 CONN, params=(BOARD, VAL_DATE), parse_dates=["trade_date"])
hs = pd.read_sql("select trade_date, close from index_daily where index_code='sh000300' "
                 "and trade_date >= %s::date - interval '2 years' order by 1",
                 CONN, params=(VAL_DATE,), parse_dates=["trade_date"])

# 组装个股表
m = members.merge(val, on="stock_code", how="left")
m.rename(columns={"ps":"ps"}, inplace=True)
m["ps_pct"] = m.stock_code.map(ps_pct)
m["r20"] = m.stock_code.map(lambda c: (ret.get(c) or {}).get("r20"))
m["r60"] = m.stock_code.map(lambda c: (ret.get(c) or {}).get("r60"))
m["pure"] = m.stock_code.map(lambda c: pure.get(c, []))
m["npure"] = m.pure.apply(len)
m = m.sort_values("mv_yi", ascending=False)

# 盈利状态(用当前 pe_ttm 正负粗判)
m["profitable"] = m.pe_ttm.apply(lambda x: pd.notna(x) and x > 0)

print(f"成分 {len(m)} 只;盈利(PE_TTM>0) {int(m.profitable.sum())} 只;"
      f"亏损/微利 {int((~m.profitable).sum())} 只")
print(f"纯正(交叉卫星/航天装备/军工电子) {int((m.npure>0).sum())} 只")
print("\n市值前 15:")
print(m.head(15)[["stock_code","name","mv_yi","pe_ttm","ps","ps_pct","r60","npure"]].round(2).to_string(index=False))

# 资金流小结
core_codes = m.loc[m.npure>0, "stock_code"].tolist()
flow_core = pd.read_sql("""
select c.trade_date, sum(c.main_net)/1e8 main_yi
from capital_flow c
where c.stock_code = any(%s) and c.trade_date >= %s::date - interval '70 days'
group by c.trade_date order by c.trade_date""", CONN, params=(core_codes, VAL_DATE))
print(f"\n[全板块288] 近60日主力净额 {flow.main_yi.sum():.0f}亿  近20日 {flow.main_yi.tail(20).sum():.0f}亿")
print(f"[核心109]   近60日主力净额 {flow_core.main_yi.sum():.0f}亿  近20日 {flow_core.main_yi.tail(20).sum():.0f}亿")

top = m.head(20).copy()
out = dict(
    val_date=VAL_DATE,
    n_total=len(m), n_profit=int(m.profitable.sum()),
    n_pure=int((m.npure > 0).sum()),
    board_curve=dict(
        dates=[str(d.date()) for d in bd.trade_date],
        board=[round(float(c/bd.close.iloc[0]-1), 4) for c in bd.close],
        hs300=[round(float(hs.close.asof_locate if False else v), 4) for v in
               (hs.set_index("trade_date").close.reindex(bd.trade_date, method="ffill")/hs.close.iloc[0]-1)]),
    flow=dict(dates=[str(d.date()) for d in pd.to_datetime(flow.trade_date)],
              main_yi=[round(float(v), 3) for v in flow.main_yi],
              cum=[round(float(v), 2) for v in flow.main_yi.cumsum()]),
    top=json.loads(top[["stock_code","name","mv_yi","pe_ttm","pb","ps","ps_pct",
                        "r20","r60","npure","profitable"]].round(4).to_json(orient="records", force_ascii=False)),
    # 估值分位分布(全体有 PS 分位的)
    ps_pct_dist=[round(float(v), 3) for v in m.ps_pct.dropna()],
    mv_yi_all=[round(float(v), 1) for v in m.mv_yi.dropna()],
)
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "aerospace_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False)
print("saved:", path)
