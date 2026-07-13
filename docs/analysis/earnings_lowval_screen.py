# -*- coding: utf-8 -*-
"""todo#41:近两日中报预告"业绩好但估值低迷"筛选(全库内数据)。

口径:
- 样本:fin_forecast 归母净利预告,ann_date >= ANN_FROM,report_date=2026-06-30;
  预告半年净利 >= 5000 万且同比 >= +50%(排除低基数微利股)。
- 估值低迷三维度:
  1) 隐含PE = 最新 total_mv ÷ (预告半年净利 × 2)  —— 粗年化,忽略季节性;
  2) PE_TTM 自身近 3 年分位(当前值在历史日频分布中的百分位,越低越"冷");
  3) 近 20 个交易日涨幅(是否已被抢跑)。
- 行业:富途行业板块(board_member 现役区间 join board)。
"""
import json
import os
import numpy as np
import pandas as pd
import psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")
ANN_FROM = "2026-07-11"   # "最近两天":7-13/7-14 为主,7-11 周末批次一并纳入(表内标注)
VAL_DATE = "2026-07-13"   # 最新估值/价格交易日

cand = pd.read_sql("""
select f.stock_code, b.name, f.ann_date, f.forecast_value/1e8 h1_yi, f.change_pct,
       v.pe_ttm, v.pb, v.total_mv/1e8 mv_yi,
       v.total_mv / nullif(f.forecast_value*2, 0) implied_pe
from fin_forecast f
join stock_basic b using(stock_code)
join daily_valuation v on v.stock_code=f.stock_code and v.trade_date=%s
where f.ann_date >= %s and f.forecast_type='归属于上市公司股东的净利润'
  and f.report_date='2026-06-30'
  and f.forecast_value >= 5e7 and f.change_pct >= 50
""", CONN, params=(VAL_DATE, ANN_FROM))
print(f"候选(业绩好): {len(cand)} 只")

codes = list(cand.stock_code)
# PE_TTM 近3年分位
hist = pd.read_sql(
    "select stock_code, trade_date, pe_ttm from daily_valuation "
    "where stock_code = any(%s) and trade_date >= %s::date - interval '3 years' "
    "and pe_ttm is not null and pe_ttm > 0", CONN, params=(codes, VAL_DATE))
pctile = {}
for c, g in hist.groupby("stock_code"):
    cur = g.loc[g.trade_date.idxmax(), "pe_ttm"]
    pctile[c] = dict(pe_pctile=float((g.pe_ttm <= cur).mean()), hist_days=len(g))
cand["pe_pctile"] = cand.stock_code.map(lambda c: pctile.get(c, {}).get("pe_pctile"))
cand["hist_days"] = cand.stock_code.map(lambda c: pctile.get(c, {}).get("hist_days"))

# 近20日涨幅
px = pd.read_sql(
    "select stock_code, trade_date, close from daily_price "
    "where stock_code = any(%s) and trade_date >= %s::date - interval '45 days' "
    "order by stock_code, trade_date", CONN, params=(codes, VAL_DATE))
ret20 = {}
for c, g in px.groupby("stock_code"):
    if len(g) >= 21:
        ret20[c] = float(g.close.iloc[-1] / g.close.iloc[-21] - 1)
cand["ret20"] = cand.stock_code.map(ret20)

# 行业(富途现役)
ind = pd.read_sql("""
select m.stock_code, min(bd.board_name) industry
from board_member m join board bd on bd.board_code=m.board_code
where m.stock_code = any(%s) and m.valid_to is null
  and bd.board_type='industry' and bd.source='futu'
group by m.stock_code""", CONN, params=(codes,))
cand = cand.merge(ind, on="stock_code", how="left")

# 低迷过滤:隐含PE<25 且 (PE分位<0.4 或 近20日涨幅<5%);按隐含PE排序
cand = cand[cand.implied_pe.notna() & (cand.implied_pe > 0)]
sel = cand[(cand.implied_pe < 25) &
           ((cand.pe_pctile < 0.4) | (cand.ret20 < 0.05))].copy()
sel = sel.sort_values("implied_pe").reset_index(drop=True)

pd.set_option("display.width", 240, "display.max_rows", 60)
cols = ["stock_code", "name", "industry", "ann_date", "h1_yi", "change_pct",
        "implied_pe", "pe_ttm", "pe_pctile", "pb", "mv_yi", "ret20", "hist_days"]
print(sel[cols].round(3).to_string(index=False))
print(f"\n入选 {len(sel)} / 候选 {len(cand)}")

out = dict(
    ann_from=ANN_FROM, val_date=VAL_DATE,
    n_forecast_total=int(pd.read_sql(
        "select count(distinct stock_code) n from fin_forecast where ann_date >= %s "
        "and forecast_type='归属于上市公司股东的净利润' and report_date='2026-06-30'",
        CONN, params=(ANN_FROM,)).n[0]),
    n_good=len(cand), n_sel=len(sel),
    rows=json.loads(sel[cols].round(4).to_json(orient="records", force_ascii=False)),
    scatter=json.loads(cand[cols].round(4).to_json(orient="records", force_ascii=False)),
)
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "earnings_lowval_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False)
print("saved:", path)
