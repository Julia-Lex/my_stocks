# -*- coding: utf-8 -*-
"""todo#54:找"宏和科技式"潜力股——业绩催化 + 少量资金可拉动(全库内数据 + 代理)。

宏和(603256)画像:2024末 8.35 → 现 206(~25x),半年 5.7x;PE 599;实控人控盘 81.75%
→ 有效流通极小、少量资金可拉盘。库里无控盘数据(已登记移交#8),故用代理:
- 业绩催化 = fin_forecast 中报预告净利同比≥100% 或 扭亏(强拐点);
- "少量资金可拉动" 代理 = 流通市值小(float_shares×收盘;越小越易拉)+ 近 60 日日均换手低;
- 动量:近 60 日涨幅(判断是否已启动;宏和是已启动型,也保留未启动的潜伏型);
控盘度必须人工核十大股东公告,本筛选只给"具备业绩+小流通"底子的候选池。
"""
import json, os
import numpy as np, pandas as pd, psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")
VAL_DATE = "2026-07-14"   # 有当日收盘? 若无回退 7-13
ANN_FROM = "2026-06-25"

# 有效交易日(取最新)
last_td = pd.read_sql("select max(trade_date) d from daily_price", CONN).d[0]
VAL_DATE = str(last_td)
print("最新交易日", VAL_DATE)

fc = pd.read_sql("""
select f.stock_code, b.name, f.ann_date, f.forecast_value/1e8 np_yi, f.change_pct yoy, f.change_desc
from fin_forecast f join stock_basic b using(stock_code)
where f.report_date='2026-06-30' and f.forecast_type='归属于上市公司股东的净利润'
  and f.ann_date >= %s
""", CONN, params=(ANN_FROM,), parse_dates=["ann_date"]).drop_duplicates("stock_code")
# 强催化:同比≥100% 或 扭亏(change_desc 含"扭亏")
fc["turnaround"] = fc.change_desc.str.contains("扭亏", na=False)
strong = fc[(fc.yoy >= 100) | fc.turnaround].copy()
codes = strong.stock_code.tolist()
print(f"强业绩催化(同比≥100%或扭亏): {len(strong)} 只")

# 流通市值 = 最新 float_shares × 最新收盘;换手、市值、涨幅
val = pd.read_sql("select stock_code, total_mv/1e8 mv_yi, pe_ttm, pb from daily_valuation "
                  "where trade_date=%s and stock_code=any(%s)", CONN, params=(VAL_DATE, codes))
sc = pd.read_sql("""select distinct on (stock_code) stock_code, float_shares, total_shares
  from share_capital where stock_code=any(%s) order by stock_code, change_date desc""",
  CONN, params=(codes,))
px = pd.read_sql("select stock_code, trade_date, close, turnover from daily_price "
  "where stock_code=any(%s) and trade_date >= %s::date - interval '100 days' order by 1,2",
  CONN, params=(codes, VAL_DATE), parse_dates=["trade_date"])
ind = pd.read_sql("""select m.stock_code, min(bd.board_name) industry from board_member m
  join board bd on bd.board_code=m.board_code where m.stock_code=any(%s) and m.valid_to is null
  and bd.board_type='industry' and bd.source='futu' group by m.stock_code""", CONN, params=(codes,))

feat = {}
for c, g in px.groupby("stock_code"):
    g = g.reset_index(drop=True)
    last = float(g.close.iloc[-1])
    r60 = float(last / g.close.iloc[0] - 1) if len(g) >= 55 else None
    r20 = float(last / g.close.iloc[-21] - 1) if len(g) >= 21 else None
    to = float(g.turnover.tail(60).mean()) if g.turnover.notna().any() else None
    feat[c] = dict(close=last, r60=r60, r20=r20, turn60=to)

m = strong.merge(val, on="stock_code", how="left").merge(sc, on="stock_code", how="left").merge(ind, on="stock_code", how="left")
m["close"] = m.stock_code.map(lambda c: feat.get(c, {}).get("close"))
m["r60"] = m.stock_code.map(lambda c: feat.get(c, {}).get("r60"))
m["r20"] = m.stock_code.map(lambda c: feat.get(c, {}).get("r20"))
m["turn60"] = m.stock_code.map(lambda c: feat.get(c, {}).get("turn60"))
m["float_mv_yi"] = m.float_shares * m.close / 1e8
m["float_ratio"] = 100 * m.float_shares / m.total_shares
m["industry"] = m.industry.fillna("其他")

# 潜伏型(未大涨,底子在):流通市值<80亿 且 近60日涨幅<50% 且 换手不高
# 已启动型:近60日涨幅>50%(宏和式,资金已进场)
m = m[m.float_mv_yi.notna()].copy()
m["type"] = np.where(m.r60.fillna(0) > 0.5, "已启动", "潜伏")
m = m.sort_values(["float_mv_yi"]).reset_index(drop=True)

pd.set_option("display.width", 240, "display.max_rows", 80)
cols = ["stock_code","name","industry","yoy","np_yi","mv_yi","float_mv_yi","float_ratio","turn60","r20","r60","type"]
# 小流通市值优先(<100亿),强业绩
cand = m[(m.float_mv_yi < 50) & (m.np_yi >= 0.3) & (~m.name.str.contains("ST"))].copy()
print(f"\n候选(流通市值<50亿+净利≥0.3亿+非ST+强业绩): {len(cand)} 只:")
print(cand[cols].round(2).head(40).to_string(index=False))

out = dict(val_date=VAL_DATE, honghe=dict(code="603256.SH", name="宏和科技", mv_yi=1864, pe=599,
             gain_since_2024=24.7, gain_half=5.7, control_pct=81.75, float_ratio=97.3),
           n_strong=len(strong), n_cand=len(cand),
           rows=json.loads(cand[cols].round(4).to_json(orient="records", force_ascii=False)))
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "honghe_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False)
print("saved", path)
