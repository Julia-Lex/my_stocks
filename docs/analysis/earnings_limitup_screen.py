# -*- coding: utf-8 -*-
"""todo#51/#52:中报季"财报驱动封板"股画像 + 用特征匹配今日预告股(全库内数据)。

口径:
- 财报事件 = fin_forecast 归母净利中报预告(report_date=2026-06-30),ann_date 在中报季内。
- "封板" = 公告日(收盘后发则次日)起 3 个交易日内出现过收盘涨停
  (主板 pct>9.8;创业板 300/301、科创板 688/689 用 20cm,pct>19.5;北交所 30cm,pct>29)。
- 特征:净利同比增速、预告净利绝对值、总市值、行业、板块、上市时长(次新?)、
  公告前 20 日涨幅(是否已抢跑)、首个涨停当日换手。
- #52:今日(最新公告日)预告股按"封板股高发特征"打分,输出高匹配候选。
"""
import json
import os
import numpy as np
import pandas as pd
import psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")
LATEST = "2026-07-14"      # 最新公告日(今日,收盘后才有行情)
VAL_DATE = "2026-07-13"    # 最新有估值/收盘的交易日

def board_of(code):
    p = code[:3]
    if p in ("300", "301"): return "创业板"
    if p in ("688", "689"): return "科创板"
    if p in ("83", "87", "88", "92") or code[:2] in ("43", "83", "87", "88", "92"): return "北交所"
    return "主板"

def limit_thresh(code):
    b = board_of(code)
    return 9.8 if b == "主板" else (29.0 if b == "北交所" else 19.5)

# 1) 中报预告池(归母净利)
fc = pd.read_sql("""
select f.stock_code, b.name, f.ann_date, f.forecast_value/1e8 np_yi, f.change_pct yoy
from fin_forecast f join stock_basic b using(stock_code)
where f.report_date='2026-06-30' and f.forecast_type='归属于上市公司股东的净利润'
  and f.ann_date >= '2026-06-25'
""", CONN, parse_dates=["ann_date"])
fc = fc.dropna(subset=["yoy"]).drop_duplicates("stock_code")
print(f"中报预告池(有同比): {len(fc)} 只")

codes = fc.stock_code.tolist()
cal = pd.read_sql("select distinct trade_date from daily_price where trade_date>='2026-06-20' order by 1",
                  CONN, parse_dates=["trade_date"]).trade_date.tolist()

px = pd.read_sql(
    "select stock_code, trade_date, pct_chg, close, turnover from daily_price "
    "where stock_code = any(%s) and trade_date >= '2026-05-20' order by 1,2",
    CONN, params=(codes,), parse_dates=["trade_date"])
val = pd.read_sql("select stock_code, total_mv/1e8 mv_yi from daily_valuation "
                  "where trade_date=%s and stock_code = any(%s)", CONN, params=(VAL_DATE, codes))
listd = pd.read_sql("select stock_code, min(trade_date) list_d from daily_price "
                    "where stock_code = any(%s) group by 1", CONN, params=(codes,), parse_dates=["list_d"])
ind = pd.read_sql("""select m.stock_code, min(bd.board_name) industry
  from board_member m join board bd on bd.board_code=m.board_code
  where m.stock_code = any(%s) and m.valid_to is null and bd.board_type='industry' and bd.source='futu'
  group by m.stock_code""", CONN, params=(codes,))

pxg = {c: g.reset_index(drop=True) for c, g in px.groupby("stock_code")}

def analyze(row):
    code = row.stock_code
    g = pxg.get(code)
    if g is None or len(g) < 21:
        return None
    # 公告日对齐:>= ann_date 的首个交易日为反应起点(盘后发→次日)
    ann = row.ann_date
    start_i = g.trade_date.searchsorted(ann)
    if start_i >= len(g):
        return None
    win = g.iloc[start_i:start_i + 3]     # 反应窗口 3 日
    thr = limit_thresh(code)
    hit = win[win.pct_chg > thr]
    limitup = len(hit) > 0
    # 连板数:从首个涨停起连续涨停
    nboard = 0
    if limitup:
        fi = win.index[win.pct_chg > thr][0]
        j = fi
        while j < len(g) and g.pct_chg.iloc[j] > thr:
            nboard += 1; j += 1
    # 公告前 20 日涨幅(抢跑)
    runup = float(g.close.iloc[start_i - 1] / g.close.iloc[max(start_i - 21, 0)] - 1) if start_i >= 1 else None
    first_to = float(hit.turnover.iloc[0]) if limitup and pd.notna(hit.turnover.iloc[0]) else None
    return dict(limitup=limitup, nboard=nboard, runup20=runup,
                first_limit_turnover=first_to, react_max=float(win.pct_chg.max()))

rows = []
for r in fc.itertuples():
    a = analyze(r)
    if a is None:
        continue
    rows.append(dict(stock_code=r.stock_code, name=r.name, ann_date=str(r.ann_date.date()),
                     np_yi=float(r.np_yi) if pd.notna(r.np_yi) else None, yoy=float(r.yoy), **a))
df = pd.DataFrame(rows)
df = df.merge(val, on="stock_code", how="left").merge(listd, on="stock_code", how="left").merge(ind, on="stock_code", how="left")
df["board"] = df.stock_code.map(board_of)
df["is_new"] = (pd.Timestamp(VAL_DATE) - df.list_d).dt.days < 365      # 次新(上市<1年)
df["industry"] = df.industry.fillna("其他")

n_lu = int(df.limitup.sum())
print(f"\n有效样本 {len(df)} 只;财报后 3 日内封板 {n_lu} 只(封板率 {n_lu/len(df):.1%})")

# 2) 封板 vs 未封板 的特征对比(都发了预告)
def cmp_feat(col, fn=lambda s: s.median()):
    a = fn(df.loc[df.limitup, col].dropna()); b = fn(df.loc[~df.limitup, col].dropna())
    return round(float(a), 3), round(float(b), 3)
feats = {}
for c in ["yoy", "np_yi", "mv_yi", "runup20", "react_max"]:
    feats[c] = dict(zip(["封板", "未封板"], cmp_feat(c)))
# 分类特征:板块/次新/行业 的封板率
def rate_by(col):
    r = df.groupby(col).limitup.agg(["mean", "count"]).sort_values("mean", ascending=False)
    return {str(k): dict(rate=round(float(v["mean"]), 3), n=int(v["count"])) for k, v in r.iterrows() if v["count"] >= 3}
board_rate = rate_by("board")
new_rate = {("次新" if k else "非次新"): v for k, v in rate_by("is_new").items()}
ind_rate = dict(list(rate_by("industry").items())[:12])

print("\n封板 vs 未封板 中位特征:", json.dumps(feats, ensure_ascii=False))
print("板块封板率:", json.dumps(board_rate, ensure_ascii=False))
print("次新封板率:", json.dumps(new_rate, ensure_ascii=False))

# 3) 封板股画像:高增速 + 小市值 + 次新/主板 的组合封板率
df["hi_growth"] = df.yoy >= 100
df["small"] = df.mv_yi < 100
df["low_runup"] = df.runup20 < 0.15
combo = df.groupby(["hi_growth", "small"]).limitup.agg(["mean", "count"])
print("\n增速×市值 分组封板率:")
print(combo.to_string())

# 封板股清单(按连板数、增速)
lu = df[df.limitup].sort_values(["nboard", "yoy"], ascending=False)
print(f"\n封板股 TOP(连板/增速): {len(lu)} 只")
print(lu.head(20)[["stock_code","name","ann_date","yoy","np_yi","mv_yi","board","nboard","runup20","industry"]].round(2).to_string(index=False))

# 4) #52:今日(LATEST)预告股,按封板股特征打分
# #52 今日池:ann_date=今日的预告股,反应尚未发生;用发布前可知特征打分。
# runup(至 7-13)用 pxg 单独算,不要求有反应窗口 bar。
tdf = fc[fc.ann_date == pd.Timestamp(LATEST)].copy()
tdf = tdf.merge(val, on="stock_code", how="left").merge(listd, on="stock_code", how="left").merge(ind, on="stock_code", how="left")
tdf["board"] = tdf.stock_code.map(board_of)
tdf["is_new"] = (pd.Timestamp(VAL_DATE) - tdf.list_d).dt.days < 365
tdf["industry"] = tdf.industry.fillna("其他")
def runup_to(code):
    g = pxg.get(code)
    if g is None or len(g) < 21: return None
    gg = g[g.trade_date <= pd.Timestamp(VAL_DATE)]
    if len(gg) < 21: return None
    return float(gg.close.iloc[-1] / gg.close.iloc[-21] - 1)
tdf["runup20"] = tdf.stock_code.map(runup_to)
tdf["limitup"] = None
today = tdf
# 评分:高增速(yoy>=100:+2, >=50:+1) + 小市值(<50:+2,<100:+1) + 未抢跑(runup20<0.1:+1) + 主板或次新(封板率高的板块)
def score(r):
    s = 0
    s += 2 if r.yoy >= 100 else (1 if r.yoy >= 50 else 0)
    s += 2 if r.mv_yi < 50 else (1 if r.mv_yi < 100 else 0)
    if pd.notna(r.runup20) and r.runup20 < 0.10: s += 1
    if r.is_new: s += 1
    return s
today["score"] = today.apply(score, axis=1)
today = today.sort_values(["score", "yoy"], ascending=False)
print(f"\n#52 今日({LATEST})预告股 {len(today)} 只,高分候选(score>=4):")
print(today[today.score >= 4][["stock_code","name","yoy","np_yi","mv_yi","board","is_new","runup20","score","limitup"]].round(2).to_string(index=False))

out = dict(
    latest=LATEST, n_total=len(df), n_limitup=n_lu, limitup_rate=round(n_lu/len(df), 4),
    feats=feats, board_rate=board_rate, new_rate=new_rate, ind_rate=ind_rate,
    combo=[dict(hi_growth=bool(k[0]), small=bool(k[1]), rate=round(float(v["mean"]), 3), n=int(v["count"]))
           for k, v in combo.iterrows()],
    limitup_list=json.loads(lu[["stock_code","name","ann_date","yoy","np_yi","mv_yi","board","nboard",
        "runup20","first_limit_turnover","is_new","industry"]].round(4).to_json(orient="records", force_ascii=False)),
    today_candidates=json.loads(today[["stock_code","name","yoy","np_yi","mv_yi","board","is_new",
        "runup20","score","limitup","industry"]].round(4).to_json(orient="records", force_ascii=False)),
    all_scatter=json.loads(df[["stock_code","name","yoy","mv_yi","limitup","nboard","runup20"]].round(4).to_json(orient="records", force_ascii=False)),
)
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "earnings_limitup_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False)
print("\nsaved:", path)
