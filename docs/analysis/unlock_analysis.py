# -*- coding: utf-8 -*-
"""港股解禁事件研究:MiniMax(00100.HK) 2026-07-09 解禁抛压 vs 近一年 A+H/AI 新股基石解禁."""
import json
import os
import numpy as np
import pandas as pd
import psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")

# 事件表:code -> (名称, 上市日, 解禁日(上市+6个月, 顺延到交易日), 类型)
EVENTS = {
    "03750.HK": ("宁德时代", "2025-05-20", "2025-11-20", "A+H"),
    "01276.HK": ("恒瑞医药", "2025-05-23", "2025-11-24", "A+H"),
    "02603.HK": ("吉宏股份", "2025-05-27", "2025-11-27", "A+H"),
    "03288.HK": ("海天味业", "2025-06-19", "2025-12-19", "A+H"),
    "02050.HK": ("三花智控", "2025-06-23", "2025-12-23", "A+H"),
    "02648.HK": ("安井食品", "2025-07-04", "2026-01-05", "A+H"),
    "06613.HK": ("蓝思科技", "2025-07-09", "2026-01-09", "A+H"),
    "06693.HK": ("赤峰黄金", "2025-03-10", "2025-09-10", "A+H"),
    "00699.HK": ("均胜电子", "2025-11-06", "2026-05-06", "A+H"),
    "02513.HK": ("智谱", "2026-01-08", "2026-07-08", "AI"),
    "00100.HK": ("MiniMax", "2026-01-09", "2026-07-09", "AI"),
}

codes = list(EVENTS)
px = pd.read_sql(
    "select stock_code, trade_date, close, pre_close, volume, amount, pct_chg, turnover "
    "from hk_daily_price where stock_code = any(%s) order by stock_code, trade_date",
    CONN, params=(codes,), parse_dates=["trade_date"],
)
hsi = pd.read_sql(
    "select trade_date, close from hk_index_daily where index_code='HSI' order by trade_date",
    CONN, parse_dates=["trade_date"],
).set_index("trade_date")["close"]

BASE_WIN = (25, 6)   # 基线窗口: T-25 .. T-6 (20个交易日, 避开解禁前一周的抢跑)
HORIZON = 60

results, curves = [], {}
for code, (name, list_d, unlock_d, typ) in EVENTS.items():
    g = px[px.stock_code == code].reset_index(drop=True)
    dates = g.trade_date
    # 解禁日对齐到 >= unlock_d 的首个交易日
    pos = dates.searchsorted(pd.Timestamp(unlock_d))
    if pos >= len(g):
        continue
    t0_date = dates.iloc[pos]
    lo, hi = pos - BASE_WIN[0], pos - BASE_WIN[1] + 1
    base = g.iloc[max(lo, 0):max(hi, 0)]
    base_vol = float(base.volume.median())
    base_amt = float(base.amount.median())

    post = g.iloc[pos:pos + HORIZON + 1].copy()
    post["k"] = range(len(post))
    post["vol_mult"] = post.volume / base_vol
    post["excess_vol"] = (post.volume - base_vol).clip(lower=0)

    pre_close = float(g.close.iloc[pos - 1])
    post["cumret"] = post.close.astype(float) / pre_close - 1
    # 市场调整(HSI 同窗口)
    hsi_pre = hsi.asof(g.trade_date.iloc[pos - 1])
    post["hsi_cumret"] = [hsi.asof(d) / hsi_pre - 1 for d in post.trade_date]
    post["adj_cumret"] = post.cumret - post.hsi_cumret

    # 量能消化天数: 滚动5日均量首次 < 1.5x / 1.2x 基线
    roll5 = post.volume.rolling(5).mean() / base_vol
    def first_below(th):
        idx = np.where(roll5.values < th)[0]
        return int(idx[0]) if len(idx) else None
    d15, d12 = first_below(1.5), first_below(1.2)

    trough_i = int(np.argmin(post.cumret.values))
    runup_lo = max(pos - 21, 0)
    runup = float(g.close.iloc[pos - 1]) / float(g.close.iloc[runup_lo]) - 1

    def cum_excess(days):
        return float(post.excess_vol.iloc[:days + 1].sum()) if len(post) > days else None

    results.append(dict(
        code=code, name=name, typ=typ, list_date=list_d,
        unlock_date=str(t0_date.date()), post_days=len(post) - 1,
        base_vol=base_vol, base_amt=base_amt,
        runup_20d=round(runup, 4),
        ret_t0=round(float(post.cumret.iloc[0]), 4),
        vol_mult_t0=round(float(post.vol_mult.iloc[0]), 2),
        vol_mult_t1=round(float(post.vol_mult.iloc[1]), 2) if len(post) > 1 else None,
        digest_days_15=d15, digest_days_12=d12,
        cum_excess_5d=cum_excess(5), cum_excess_20d=cum_excess(20),
        cum_excess_60d=cum_excess(60),
        trough_day=trough_i, trough_ret=round(float(post.cumret.iloc[trough_i]), 4),
        ret_5d=round(float(post.cumret.iloc[5]), 4) if len(post) > 5 else None,
        ret_10d=round(float(post.cumret.iloc[10]), 4) if len(post) > 10 else None,
        ret_20d=round(float(post.cumret.iloc[20]), 4) if len(post) > 20 else None,
        ret_40d=round(float(post.cumret.iloc[40]), 4) if len(post) > 40 else None,
        ret_60d=round(float(post.cumret.iloc[60]), 4) if len(post) > 60 else None,
        adj_ret_20d=round(float(post.adj_cumret.iloc[20]), 4) if len(post) > 20 else None,
        adj_ret_60d=round(float(post.adj_cumret.iloc[60]), 4) if len(post) > 60 else None,
    ))
    curves[code] = dict(
        name=name, typ=typ,
        k=post.k.tolist(),
        dates=[str(d.date()) for d in post.trade_date],
        vol_mult=[round(v, 3) for v in post.vol_mult],
        cumret=[round(v, 4) for v in post.cumret],
        adj_cumret=[round(v, 4) for v in post.adj_cumret],
        excess_vol=[int(v) for v in post.excess_vol],
        close=[float(c) for c in post.close],
        volume=[int(v) for v in post.volume],
    )

res = pd.DataFrame(results).set_index("code")
pd.set_option("display.width", 250, "display.max_columns", 50)
print(res.to_string())

# ---- MiniMax 全历史(供走势图与基线核对) ----
mm = px[px.stock_code == "00100.HK"].copy()
print("\nMiniMax 最近 15 个交易日:")
print(mm[["trade_date", "close", "pct_chg", "volume", "amount", "turnover"]].tail(15).to_string(index=False))

mm_hist = dict(
    dates=[str(d.date()) for d in mm.trade_date],
    close=[float(c) for c in mm.close],
    volume=[int(v) for v in mm.volume],
    amount=[float(a) if a is not None else None for a in mm.amount],
)

out = dict(results=results, curves=curves, minimax_hist=mm_hist)
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "unlock_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False)
print("\nsaved:", path)
