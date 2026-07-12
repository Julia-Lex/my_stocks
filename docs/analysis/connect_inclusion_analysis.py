# -*- coding: utf-8 -*-
"""港股通纳入事件研究:近两年新股入通前后的量价表现(todo#6, MiniMax 入通预判)。

口径:
- T0 = 入通生效日(非公告日;公告一般提前 ~2 周,故 T-20 起的窗口能覆盖抢跑段)。
- 基线成交量 = T-45..T-26 中位数(公告前,避免公告后的抢跑放量污染基线)。
- 累计涨跌以 T-21 收盘为锚(展示 抢跑段 + 入通后 全路径);市场调整用恒指同窗口。
"""
import json
import os
import numpy as np
import pandas as pd
import psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")

# code -> (名称, 入通生效日, 分组)
EVENTS = {
    "02513.HK": ("智谱", "2026-06-08", "AI·6/8批"),
    "06082.HK": ("壁仞科技", "2026-06-08", "AI·6/8批"),
    "09903.HK": ("天数智芯", "2026-06-08", "AI·6/8批"),
    "02675.HK": ("精锋医疗-B", "2026-06-08", "6/8批"),
    "02706.HK": ("海致科技", "2026-06-08", "6/8批"),
    "01768.HK": ("鸣鸣很忙", "2026-06-08", "6/8批"),
    "02026.HK": ("小马智行-W", "2026-06-04", "WVR"),
    "00800.HK": ("文远知行-W", "2026-06-04", "WVR"),
    "09660.HK": ("地平线机器人-W", "2025-05-26", "WVR"),
    "02097.HK": ("蜜雪集团", "2025-06-09", "消费批"),
    "01364.HK": ("古茗", "2025-06-09", "消费批"),
    "00325.HK": ("布鲁可", "2025-06-09", "消费批"),
    "06181.HK": ("老铺黄金", "2024-09-10", "先例"),
}
PRE, POST = 20, 60          # 展示窗口
BASE = (45, 26)             # 基线窗口(相对 T0 的交易日偏移)

codes = list(EVENTS)
px = pd.read_sql(
    "select stock_code, trade_date, close, volume from hk_daily_price "
    "where stock_code = any(%s) order by stock_code, trade_date",
    CONN, params=(codes,), parse_dates=["trade_date"],
)
hsi = pd.read_sql(
    "select trade_date, close from hk_index_daily where index_code='HSI' order by trade_date",
    CONN, parse_dates=["trade_date"],
).set_index("trade_date")["close"]

results, curves = [], {}
for code, (name, incl_d, grp) in EVENTS.items():
    g = px[px.stock_code == code].reset_index(drop=True)
    pos = g.trade_date.searchsorted(pd.Timestamp(incl_d))
    if pos >= len(g) or pos < BASE[0]:
        # 上市到入通不足基线窗口时放宽:基线改用上市后可得区间
        lo, hi = max(pos - BASE[0], 0), max(pos - BASE[1] + 1, 1)
    else:
        lo, hi = pos - BASE[0], pos - BASE[1] + 1
    base_vol = float(g.volume.iloc[lo:hi].median())

    a = max(pos - PRE - 1, 0)                 # 含锚点 T-(PRE+1)
    post = g.iloc[a:pos + POST + 1].copy()
    post["k"] = range(a - pos, len(post) + a - pos)
    anchor = float(post.close.iloc[0])
    post["cumret"] = post.close.astype(float) / anchor - 1
    hsi_anchor = hsi.asof(post.trade_date.iloc[0])
    post["adj_cumret"] = post.cumret - np.array([hsi.asof(d) / hsi_anchor - 1 for d in post.trade_date])
    post["vol_mult"] = post.volume / base_vol

    def at(k):
        row = post[post.k == k]
        return float(row.cumret.iloc[0]) if len(row) else None
    def vm(k):
        row = post[post.k == k]
        return float(row.vol_mult.iloc[0]) if len(row) else None

    pre_close = at(-1)
    t0 = at(0)
    pk = post[post.k >= 0]
    results.append(dict(
        code=code, name=name, grp=grp, incl_date=incl_d,
        post_days=int(post.k.max()), base_vol=base_vol,
        runup_20d=round(pre_close, 4) if pre_close is not None else None,   # T-21→T-1
        ret_t0=round((1 + t0) / (1 + pre_close) - 1, 4) if None not in (t0, pre_close) else None,
        vol_mult_t0=round(vm(0), 2) if vm(0) else None,
        vol_mult_5d=round(float(pk.vol_mult.iloc[:6].mean()), 2) if len(pk) else None,
        ret_5d=round((1 + at(5)) / (1 + pre_close) - 1, 4) if None not in (at(5), pre_close) else None,
        ret_20d=round((1 + at(20)) / (1 + pre_close) - 1, 4) if None not in (at(20), pre_close) else None,
        ret_60d=round((1 + at(60)) / (1 + pre_close) - 1, 4) if None not in (at(60), pre_close) else None,
        adj_ret_20d=round(float(post[post.k == 20].adj_cumret.iloc[0]) - float(post[post.k == -1].adj_cumret.iloc[0]), 4)
            if len(post[post.k == 20]) and len(post[post.k == -1]) else None,
        adj_ret_60d=round(float(post[post.k == 60].adj_cumret.iloc[0]) - float(post[post.k == -1].adj_cumret.iloc[0]), 4)
            if len(post[post.k == 60]) and len(post[post.k == -1]) else None,
    ))
    curves[code] = dict(
        name=name, grp=grp,
        k=post.k.tolist(),
        dates=[str(d.date()) for d in post.trade_date],
        cumret=[round(v, 4) for v in post.cumret],
        adj_cumret=[round(v, 4) for v in post.adj_cumret],
        vol_mult=[round(v, 3) for v in post.vol_mult],
        close=[float(c) for c in post.close],
    )

res = pd.DataFrame(results)
pd.set_option("display.width", 250, "display.max_columns", 40)
print(res.to_string(index=False))

# MiniMax 现状(供报告时间轴):最近 40 个交易日
mm = px_mm = pd.read_sql(
    "select trade_date, close, volume, pct_chg from hk_daily_price "
    "where stock_code='00100.HK' order by trade_date",
    CONN, parse_dates=["trade_date"])
mm_hist = dict(dates=[str(d.date()) for d in mm.trade_date],
               close=[float(c) for c in mm.close],
               volume=[int(v) for v in mm.volume])

out = dict(results=results, curves=curves, minimax_hist=mm_hist)
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "connect_data.json")
json.dump(out, open(path, "w"), ensure_ascii=False)
print("saved:", path)
