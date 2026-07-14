# -*- coding: utf-8 -*-
"""宏和科技(603256)涨势斜率拟合:对数线性(指数增长率)+ 分段加速检测。"""
import numpy as np, pandas as pd, psycopg2
CONN = psycopg2.connect(dbname="astock", user="zhu")

px = pd.read_sql("select trade_date, close from daily_price where stock_code='603256.SH' "
  "and trade_date>='2024-12-31' order by trade_date", CONN, parse_dates=["trade_date"])
px["t"] = range(len(px))          # 交易日序号
px["ln"] = np.log(px.close.astype(float))
N = len(px)

def fit(sub, label):
    b, a = np.polyfit(sub.t, sub.ln, 1)   # ln = a + b*t
    daily = np.exp(b) - 1
    ann = np.exp(b*244) - 1
    r2 = 1 - np.sum((sub.ln - (a+b*sub.t))**2)/np.sum((sub.ln - sub.ln.mean())**2)
    print(f"[{label}] n={len(sub)}  日均对数斜率 b={b:.4f}  日涨 {daily*100:.2f}%  "
          f"年化 {ann*100:.0f}%  翻倍需 {np.log(2)/b:.0f} 交易日  R²={r2:.3f}")
    return b

print(f"区间 2024末→今:{px.close.iloc[0]:.2f} → {px.close.iloc[-1]:.2f} = {px.close.iloc[-1]/px.close.iloc[0]:.1f}x  共{N}个交易日\n")
b_all = fit(px, "全程 2025-01~2026-07")
# 半年
h = px[px.t >= N-122]
fit(h, "近半年")
# 近3个月/近1月
fit(px[px.t >= N-61], "近3个月")
fit(px[px.t >= N-21], "近1个月")

# 加速检测:60日滚动对数斜率
print("\n60日滚动对数斜率(年化%)——看斜率是否递增(加速):")
roll = []
for i in range(60, N, 20):
    sub = px.iloc[i-60:i]
    b, _ = np.polyfit(sub.t, sub.ln, 1)
    roll.append((str(px.trade_date.iloc[i-1].date()), round((np.exp(b*244)-1)*100)))
for d, v in roll:
    print(f"  截至 {d}: 年化 {v}%")

# 分段拐点:找斜率最陡的60日窗口
best = max(range(60, N), key=lambda i: np.polyfit(px.t.iloc[i-60:i], px.ln.iloc[i-60:i], 1)[0])
bb, _ = np.polyfit(px.t.iloc[best-60:best], px.ln.iloc[best-60:best], 1)
print(f"\n最陡60日窗口:截至 {px.trade_date.iloc[best-1].date()},年化 {(np.exp(bb*244)-1)*100:.0f}%,"
      f"日涨 {(np.exp(bb)-1)*100:.2f}%")
