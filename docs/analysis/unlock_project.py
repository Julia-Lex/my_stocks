# -*- coding: utf-8 -*-
"""MiniMax 抛压外推:基于对照组量能衰减规律 + 自身前两日观测."""
import json
import os
import numpy as np

SP = os.path.dirname(os.path.abspath(__file__))
data = json.load(open(f"{SP}/unlock_data.json"))
curves = data["curves"]

UNLOCK_SHARES = 153_000_000      # 解禁约1.53亿股(占总股本48.9%)
TOTAL_SHARES = 313_000_000       # 总股本约3.13亿股(908亿港元市值/289.6收盘价)
mm = curves["00100.HK"]
base_vol = 2_195_123.5           # T-25..T-6 中位数日成交量(股)

# --- 对照组量能衰减:A+H 组 (剔除只有2天数据的智谱/自身) vol_mult-1 的中位数曲线 ---
ah = [c for k, c in curves.items() if c["typ"] == "A+H"]
maxk = 60
med_curve = []
for k in range(maxk + 1):
    vals = [c["vol_mult"][k] for c in ah if len(c["vol_mult"]) > k]
    med_curve.append(float(np.median(vals)))

# 高倍数子组(T0 倍数>4: 宁德/蓝思/三花)更贴近 MiniMax 的 9.5x
hi = [c for c in ah if c["vol_mult"][0] > 4]
hi_curve = [float(np.median([c["vol_mult"][k] for c in hi if len(c["vol_mult"]) > k])) for k in range(maxk + 1)]
print("对照组T0倍数>4子组:", [c["name"] for c in hi])

# 对高倍数子组拟合 (mult-1) = (m0-1)*exp(-lam*k), 用前20天
y = np.array(hi_curve[:21]) - 1
y = np.clip(y, 1e-6, None)
k = np.arange(21)
lam_hi = -np.polyfit(k, np.log(y), 1)[0]
print(f"高倍数子组衰减率 lambda={lam_hi:.3f} (半衰期 {np.log(2)/lam_hi:.1f} 天)")

# MiniMax 自身两日衰减率
m0, m1 = mm["vol_mult"][0], mm["vol_mult"][1]
lam_own = np.log((m0 - 1) / (m1 - 1))
print(f"MiniMax 自身两日衰减率 lambda={lam_own:.3f} (半衰期 {np.log(2)/lam_own:.1f} 天)")

obs_excess = sum(mm["excess_vol"])
print(f"\n已观测超额成交(T0+T1): {obs_excess/1e6:.1f}M 股 = 解禁量的 {obs_excess/UNLOCK_SHARES:.1%}")

def project(lam, label):
    """从 T+2 起外推: excess_k = (m1-1)*base*exp(-lam*(k-1)), 至 5日均量<1.2x."""
    total = obs_excess
    days_to_norm = None
    k = 2
    recent = [m0, m1]
    while k <= 120:
        mult = 1 + (m1 - 1) * np.exp(-lam * (k - 1))
        recent.append(mult)
        total += (mult - 1) * base_vol
        if days_to_norm is None and len(recent) >= 5 and np.mean(recent[-5:]) < 1.2:
            days_to_norm = k
        k += 1
    print(f"[{label}] lam={lam:.3f}: 量能回归(5日均<1.2x)约 T+{days_to_norm}; "
          f"外推总抛售 ≈ {total/1e6:.0f}M 股 = 解禁量的 {total/UNLOCK_SHARES:.1%}")
    return dict(label=label, lam=round(float(lam), 3), days_to_norm=days_to_norm,
                total_sold=int(total), sold_pct=round(total / UNLOCK_SHARES, 3))

scen = [project(lam_own, "快衰减(自身两日速率)"),
        project(lam_hi, "慢衰减(对照组高倍数子组速率)")]

# 目标抛售比例视角: 若最终有 S% 解禁股要卖, 按慢衰减节奏何时卖完
def days_to_sell(target_frac, lam):
    target = target_frac * UNLOCK_SHARES
    cum = 0.0
    for k in range(0, 121):
        mult = m0 if k == 0 else 1 + (m1 - 1) * np.exp(-lam * (k - 1))
        cum += (mult - 1) * base_vol
        if cum >= target:
            return k
    return None

print("\n若最终抛售比例为 X% 解禁股, 按慢衰减节奏需要的交易日:")
sell_days = {}
for s in (0.15, 0.20, 0.30, 0.40):
    d = days_to_sell(s, lam_hi)
    sell_days[s] = d
    print(f"  {s:.0%} ({s*UNLOCK_SHARES/1e6:.0f}M股): T+{d}" if d else f"  {s:.0%}: 120日内卖不完(量能先枯竭)")

# 对照组摘要给 artifact 用
data["projection"] = dict(
    unlock_shares=UNLOCK_SHARES, total_shares=TOTAL_SHARES, base_vol=base_vol,
    obs_excess=int(obs_excess), scenarios=scen,
    sell_days={str(k): v for k, v in sell_days.items()},
    med_curve=[round(v, 3) for v in med_curve], hi_curve=[round(v, 3) for v in hi_curve],
    hi_names=[c["name"] for c in hi], lam_own=round(float(lam_own), 3), lam_hi=round(float(lam_hi), 3),
)
json.dump(data, open(f"{SP}/unlock_data.json", "w"), ensure_ascii=False)
print("\nsaved projection")
