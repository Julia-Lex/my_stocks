# -*- coding: utf-8 -*-
"""长鑫科技 国产替代×周期 逐年 PE 推演(2026-2030)——自下而上、内部一致。

⚠️ 情景推演非预测。库里无全球DRAM市场/份额/价格(已登记移交#5#7),全部公开假设。
方法:营收=全球DRAM市场×长鑫份额;净利=营收×净利率;PE=固定市值÷净利。
关键洞察:在 2 万亿市值下,PE 由"份额×净利率"决定——只有当长鑫做到接近全球
一线份额(利润~千亿级)时 PE 才落到 ~20x;近期真实份额(5-7%)对应 PE 远高于此。
即 2 万亿是"长期国产替代成功"的定价,不是"便宜的周期股"。
"""
import json, os

YEARS = [2026, 2027, 2028, 2029, 2030]
MKT = 20000                      # 假设市值 2 万亿(亿元),固定看 PE 随基本面变化
USD = 7.2
# 全球 DRAM 市场(亿美元):2026 高位→2027-28 周期回落→2029-30 AI 驱动再上
DRAM = {2026:1000, 2027:850, 2028:950, 2029:1100, 2030:1250}

# 情景:全球份额% 与 净利率%(周期调整,DRAM 峰值净利率可达 35-40%,谷底个位数)
SCEN = {
  "乐观(替代快+周期强)":   dict(share=[8,11,14,17,20],  margin=[35,32,30,32,34], color="good"),
  "中性(份额稳升+周期波动)": dict(share=[7,9,11,13,15],  margin=[30,22,22,26,28], color="blue"),
  "悲观(2027-28周期大跌)":  dict(share=[6,7,8,10,12],    margin=[22,8,6,14,20],  color="bad"),
}

out = dict(mkt_cap=MKT, years=YEARS, dram=DRAM, hynix_pe=10, scenarios={})
for name, s in SCEN.items():
    rev = [round(DRAM[y]*sh/100*USD,0) for y,sh in zip(YEARS,s["share"])]      # 亿元
    prof = [round(r*m/100,0) for r,m in zip(rev,s["margin"])]
    pe = [round(MKT/p,1) if p>0 else None for p in prof]
    out["scenarios"][name]=dict(share=s["share"],margin=s["margin"],rev=rev,profit=prof,pe=pe,color=s["color"])
    print(f"{name}")
    print(f"  份额% {s['share']}  净利率% {s['margin']}")
    print(f"  营收亿 {rev}")
    print(f"  净利亿 {prof}")
    print(f"  PE(市值2万亿) {pe}   (对照海力士 ~10x)")
path=os.path.join(os.path.dirname(os.path.abspath(__file__)),"cxmt_projection_data.json")
json.dump(out,open(path,"w"),ensure_ascii=False)
print("saved",path)
