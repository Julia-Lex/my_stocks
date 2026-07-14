# -*- coding: utf-8 -*-
"""todo#54 续:业绩催化 + 题材叙事 + 市值≤500亿(全库内数据)。

- 业绩催化 = fin_forecast 中报预告净利同比≥100% 或扭亏;
- 题材叙事 = 属于热门题材概念板(AI/算力/半导体/存储/机器人/CPO/先进封装/军工航天/固态电池等);
  题材数越多=叙事叠加越强;
- 市值 = 总市值≤500亿(用户放宽);
- 动量:近60日涨幅(潜伏=未涨,已启动=已涨);优先"多题材+潜伏"。
控盘度仍需人工核十大股东(库内缺,移交#8)。
"""
import json, os
import numpy as np, pandas as pd, psycopg2

CONN = psycopg2.connect(dbname="astock", user="zhu")
VAL_DATE = str(pd.read_sql("select max(trade_date) d from daily_price", CONN).d[0])

# 热门题材概念板(富途 concept),剔传统光伏/锂电细分
HOT = """SH.LIST23044 SH.LIST23001 SH.LIST23073 SH.LIST23604 SH.LIST23130 SH.LIST24016 SH.LIST0656
SH.LIST23132 SH.LIST23141 SH.LIST23455 SH.LIST23583 SH.LIST23042 SH.LIST23429 SH.LIST24229
SH.LIST23147 SH.LIST23146 SH.LIST23085 SH.LIST23594 SH.LIST23082 SH.LIST23089 SH.LIST23298
SH.LIST23385 SH.LIST23386 SH.LIST0594 SH.LIST23857 SH.LIST23106 SH.LIST0701 SH.LIST0718
SH.LIST23306 SH.LIST0636 SH.LIST23297 SH.LIST23120 SH.LIST23122 SH.LIST23133 SH.LIST23303
SH.LIST23307 SH.LIST23309 SH.LIST23079 SH.LIST23064 SH.LIST23123 SH.LIST0463 SH.LIST23134
SH.LIST0393 SH.LIST23917 SH.LIST23709 SH.LIST23137 SH.LIST23654 SH.LIST0500 SH.LIST23195
SH.LIST23196 SH.LIST23040 SH.LIST23376 SH.LIST23192 SH.LIST0775 SH.LIST23978 SH.LIST23180
SH.LIST23140 SH.LIST0786""".split()

fc = pd.read_sql("""
select f.stock_code, b.name, f.change_pct yoy, f.forecast_value/1e8 np_yi, f.change_desc
from fin_forecast f join stock_basic b using(stock_code)
where f.report_date='2026-06-30' and f.forecast_type='归属于上市公司股东的净利润' and f.ann_date>='2026-06-25'
""", CONN).drop_duplicates("stock_code")
fc["turn"] = fc.change_desc.str.contains("扭亏", na=False)
strong = fc[(fc.yoy>=100)|fc.turn].copy()
codes = strong.stock_code.tolist()

# 题材:每只股所属热门题材板(名称列表)
th = pd.read_sql("""select m.stock_code, bd.board_name from board_member m
  join board bd on bd.board_code=m.board_code
  where m.stock_code=any(%s) and m.valid_to is null and m.board_code=any(%s)
""", CONN, params=(codes, HOT))
themes = th.groupby("stock_code").board_name.apply(list).to_dict()

val = pd.read_sql("select stock_code, total_mv/1e8 mv_yi, pe_ttm from daily_valuation "
  "where trade_date=%s and stock_code=any(%s)", CONN, params=(VAL_DATE, codes))
sc = pd.read_sql("""select distinct on (stock_code) stock_code, float_shares from share_capital
  where stock_code=any(%s) order by stock_code, change_date desc""", CONN, params=(codes,))
px = pd.read_sql("select stock_code, trade_date, close from daily_price where stock_code=any(%s) "
  "and trade_date >= %s::date - interval '100 days' order by 1,2", CONN, params=(codes, VAL_DATE))
def logfit(closes):
    y=np.log(np.asarray(closes,float)); x=np.arange(len(y))
    b,a=np.polyfit(x,y,1)
    r2=1-np.sum((y-(a+b*x))**2)/max(np.sum((y-y.mean())**2),1e-12)
    return b, r2
def phase(b60,r2,b20):
    d60=np.exp(b60)-1  # 60日日均涨
    if d60<=0.002: return "未启动"
    if d60>0.015: return "冲高回落" if (np.exp(b20)-1)<-0.003 else "主升加速"
    if d60>=0.002 and r2>=0.6: return "早期爬升"   # 宏和早期型(黄金)
    return "缓升震荡"
r = {}
for c,g in px.groupby("stock_code"):
    g=g.reset_index(drop=True); cl=g.close.astype(float)
    b60=r2=b20=None
    if len(g)>=55: b60,r2=logfit(cl.tail(60))
    if len(g)>=21: b20,_=logfit(cl.tail(21))
    ph = phase(b60,r2,b20) if (b60 is not None and b20 is not None) else "数据不足"
    r[c]=dict(close=float(cl.iloc[-1]),
              r60=float(cl.iloc[-1]/cl.iloc[0]-1) if len(g)>=55 else None,
              r20=float(cl.iloc[-1]/cl.iloc[-21]-1) if len(g)>=21 else None,
              slope60=float(np.exp(b60)-1) if b60 is not None else None,
              r2=float(r2) if r2 is not None else None, phase=ph)

m = strong.merge(val, on="stock_code").merge(sc, on="stock_code", how="left")
m["themes"] = m.stock_code.map(lambda c: themes.get(c, []))
m["nth"] = m.themes.apply(len)
m = m[(m.nth>0) & (m.mv_yi<=500) & (~m.name.str.contains("ST"))].copy()
m["close"] = m.stock_code.map(lambda c: r.get(c,{}).get("close"))
m["r60"] = m.stock_code.map(lambda c: r.get(c,{}).get("r60"))
m["r20"] = m.stock_code.map(lambda c: r.get(c,{}).get("r20"))
m["float_mv"] = (m.float_shares*m.close/1e8).round(1)
m["slope60"] = m.stock_code.map(lambda c: r.get(c,{}).get("slope60"))
m["r2"] = m.stock_code.map(lambda c: r.get(c,{}).get("r2"))
m["phase"] = m.stock_code.map(lambda c: r.get(c,{}).get("phase"))
m["type"] = np.where(m.r60.fillna(0)>0.5, "已启动", "潜伏")
# 排序:题材数↓ + 潜伏优先(近60日↑)
m = m.sort_values(["nth","r60"], ascending=[False, True]).reset_index(drop=True)

pd.set_option("display.width",240,"display.max_rows",80)
show=m.copy(); show["题材"]=show.themes.apply(lambda x: "/".join(x[:3])+("…" if len(x)>3 else ""))
print("宏和参照:全程日斜率+0.94%/R²0.93,近半年+1.64%,最陡+2.41%(已抛物线末段)")
print("阶段分布:", m.phase.value_counts().to_dict())
print(f"候选(强业绩+题材+市值≤500亿+非ST): {len(m)} 只\n")
print(show[["stock_code","name","mv_yi","nth","题材","yoy","np_yi","r20","r60","type"]].round(2).head(45).to_string(index=False))

out=dict(val_date=VAL_DATE, n=len(m),
  rows=json.loads(m[["stock_code","name","mv_yi","float_mv","nth","yoy","np_yi","r20","r60","slope60","r2","phase","type"]].round(4).to_json(orient="records",force_ascii=False)),
  themes={r["stock_code"]: r["themes"] for _,r in m.iterrows()})
path=os.path.join(os.path.dirname(os.path.abspath(__file__)),"theme_data.json")
json.dump(out, open(path,"w"), ensure_ascii=False)
print("saved",path)
