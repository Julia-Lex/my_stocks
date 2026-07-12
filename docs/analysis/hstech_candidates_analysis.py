# -*- coding: utf-8 -*-
"""恒生科技指数候选盘点(todo#7)。

方法:
- 现役 30 成分(index_member.HSTECH)+ 近一年上市科技新股候选池;
- 腾讯实时接口取该港股类别总市值(亿港元;A+H 公司为 H 股部分市值,与恒生按上市证券类别
  计市值的口径一致,但存在不确定性,见报告"口径"节);
- 恒生排名口径是"过去12个月日均总市值",用 当前市值 × (近12个月hfq均价/最新hfq价) 折算代理
  (hfq=close×复权因子,对拆合股稳健;新股按上市以来全部交易日;忽略股本增发误差);
- 缓冲区:候选升至第 24 名内调入;现成分跌出第 36 名调出(调出风险用成分内垫底名次作代理);
- 9 月季检数据截止 2026-06-30:晚于该日上市的新股本轮无资格(除非上市时触发快速纳入)。
"""
import json
import os
import time

import pandas as pd
import psycopg2
import requests

CONN = psycopg2.connect(dbname="astock", user="zhu")
CUTOFF = "2026-06-30"   # 9月季检数据截止日

CANDIDATES = {
    "02475.HK": "A+H·消费电子", "03986.HK": "A+H·半导体", "02249.HK": "A+H·半导体",
    "02476.HK": "A+H·PCB", "06809.HK": "A+H·半导体", "06166.HK": "A+H·光模块",
    "06951.HK": "A+H·电子元件", "09903.HK": "原生·AI芯片", "06651.HK": "原生·数字孪生",
    "06082.HK": "原生·AI芯片", "06613.HK": "A+H·消费电子", "02631.HK": "A+H·半导体",
    "01688.HK": "A+H·消费电子", "06880.HK": "原生·智驾", "02050.HK": "A+H·汽车零部件",
    "03661.HK": "A+H·半导体", "03200.HK": "A+H·数控设备", "03296.HK": "A+H·ODM",
    "02525.HK": "原生·激光雷达", "02026.HK": "原生·Robotaxi", "00800.HK": "原生·Robotaxi",
    "02432.HK": "原生·机器人", "09678.HK": "原生·AI", "02715.HK": "A+H·机器人",
    "09630.HK": "A+H·半导体设备", "00668.HK": "A+H·消费电子", "06869.HK": "老股·光纤(用户提问)",
}

members = pd.read_sql(
    "select m.stock_code, coalesce(b.name_cn,b.name) nm from index_member m "
    "left join hk_stock_basic b on b.stock_code=m.stock_code "
    "where m.index_code='HSTECH' and m.out_date is null", CONN)
cand = pd.read_sql(
    "select stock_code, coalesce(name_cn,name) nm from hk_stock_basic "
    "where stock_code = any(%s)", CONN, params=(list(CANDIDATES),))
codes = list(members.stock_code) + list(cand.stock_code)

# 近12个月 hfq 均价/最新 hfq 价(市值折算比例,对拆合股稳健)+ 上市日
ratio = pd.read_sql("""
with pf as (
  select p.stock_code, p.trade_date, p.close * coalesce(
    (select a.adj_factor from hk_adj_factor a
     where a.stock_code = p.stock_code and a.trade_date <= p.trade_date
     order by a.trade_date desc limit 1), 1) hfq
  from hk_daily_price p
  where p.stock_code = any(%s) and p.trade_date >= current_date - interval '365 days')
select stock_code, avg(hfq) avg_hfq,
       (array_agg(hfq order by trade_date desc))[1] last_hfq, count(*) n
from pf group by stock_code""", CONN, params=(codes,))
fd = pd.read_sql(
    "select stock_code, min(trade_date) list_d from hk_daily_price "
    "where stock_code = any(%s) group by 1", CONN, params=(codes,))

def fetch_mv(code):
    try:
        r = requests.get("https://qt.gtimg.cn/q=hk" + code.split(".")[0], timeout=5)
        r.encoding = "gbk"
        f = r.text.split("~")
        return float(f[44]) if len(f) > 44 and f[44] else None
    except Exception:
        return None

rows = []
for _, s in pd.concat([members.assign(role="成分"), cand.assign(role="候选")]).iterrows():
    mv = fetch_mv(s["stock_code"])
    time.sleep(0.4)
    rows.append(dict(stock_code=s["stock_code"], name=s["nm"], role=s["role"], mv_yi=mv))

df = pd.DataFrame(rows).merge(ratio, on="stock_code").merge(fd, on="stock_code")
df["typ"] = df.stock_code.map(CANDIDATES).fillna("")
df["avg_mv_yi"] = (df.mv_yi * df.avg_hfq.astype(float) / df.last_hfq.astype(float)).round(1)
df["eligible_sep"] = df.list_d.astype(str) <= CUTOFF
df = df.sort_values("avg_mv_yi", ascending=False).reset_index(drop=True)
df["rank"] = df.index + 1

out = df[["rank", "stock_code", "name", "role", "typ", "mv_yi", "avg_mv_yi",
          "list_d", "n", "eligible_sep"]].copy()
out["mv_yi"] = out.mv_yi.round(1)
out["list_d"] = out.list_d.astype(str)
pd.set_option("display.width", 220, "display.max_rows", 80)
print(out.to_string(index=False))

path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hstech_data.json")
json.dump(dict(rows=out.to_dict("records"), cutoff=CUTOFF),
          open(path, "w"), ensure_ascii=False)
print("saved:", path)
