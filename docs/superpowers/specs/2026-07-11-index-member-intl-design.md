# 港美指数成分区间表设计(小包)

日期:2026-07-11;用户已确认。分工:A股全部成分归另一会话,本包仅港美。

## 表(16_schema_index_member.sql)
index_member (index_code VARCHAR(16), stock_code VARCHAR(16), in_date DATE NOT NULL,
              out_date DATE NULL——NULL=在册, PK (index_code, stock_code, in_date))
区间语义同板块层:任意日成分 = in_date<=d AND (out_date IS NULL OR out_date>d)。

## 源(2026-07-11 实探)
- 恒指 HSI=富途 get_plate_stock('HK.800000') 93只 ✅;恒生科技 HK.800700 30只 ✅;
  国企指数 HK.800100 实施探测。历史变更:免费无 → 从启用日起每日 diff 累积,
  历史留白(已知限制,README 声明)。首日 in_date 记为启用日(非真实纳入日)。
- SP500:GitHub datasets 现势 + 历史变更数据集回填(1996起,实施探测具体 repo,
  候选 fja05680/sp500 等);NDX100:现势(Wikipedia/GitHub),历史按所及。
- 美股 stock_code 补 .US 后缀;港股 5 位补零 + .HK。

## ETL(17_index_member_intl.py)
- init:快照建区间(开区间)+ SP500 历史回填(变更数据集→区间重建)
- daily diff:新出现=插开区间(in_date=今日);消失=闭区间(out_date=今日)
- 挂 18:50 富途链尾(cron 行追加 && 调用,不新增条目)
- 幂等:同快照重跑零变化

## 验收
- HSI 在册数 ~93、HSTECH ~30;SP500 在册 ~503 且历史区间抽查(如 TSLA 2020-12-21 纳入);
- diff 幂等;防偷看查询示例实测进 README。

## 不做:A股(另一会话)、恒指历史成分(付费)、指数权重(YAGNI,后续可加 weight 列)。
