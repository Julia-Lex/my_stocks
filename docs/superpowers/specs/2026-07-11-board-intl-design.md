# 港美板块层设计(小包,富途源)

日期:2026-07-11;用户裁定:**每市场一张表**(hk_board*/us_board*,与方案B哲学完全一致)。
分工:A股板块(board_*, 东财)=另一会话;本包=港美(富途 get_plate_list/get_plate_stock)。

## 表(18_schema_board_intl.sql)
hk_board (board_code VARCHAR(24) PK, board_name, board_type industry|concept, updated_at)
hk_board_member (board_code, stock_code, in_date, out_date NULL=在册, note,
                 PK (board_code, stock_code, in_date))   -- 区间语义同 index_member
us_ 同构镜像。

## 源与限制
- 富途 Plate.INDUSTRY / Plate.CONCEPT(US 概念板块存在性实施探测,无则只做 industry);
  每板块 get_plate_stock 成分快照 → 每日 diff 累积;板块历史成分免费不可得,
  note='snapshot-open' 行 in_date=建档日(README 声明)。
- v1 不做板块日线/资金流(富途历史K线走行情额度;需求出现再评估)。
- 预算:~200-400 板块 × 1 请求,1.05s 节流下 ~4-8 分钟/日。

## ETL(19_board_intl.py)
--init 建档 + 每日 diff;挂 18:50 富途链尾(17 之后)。板块更名/消失:board upsert 覆盖名,
消失板块其成员全部闭区间。

## 验收
腾讯(00700.HK)所属板块 ≥1 且含行业类;港美板块数量级(HK ~90 行业);幂等零变化;
反查示例("港股XX行业成分")实测进 README。
