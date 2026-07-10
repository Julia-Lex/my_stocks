# 港股/美股基本面数据层设计(三期)

日期:2026-07-10
状态:方案与设计已获用户确认;本文档待用户评审

## 背景与范围

镜像二期 A 股基本面设计(`2026-07-10-ashare-fundamental-design.md`),覆盖港股(hk_stock_basic 全量 ~2,809 只)与美股(us_stock_basic ~550 只)。二期已定的原则直接继承:免费源所及、`report_date ≥ 2015-12-31`、分市场独立表、防未来函数(ann_date + as-of 唯一入口)、宁缺勿假。

**本期裁定(2026-07-10 用户确认):只做报表 + 指标两层**——估值层与股本层免费源短板明显(百度估值仅 ~2.5 年、无股本历史序列),YAGNI 跳过;回测用 hfq 价格 + 指标层 EPS_TTM/ROE 已够构建主流因子,真需要时再补(派生估值方案已评估可行,含币种换算工程,备档待启)。

## 架构(每市场两表两函数)

| 对象 | 说明 |
| --- | --- |
| `hk_fin_statement` / `us_fin_statement` | JSONB 原始报表:`(stock_code, report_date, stmt_type, ann_date, data JSONB)`,PK 同二期。data = `{科目名: 金额}`(东财长表 pivot 而来,保留源中文/英文科目名) |
| `hk_fin_indicator` / `us_fin_indicator` | 指标宽表 ~20 核心列 + **`currency VARCHAR(8)`**(港股财报存在 CNY 计价,如实存不换算)+ `ann_date`,PK `(stock_code, report_date)` |
| `hk_fin_asof` / `hk_fin_asof_all`(us_ 同构) | 与二期 `fin_asof` 同语义:`ann_date IS NOT NULL AND ann_date <= p_date` 的最新报告期 |

指标列(以东财港/美接口实际列为准,实施时定映射):ROE、ROA、毛利率、净利率、EPS、EPS_TTM、BPS、每股经营现金流、营收、营收同比、净利润、净利同比、资产负债率、流动比率、报表币种等。港美列集允许小幅差异(源所及),两市场各自建映射字典。

## 数据源(2026-07-10 已实探)

| 数据 | 接口 | 实测 |
| --- | --- | --- |
| 港股三大报表 | `stock_financial_hk_report_em(stock, symbol=报表名, indicator="报告期")` | 长表(STD_ITEM_NAME/AMOUNT 行),00700 回溯 2001,4,233 行/表 |
| 美股三大报表 | `stock_financial_us_report_em(stock, symbol=报表名, indicator="年报")` | 长表同构;indicator 枚举:年报/单季报/累计——**年报+累计两种都拉**(美股季报科目在"累计"),实施时探测去重策略 |
| 港股指标 | `stock_financial_hk_analysis_indicator_em(symbol, indicator="报告期"/"年度")` | 宽表 36 列(ROE_AVG/CURRENT_RATIO/EPS_TTM/CURRENCY 等),~9 期/调用 |
| 美股指标 | `stock_financial_us_analysis_indicator_em(symbol, indicator="年报"等)` | 宽表 49 列,**含 NOTICE_DATE(公告日)** ✅,TSLA 20 期 |

**公告日(ann_date)现实**:
- 美股:指标接口 NOTICE_DATE 直取 ✅。
- 港股:实施时按序探测——①指标接口全列(实探只展示了部分列,NOTICE_DATE 可能在未展示区);②报表接口的 STD_REPORT_DATE 字段语义;③东财港股 F10 公告日字段。**全部落空则 ann_date 置 NULL**,并在 README 显著声明"港股 as-of 防未来函数不可用,回测请勿使用港股基本面因子或自行确认披露滞后"——宁缺勿假,绝不用报告期估算冒充公告日。
- 报表层 ann_date 从对应指标层回填(同二期模式)。

## ETL

- `11_schema_fundamental_intl.sql`:上述对象,幂等。
- `12_init_fundamental_intl.py --market hk|us [--workers N] [--limit N] [--reset]`:逐股 4 请求(3 报表 + 1 指标),`etl_progress` task=`init_fund_hk` / `init_fund_us`,run_stock_todo(熔断 15)+ 节流。请求预算:港股 ~11K + 美股 ~2.2K(东财 datacenter 接口,已实证白天 3 并发 <1 小时安全;仍分市场错峰、避开 18:00-18:50 cron 窗口)。
- `13_fundamental_update_intl.py --market hk|us`:复用二期 7 天门控(`_hk_fund_check` 哨兵);对"最近 2 个报告期有更新的股票"重拉。cron:`50 18 * * 1-5`(门控使其平日秒退,等效周检)。
- 指标数值溢出保护沿用二期 `_NUM_LIMIT` 分级模式。

## 验收

1. 试跑 20 只 × 2 市场:JSONB pivot 正确(科目数合理)、指标列映射、币种字段分布(CNY 计价港股占比统计)。
2. 数值抽查:00700.HK ROE/毛利率与腾讯公开财报对照;AAPL 或 TSLA 指标与公开数据对照,误差 <2%。
3. as-of 边界用例(美股必做;港股视公告日探测结果)。
4. 全量:港股报表 JSONB 预计 6-8 万报告期行(2,809 只 × ~10 期 × 3 表 ÷ 长表聚合),美股 ~1.5 万;指标各 ~2 万 / ~1 万行。
5. 幂等复跑。

## 不做的事(YAGNI)

- 估值层、股本层(本期裁定,备档待启)。
- 币种换算 / 汇率表(currency 列如实存,消费方自理)。
- 港股公告日的付费源兜底。
- 美股 SEC EDGAR / XBRL 直连(工程量大,东财口径已够)。
