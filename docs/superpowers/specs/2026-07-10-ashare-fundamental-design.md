# A 股基本面数据层设计(二期 · 子项目 1/3)

日期:2026-07-10
状态:方案与设计已获用户确认;本文档待用户评审

## 背景与范围

一期已完成三市场日线 + 分钟线。二期为**基本面数据层**,用户确认:

- **用途(三者都要)**:量化因子回测(严格时点对齐)、个股基本面研究(全科目)、选股筛选(最新截面)。
- **市场:三市场都做,按子项目串行**——本 spec 只覆盖 **A 股**;港股、美股各自独立 spec(表结构镜像本设计,数据源不同,免费源仅主要科目)。
- **历史深度:近 10 年**(`report_date ≥ 2015-12-31`,留缓冲)。

## 设计核心:防未来函数

所有含财务信息的表必须带**公告日 `ann_date`**(数据何时公之于众)与**报告期 `report_date`**(数据描述哪个期间)。回测取数唯一入口是 **as-of 语义**:给定交易日 t,只能看到 `ann_date ≤ t` 的最新报告期。

已知诚实限制(写死在这里,防止未来误解):

- 免费源只有财报**最新值**,无修订历史。`ann_date` 防"提前看",防不了"事后修正"(真 PIT 修订史是付费数据)。
- 乐咕估值为全市场统一口径,个别股票与东财口径有小差异。

## 四层架构

| 层 | 表 | 形态 | 服务于 |
| --- | --- | --- | --- |
| 原始报表 | `fin_statement` | JSONB 长表 | 个股研究(全科目) |
| 指标 | `fin_indicator` | 精选宽表 ~30 列 | 因子回测(主力) |
| 股本 | `share_capital` | 变动时间序列 | 市值/换手率计算 |
| 日频估值 | `daily_valuation` | 年分区行情式表 | 筛选 + 估值因子 |

**指标层不另找数据源,从原始报表层派生计算**(SQL/Python),`ann_date` 自动继承——单一事实源,杜绝两源对齐问题。

## Schema(新文件 `08_schema_fundamental.sql`)

```sql
-- 原始报表:三大报表全科目,JSONB 免疫科目名漂移
fin_statement (
    stock_code  VARCHAR(12) NOT NULL,
    report_date DATE        NOT NULL,   -- 报告期(季末日)
    stmt_type   VARCHAR(8)  NOT NULL,   -- income / balance / cashflow
    ann_date    DATE,                   -- 公告日(东财 NOTICE_DATE;NULL=源未给)
    data        JSONB       NOT NULL,   -- 全科目原始键值
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date, stmt_type)
)
-- 索引:(report_date, stock_code) 支持截面;(ann_date) 支持 as-of

-- 指标宽表:从 fin_statement 派生
fin_indicator (
    stock_code, report_date, ann_date,
    -- 盈利能力:roe, roa, gross_margin, net_margin,
    -- 每股:eps, bps, ocf_ps,
    -- 成长(同比):revenue_yoy, net_profit_yoy,
    -- 质量:debt_ratio, current_ratio, ocf_to_profit,
    -- 规模:revenue, net_profit, total_assets, total_equity ...(~30 列,NUMERIC)
    PRIMARY KEY (stock_code, report_date)
)

-- 股本变动
share_capital (
    stock_code, change_date, total_shares BIGINT, float_shares BIGINT,
    PRIMARY KEY (stock_code, change_date)
)

-- 日频估值(乐咕):按年分区 2015~2030,结构同行情表模式
daily_valuation (
    stock_code, trade_date, pe, pe_ttm, pb, ps, ps_ttm,
    dv_ratio, dv_ttm, total_mv,
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY RANGE (trade_date)

-- as-of 取数函数:回测唯一入口
fin_asof(p_stock VARCHAR, p_date DATE) RETURNS SETOF fin_indicator
  -- 返回该股 ann_date <= p_date 的最新 report_date 那一行
-- 另提供截面版视图/函数:fin_asof_all(p_date) —— 全市场某日可见最新指标
```

## 数据源(A 股)· 2026-07-10 实探修订

> 原定东财 by-report 全科目接口实测为**每股每表分页 21 次请求**(5,500 股 × 3 表 ≈ 35 万请求,爆预算),弃用。改为"截面 + 新浪全科目"组合,总预算降至 ~2.5 万请求:

| 数据 | 接口 | 请求量 | 说明 |
| --- | --- | --- | --- |
| 指标骨干 + **公告日** | 东财按报告期截面:`stock_yjbb_em(date)`(EPS/营收及同比/净利及同比/BPS/ROE/每股现金流/毛利率/**最新公告日期**)+ `stock_lrb_em` / `stock_zcfz_em` / `stock_xjll_em`(三表主要科目,含**公告日期**) | 40 期 × 4 接口 × ~12 页 ≈ 2,000(东财) | 一次调用拿全市场一个报告期,ann_date 的唯一权威来源 |
| 三大报表全科目 | 新浪 `stock_financial_report_sina(stock="sh600519", symbol="资产负债表"/"利润表"/"现金流量表")` | 3 × 5,500 ≈ 16,500(新浪) | 每股每表 1 请求返回全部历史期 × ~150 列;**无公告日**,ann_date 由截面层回填 |
| 股本变动 | 东财 `stock_zh_a_gbjg_em(symbol)` | ~5,500(东财) | 历史股本结构变动 |
| 日频估值 | 东财 `stock_value_em`(实施时探测签名;若为每股全历史则 ~5,500 请求) | ~5,500(东财) | PE/PB/PS/总市值等 |

指标层构成 = 截面接口直取(自带 ann_date)+ 由 fin_statement JSONB 派生补充(资产负债率、流动比率、现金含量等比率类)。

## ETL

- `09_init_fundamental.py [--workers N] [--limit N] [--reset]`:逐股拉三大报表(过滤 report_date ≥ 2015-12-31)+ 股本 + 估值,随后批量派生指标层。`etl_progress` task=`init_fund`;复用 `common.run_stock_todo`(熔断 15、节流、断点续传全继承)。
- `10_fundamental_update.py`:每日 cron。估值增量每日;财报按自适应节奏——披露季(1/2/4/7/8/10 月)每日核查"有无新公告的报告期",平季每周一核查一次;新报表入库后重算该股指标行。
- 请求预算:5,500 只 × ~5 请求 ≈ 2.75 万,东财预算下分段跑(≤3 并发 + 中途冷却),预计 3~4 小时。一期的封禁教训与基建直接适用。

## 验收

1. `--limit 20` 试跑:三表 JSONB 入库、NOTICE_DATE 解析为 ann_date、指标派生数值抽查(手算 ROE 对照)。
2. as-of 正确性:构造用例——某股 Q3 报告 10-25 公告,则 `fin_asof(股, '2025-10-24')` 必须返回 Q2 行,`'2025-10-26'` 返回 Q3 行。
3. 全量后:fin_statement 行数 ≈ 5,500 股 × ~40 期 × 3 表(±退市/新股);daily_valuation ≈ 1,300 万行。
4. 幂等:重跑增量行数不变。

## 不做的事(YAGNI)

- 不做财报修订历史(付费数据,免费源无)。
- 不做业绩预告/快报、分红送配明细表(可作三期)。
- 本 spec 不含港股/美股(各自后续 spec)。
- 不做自算 PE/PB 引擎(乐咕现成;自算能力由"股本层+报表层"天然保留,需要时再加)。
