# C 补全包设计:业绩预告/快报、龙虎榜、北向、股票域治理

日期:2026-07-11
状态:设计已获用户确认;本文档待用户评审

## 背景与范围

四项小型补全(用户 2026-07-11 确认全做),复用既有 ETL 基建(with_retry / 截面循环模式 /
etl_progress 断点续传 / 熔断)。A 股事件类数据为主 + 一项纯代码治理。

2026-07-11 已实探:`stock_yjyg_em(date)` 业绩预告截面(715 行/期,含**公告日期**)、
`stock_yjkb_em(date)` 业绩快报截面(69 行/期,含公告日期)、`stock_lhb_detail_em(start,end)`
龙虎榜明细(3 天 296 行)——三者全绿。北向系接口存在但 2024 披露改制后能力待实施探测。

## Schema(`14_schema_events.sql`)

| 表 | 主键 | 列要点 |
| --- | --- | --- |
| `fin_forecast` | `(stock_code, report_date, forecast_type)` | `ann_date`、业绩变动、预测数值、变动幅度、变动原因;同期可多预测指标行 |
| `fin_express` | `(stock_code, report_date)` | `ann_date` + 快报数值列(营收/净利/EPS/BPS/ROE 等,按接口实际列映射) |
| `lhb_detail` | `(stock_code, trade_date, reason)` | 上榜原因(同日同股可多榜)、收盘价/涨跌幅、龙虎榜净买额/买入额/卖出额 |
| `nb_hold` | `(stock_code, trade_date)` | 北向持股(股数/市值);**列按实施探测所及定**,若免费源已无个股级序列则本表缓建并在 README 记录 |

- 预告/快报带 `ann_date` 直接参与防未来查询(`WHERE ann_date <= d`);表小,不建独立 as-of 函数(YAGNI)。
- 数值 NUMERIC;历史范围:预告/快报 `report_date ≥ 2015-12-31`(与基本面一致);龙虎榜自 2016-01-01;北向按源所及。

## 股票域治理(纯代码)

- 新表 `stock_alias (old_code PK, new_code, new_symbol, note, updated_at)`;种子行:`BGNE.US → ONC(2025 改名 BeOne)`。
- 港美拉取层(富途/东财调用前)查 alias:**用新码拉数、按旧码入库**(保历史连续性)。改动点:`futu_code` 上游加一次 alias 解析(common.py,单点)。
- 检测:15 脚本每周把"连续 2 轮 error 的股票"汇总打 log.warning(人工处置,不自动改域)。

## ETL

- `14_init_events.py [--part forecast|express|lhb|north|all] [--reset]`:
  - 预告/快报:43 期 × 2 接口循环截面(~86 请求,东财),`etl_progress` 借存 `'YYYYMMDD:yjyg'` 模式(同二期阶段1);
  - 龙虎榜:按日循环 2016-01-01→今(仅交易日,查 `trade_calendar`,~2,550 请求,≤3 并发或串行+轻节流);按 `'YYYYMMDD:lhb'` 断点续传;
  - 北向:先探测(`stock_hsgt_hold_stock_em` 等三接口),按所及实现或缓建。
- `15_events_update.py`:每日——龙虎榜补昨日(含 5 日回看补漏);预告/快报在披露季(1/2/4/7/8/10 月)每日核查最近 2 期、平季 7 天门控;北向日更(若建);alias 错误周报。
- cron:`0 19 * * 1-5`(在 18:50 链之后)。

## 验收

1. 预告 as-of 价值验证:抽 3 只股,`fin_forecast.ann_date` 应显著早于 `fin_indicator.ann_date`(同报告期),记录领先天数分布。
2. 龙虎榜:近 3 日行数与东财页面对照;十年总量 ~20-30 万行量级核对。
3. 幂等复跑;`BGNE.US` 经 alias 拉到 ONC 数据且按 BGNE.US 入库。
4. 已知限制写 README:预告数值是区间/定性混合(forecast_value 可能 NULL)、北向按源现状、龙虎榜披露规则历史有变(2017 修订)。

## 不做的事(YAGNI)

- 大宗交易、融资融券(未列入本包);龙虎榜席位明细(机构/营业部逐席)——先做榜单主表,席位表按需二期;北向盘中分钟级(免费源已停)。
