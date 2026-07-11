# C 补全包 Implementation Plan

> **For agentic workers:** 本计划由控制者直写执行(用户 2026-07-11 授权的轻量模式:任务级粒度 + 真实试跑验证 + 最终全分支审查兜底)。Steps use checkbox (`- [ ]`) syntax.

**Goal:** 四表事件数据(业绩预告/快报、龙虎榜、北向持股)+ stock_alias 改码治理,近 10 年 + 每日增量。

**Architecture:** 预告/快报复用二期"按报告期截面循环"模式(公告日随行);龙虎榜按交易日循环;alias 在 common.py 拉取层单点解析(新码拉数、旧码入库)。全部复用 with_retry/etl_progress/熔断。

**Tech Stack:** AKShare(东财)、psycopg2、PostgreSQL。

**Spec:** `docs/superpowers/specs/2026-07-11-events-pack-design.md`

## Global Constraints

- `ASTOCK_DB_USER=zhu`;`.venv/bin/python`;东财走 `with_retry`,≤3 并发;避开 18:00-19:05 cron 窗。
- 历史范围:预告/快报 `report_date ≥ 2015-12-31`;龙虎榜 `trade_date ≥ 2016-01-01`;北向按源所及。
- 主键与列见 spec;数值 NUMERIC;`forecast_value` 允许 NULL(定性预告)。
- alias 语义:**用 new_code 拉数、按 old_code 入库**;不自动改域。
- 每接口先探测完整列名再写映射(已探部分:yjyg 含 公告日期/预测指标/业绩变动/预测数值/业绩变动幅度/业绩变动原因;lhb 前 10 列见 spec)。

---

### Task E1: Schema(`14_schema_events.sql`)
- [ ] 五表 DDL(fin_forecast/fin_express/lhb_detail/nb_hold/stock_alias)+ 截面索引(report_date/trade_date 反向)+ stock_alias 种子行 `('BGNE.US','ONC.US','ONC','2025 改名 BeOne Medicines')`(ON CONFLICT DO NOTHING)
- [ ] psql 应用 + 表/种子验证 + commit "feat: add events-pack schema (forecast/express/lhb/northbound/alias)"

### Task E2: common.py 事件层 + alias 单点
- [ ] 探测:yjkb 完整列、lhb 完整列(含上榜原因列名)、北向三接口(`stock_hsgt_hold_stock_em` 等)现状——北向若无个股历史序列则 nb_hold 缓建(README 记录)
- [ ] `resolve_alias(conn, stock_code) -> (fetch_code, fetch_symbol)`:模块级缓存一次性读 stock_alias(表小);接入 `futu_code` 上游与 12/13 的东财 symbol 推导处(单点:12 的 load 与 13 的 update 取 symbol 前过 alias)
- [ ] `fetch_yjyg(period)` / `fetch_yjkb(period)` / `fetch_lhb(start, end)`:重命名映射 + stock_code 补后缀 + ann_date/trade_date 转 date
- [ ] 验证:单期/单段真实调用断言列齐;alias 断言 `resolve_alias('BGNE.US') == ('ONC.US','ONC')`;commit "feat: add events fetch layer and stock alias resolution"

### Task E3: 14_init_events.py
- [ ] `--part forecast|express|lhb|north|all --reset`;预告/快报 43 期循环(etl_progress 借存 `'YYYYMMDD:yjyg'`/`':yjkb'`);龙虎榜按交易日循环(查 trade_calendar,借存 `'YYYYMMDD:lhb'`,串行 + 0.3s 轻节流);north 按 E2 探测结果实现或打印缓建说明
- [ ] 试跑:forecast/express 各 2 期、lhb 5 天;SQL 核对;幂等复跑
- [ ] 全量:forecast+express(~86 请求,分钟级);lhb 全量后台(~2,550 请求,~30-45 分钟)
- [ ] commit "feat: add events init loader"

### Task E4: 15_events_update.py + README + cron 行
- [ ] 每日:lhb 回看 5 交易日补漏;预告/快报披露季(1/2/4/7/8/10 月)核查最近 2 期、平季 7 天门控(哨兵 `daily_events/_check`);north 日更(若建);alias 周报(连续 2 轮 error 股票汇总 warning)
- [ ] README 新章节(四表用途/预告区间值限制/北向现状/alias 机制)+ cron 行 `0 19 * * 1-5`(示例,实装等验收)
- [ ] 验证跑通 + 幂等;commit "feat: add events daily updater; docs"

### Task E5: 验收 + cron 实装(用户批)+ 合并推送
- [ ] 预告 as-of 领先性:抽 3 股统计 `fin_forecast.ann_date` vs `fin_indicator.ann_date` 领先天数
- [ ] lhb 近 3 日行数对照东财页;十年总量量级核对(预期 20-30 万行)
- [ ] BGNE.US 经 alias 拉到数据并按 BGNE.US 入库(跑 12 --market us 单股验证)
- [ ] 最终全分支审查(fable)→ 修复波(如有)→ 合并推送 → cron 征询安装

## Self-Review
- Spec 覆盖:四表+alias+ETL+验收+YAGNI 全对应;北向缓建路径在 E2/E3 有明确出口。
- 无占位符(探测依赖项均有探测步与落空处理);直写模式下代码细节在执行时落地,计划锚定接口与语义。
- 类型一致:resolve_alias 返回二元组在 E2 定义、E3/12/13 消费;etl_progress 借存键风格与二期一致。
