# A 股板块数据层设计(支持板块轮动分析)

日期:2026-07-10
状态:已与用户对齐(交付范围/板块类型/资金流/成分存法均确认)

## 背景与目标

用户要做板块轮动分析。库内现有板块信息仅 `fin_indicator.industry`
(东财业绩报表"所处行业"副产品,5613 只 / 128 个行业,无板块行情、无成分关系表),
不足以支撑轮动分析。

**交付范围:只建数据层**(表 + 全量初始化 + 每日增量 ETL)。
轮动指标与分析由用户自行用 SQL/Python 完成;本设计不含指标视图与分析脚本。

**纳入**:东财行业板块(~86 个)+ 概念板块(~450 个),含板块指数日线全历史与
板块资金流历史。**不纳入**:地域板块、板块分钟线。

## 数据源(2026-07-10 实探/源码确认)

| 数据 | akshare 接口 | 底层 | 说明 |
|---|---|---|---|
| 行业板块列表 | `stock_board_industry_name_em` | push2 clist | 板块代码 BKxxxx + 名称 |
| 概念板块列表 | `stock_board_concept_name_em` | push2 clist | 同上 |
| 板块日 K | `stock_board_{industry,concept}_hist_em` | push2his kline | 支持 beg/end,行业板块历史约到 2006 |
| 当前成分股 | `stock_board_{industry,concept}_cons_em` | push2 clist | **仅当前快照,无历史成分** |
| 资金流历史 | `stock_{sector,concept}_fund_flow_hist` | push2his fflow/daykline | `lmt=0` 返回全部可用历史;按板块名称查询 |

关键约束:
1. **成分股历史不可回溯**——变迁只能从建库日起每日快照自行积累;
2. 以上全部为东财**行情族(push2/push2his)接口**,与个股日线共享 IP 限流预算
   (2026-07-10 下午实测该族正对本机限流,datacenter 族正常;首次全量需等解封);
3. 板块日 K 成交量源单位为**手**,入库 ×100 统一为**股**(全库约定);
4. hist/资金流接口按**板块名称**查询,板块可能改名——每日更新刷新 `board_name`,
   调用时始终用库里最新名称。

## 表结构(11_schema_board.sql)

### board 板块字典
```sql
CREATE TABLE IF NOT EXISTS board (
    board_code  TEXT PRIMARY KEY,          -- 东财代码,如 BK0475
    board_name  TEXT NOT NULL,             -- 最新名称(改名时更新)
    board_type  TEXT NOT NULL CHECK (board_type IN ('industry', 'concept')),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,  -- 从列表消失置 false,历史数据保留
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### board_member 成分区间表(方案 A,用户已选定)
```sql
CREATE TABLE IF NOT EXISTS board_member (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    stock_code  TEXT NOT NULL,
    valid_from  DATE NOT NULL,             -- 观测到纳入的日期(见语义说明)
    valid_to    DATE,                      -- NULL = 当前仍在;观测到移出当天关区间
    PRIMARY KEY (board_code, stock_code, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_board_member_stock ON board_member (stock_code);
CREATE INDEX IF NOT EXISTS idx_board_member_open  ON board_member (board_code) WHERE valid_to IS NULL;
```
**语义**:`valid_from` 是**观测起点**,不是真实纳入日——首次建库当天,全部存量成分的
`valid_from` = 建库日;此后精度 = 每日快照粒度(周末/停跑期间的变动会归到下一次观测日)。
"某日 d 某股属于哪些板块":`valid_from <= d AND (valid_to IS NULL OR valid_to > d)`。

落选方案:B 每日全量快照(一年 ~700 万冗余行,as-of 查询绕);C 只存当前(回测缺腿)。

### board_daily 板块指数日线
```sql
CREATE TABLE IF NOT EXISTS board_daily (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    trade_date  DATE NOT NULL,
    open NUMERIC(12,3), high NUMERIC(12,3), low NUMERIC(12,3), close NUMERIC(12,3),
    volume BIGINT,                          -- 股(源为手,入库 ×100)
    amount NUMERIC(20,2),                   -- 元
    pct_chg NUMERIC(8,4), turnover NUMERIC(8,4),
    PRIMARY KEY (board_code, trade_date)
);
```
约 536 板块 × ~2500 日 ≈ 130 万行,不分区。

### board_fund_flow 板块资金流
```sql
CREATE TABLE IF NOT EXISTS board_fund_flow (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    trade_date  DATE NOT NULL,
    main_net   NUMERIC(20,2), main_net_pct   NUMERIC(8,4),   -- 主力净流入 额(元)/占比(%)
    xlarge_net NUMERIC(20,2), xlarge_net_pct NUMERIC(8,4),   -- 超大单
    large_net  NUMERIC(20,2), large_net_pct  NUMERIC(8,4),
    mid_net    NUMERIC(20,2), mid_net_pct    NUMERIC(8,4),
    small_net  NUMERIC(20,2), small_net_pct  NUMERIC(8,4),
    PRIMARY KEY (board_code, trade_date)
);
```

## ETL 设计

公共 fetch 层加在 `common.py`(与现有基本面 fetch 同区):板块列表 / 板块日 K /
当前成分 / 资金流历史四个函数,列名映射集中在顶部 RENAME 常量,沿用 `with_retry`。

### 12_init_board.py 一次性全量(断点续传)
1. 板块列表(行业+概念)→ upsert `board`;
2. 逐板块日 K 全历史(beg=19900101)→ `board_daily`,**挂 `drop_unclosed_bars`**,volume ×100;
3. 逐板块资金流 `lmt=0` → `board_fund_flow`,同样按 cutoff 过滤 trade_date;
4. 当前成分 → `board_member` 开区间(valid_from=当日,valid_to=NULL)。
- 断点续传:`etl_progress` task=`init_board`,stock_code 字段借存 board_code
  (惯例同 `init_fund_cross`);
- 并发 ≤3,`run_stock_todo` + 熔断器(max_consecutive_errors=15);
- 请求量 ≈ 536 × 3 ≈ 1600 次,3 并发 15–20 分钟;**须在 push2 限流解除后运行**。

### 13_board_update.py 每日增量(cron 18:10,排在 03 日线 18:00 之后)
1. 刷新板块列表:新板块 → 插入 `board` 并补拉全历史;改名 → 更新 `board_name`;
   从列表消失 → `is_active=false`(不删数据);
2. 日 K 增量:各板块自 `max(trade_date)+1` 拉到今天,upsert 幂等;
3. 资金流:`lmt=0` 全拉 upsert 覆盖(量小,幂等覆盖比算增量省心);
4. 成分 diff(仅 is_active 板块):当前快照 vs 开区间——新出现开区间(valid_from=今天)、
   消失关区间(valid_to=今天);**成分接口失败或返回空时跳过该板块 diff**
   (宁可当天不更新,不能把接口故障误判为"全员移出")。

### 错误处理
- 单板块失败记 `etl_progress` error,不中断整体,下次续传补;
- 连续失败熔断(疑似限流),停止派发、留待冷却后续传;
- 日 K 与资金流独立计错(一个接口挂不拖累另一个);
- 收盘防护沿用加固后的 `safe_cutoff_date`(双时钟取保守值,commit 3f0e642)。

## 验证(初始化后执行)

1. 行数/覆盖:`board` ≈ 536(行业 ~86 + 概念 ~450);`board_daily` 各板块
   min/max trade_date 合理(行业板块最早 ~2006);`board_member` 开区间总数 ≈ 3~4 万;
2. 交叉校验:抽 3–5 个板块,当日成分股涨跌幅(流通市值加权,取 `daily_valuation`)
   与 `board_daily.pct_chg` 同向且量级一致(容差放宽到 ±1pp,加权口径与东财不完全相同);
3. 资金流自洽:抽查 主力净额 = 超大单+大单 净额之和;主力+中单+小单 ≈ 0;
4. 无未来日期:`board_daily`/`board_fund_flow` 均无 trade_date > 收盘防护 cutoff 的行。

## 明确不做(YAGNI)

- 地域板块、板块分钟线、板块估值;
- 轮动指标物化视图与分析脚本(用户自行分析;将来需要另立项目);
- 申万/中信等其他板块分类体系;
- 成分股权重(东财不提供板块内权重,交叉校验用流通市值近似)。
