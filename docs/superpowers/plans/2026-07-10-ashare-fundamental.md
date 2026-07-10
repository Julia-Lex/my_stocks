# A 股基本面数据层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 astock 库新增 A 股基本面四层(JSONB 全科目报表 / 指标宽表 / 股本变动 / 日频估值),全表带 `ann_date`,配 `fin_asof` 防未来函数取数入口,近 10 年历史 + 每日增量。

**Architecture:** 东财按报告期截面接口供指标骨干与公告日(全市场一期一调),新浪按股供全科目 JSONB,比率类指标由 JSONB 派生;复用一期 `common.py` 全部基建(with_retry / run_stock_todo 熔断节流 / etl_progress 断点续传 / upsert)。

**Tech Stack:** Python 3.12(`.venv`)、AKShare、psycopg2、PostgreSQL 14(JSONB)。

**Spec:** `docs/superpowers/specs/2026-07-10-ashare-fundamental-design.md`(含 2026-07-10 数据源实探修订)

## Global Constraints

- 数据库连接带 `ASTOCK_DB_USER=zhu`;Python 用 `.venv/bin/python`。
- 历史范围:`report_date >= '2015-12-31'`。
- 所有含财务信息的表必须有 `ann_date DATE`(可 NULL,源未给时);回测入口只经 `fin_asof`。
- 请求预算纪律:东财 ≤3 并发、新浪逐股 workers ≤3;全量执行须与 18:00-18:35 的 cron 窗口错峰;熔断参数 `max_consecutive_errors=15` 必传。
- 接口列名不可信:每个新接口先探测(单请求)再写映射,映射集中在 `common.py` RENAME_* 区。
- 金额列 NUMERIC;JSONB 里保留源原始键名(中文),不做翻译。
- 已知限制照抄 spec:无修订历史;`ann_date` 防"提前看"不防"事后修正"。

## File Structure

| 文件 | 动作 | 职责 |
| --- | --- | --- |
| `08_schema_fundamental.sql` | Create | 四层表 + 索引 + `fin_asof` / `fin_asof_all` 函数 |
| `common.py` | Modify | 基本面拉取层(截面/新浪报表/股本/估值)+ 列名映射 + upsert 帮手 |
| `09_init_fundamental.py` | Create | 四阶段全量初始化 |
| `10_fundamental_update.py` | Create | 每日增量(估值日更 + 披露季自适应财报核查) |
| `README.md` | Modify | 二期章节 + cron 行 |

---

### Task 1: 基本面 Schema

**Files:**
- Create: `08_schema_fundamental.sql`

**Interfaces:**
- Produces: 表 `fin_statement` / `fin_indicator` / `share_capital` / `daily_valuation`(年分区 2015..2030);函数 `fin_asof(p_stock VARCHAR, p_date DATE) RETURNS SETOF fin_indicator`、`fin_asof_all(p_date DATE) RETURNS SETOF fin_indicator`。

- [ ] **Step 1: 写 schema 文件(完整内容)**

```sql
-- =============================================================================
-- A股基本面数据层(二期)· 设计见 docs/superpowers/specs/2026-07-10-ashare-fundamental-design.md
-- 核心原则:全表带 ann_date(公告日);回测取数唯一入口是 fin_asof(防未来函数)。
-- 已知限制:免费源无财报修订历史,ann_date 防"提前看"不防"事后修正"。
-- 用法: psql -d astock -f 08_schema_fundamental.sql
-- =============================================================================

-- 原始报表(全科目,新浪源):JSONB 免疫科目名漂移,键为源中文科目名
CREATE TABLE IF NOT EXISTS fin_statement (
    stock_code  VARCHAR(12) NOT NULL,
    report_date DATE        NOT NULL,              -- 报告期(季末日)
    stmt_type   VARCHAR(8)  NOT NULL,              -- income / balance / cashflow
    ann_date    DATE,                              -- 公告日(截面层回填;NULL=未匹配到)
    data        JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date, stmt_type)
);
CREATE INDEX IF NOT EXISTS idx_fin_statement_period ON fin_statement (report_date, stock_code);

-- 指标宽表(截面接口直取 + JSONB 派生),回测主力
CREATE TABLE IF NOT EXISTS fin_indicator (
    stock_code      VARCHAR(12)  NOT NULL,
    report_date     DATE         NOT NULL,
    ann_date        DATE,                          -- 东财"最新公告日期"
    -- 截面直取(stock_yjbb_em)
    eps             NUMERIC(12,4),                 -- 每股收益(元)
    bps             NUMERIC(12,4),                 -- 每股净资产(元)
    ocf_ps          NUMERIC(12,4),                 -- 每股经营现金流(元)
    roe             NUMERIC(10,4),                 -- 净资产收益率 %
    gross_margin    NUMERIC(10,4),                 -- 销售毛利率 %
    revenue         NUMERIC(20,2),                 -- 营业总收入(元)
    revenue_yoy     NUMERIC(10,4),                 -- 营收同比 %
    net_profit      NUMERIC(20,2),                 -- 净利润(元)
    net_profit_yoy  NUMERIC(10,4),                 -- 净利同比 %
    industry        VARCHAR(32),                   -- 所处行业(东财口径)
    -- 截面直取(zcfz/xjll)
    total_assets    NUMERIC(20,2),
    total_liab      NUMERIC(20,2),
    total_equity    NUMERIC(20,2),                 -- 股东权益合计
    ocf             NUMERIC(20,2),                 -- 经营现金流净额
    -- JSONB 派生(Task 4 计算)
    net_margin      NUMERIC(10,4),                 -- 净利率 % = 净利润/营业总收入*100
    roa             NUMERIC(10,4),                 -- 总资产收益率 % = 净利润/总资产*100
    debt_ratio      NUMERIC(10,4),                 -- 资产负债率 % = 总负债/总资产*100
    current_ratio   NUMERIC(10,4),                 -- 流动比率 = 流动资产合计/流动负债合计
    ocf_to_profit   NUMERIC(10,4),                 -- 现金含量 = 经营现金流净额/净利润
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date)
);
CREATE INDEX IF NOT EXISTS idx_fin_indicator_period ON fin_indicator (report_date, stock_code);
CREATE INDEX IF NOT EXISTS idx_fin_indicator_ann ON fin_indicator (stock_code, ann_date);

-- 股本变动
CREATE TABLE IF NOT EXISTS share_capital (
    stock_code   VARCHAR(12) NOT NULL,
    change_date  DATE        NOT NULL,
    total_shares BIGINT,                           -- 总股本(股)
    float_shares BIGINT,                           -- 流通股本(股)
    reason       VARCHAR(64),                      -- 变动原因(源给则存)
    PRIMARY KEY (stock_code, change_date)
);

-- 日频估值(东财)· 年分区
CREATE TABLE IF NOT EXISTS daily_valuation (
    stock_code VARCHAR(12)  NOT NULL,
    trade_date DATE         NOT NULL,
    pe         NUMERIC(12,4),
    pe_ttm     NUMERIC(12,4),
    pb         NUMERIC(12,4),
    ps         NUMERIC(12,4),
    ps_ttm     NUMERIC(12,4),
    dv_ratio   NUMERIC(10,4),                      -- 股息率 %
    total_mv   NUMERIC(20,2),                      -- 总市值(元)
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY RANGE (trade_date);
CREATE INDEX IF NOT EXISTS idx_daily_valuation_date ON daily_valuation (trade_date, stock_code);
DO $$
DECLARE y INT;
BEGIN
    FOR y IN 2015..2030 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS daily_valuation_%s '
            'PARTITION OF daily_valuation FOR VALUES FROM (%L) TO (%L)',
            y, format('%s-01-01', y), format('%s-01-01', y + 1));
    END LOOP;
END $$;

-- =============================================================================
-- as-of 取数(防未来函数唯一入口):给定交易日,只见 ann_date <= 该日的最新报告期
-- ann_date IS NULL 的行视为不可见(宁缺勿偷看)
-- =============================================================================
CREATE OR REPLACE FUNCTION fin_asof(p_stock VARCHAR, p_date DATE)
RETURNS SETOF fin_indicator AS $$
    SELECT * FROM fin_indicator
    WHERE stock_code = p_stock AND ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY report_date DESC
    LIMIT 1;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION fin_asof_all(p_date DATE)
RETURNS SETOF fin_indicator AS $$
    SELECT DISTINCT ON (stock_code) *
    FROM fin_indicator
    WHERE ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY stock_code, report_date DESC;
$$ LANGUAGE sql STABLE;
```

- [ ] **Step 2: 应用并验证**

Run:
```bash
psql -d astock -f 08_schema_fundamental.sql
psql -d astock -tAc "select count(*) from pg_class where relname like 'daily_valuation_%' and relkind='r'"
psql -d astock -tAc "select proname from pg_proc where proname in ('fin_asof','fin_asof_all') order by 1"
```
Expected: 无 ERROR;`16`;两个函数名。

- [ ] **Step 3: fin_asof 语义单测(用手工插入的假数据,测完删)**

Run:
```bash
psql -d astock <<'SQL'
INSERT INTO fin_indicator (stock_code, report_date, ann_date, eps) VALUES
 ('TEST01.SZ','2025-06-30','2025-08-20',1.0),
 ('TEST01.SZ','2025-09-30','2025-10-25',2.0),
 ('TEST01.SZ','2025-12-31',NULL,3.0);
SELECT 'D-1', eps FROM fin_asof('TEST01.SZ','2025-10-24');
SELECT 'D0',  eps FROM fin_asof('TEST01.SZ','2025-10-25');
SELECT 'NULL不可见', count(*) FROM fin_asof('TEST01.SZ','2026-07-01') WHERE eps=3.0;
DELETE FROM fin_indicator WHERE stock_code='TEST01.SZ';
SQL
```
Expected: `D-1 → 1.0`(公告前一日只见 Q2)、`D0 → 2.0`(公告当日可见 Q3)、`NULL不可见 → 0`。

- [ ] **Step 4: Commit**

```bash
git add 08_schema_fundamental.sql
git commit -m "feat: add A-share fundamental schema (4 layers + fin_asof)"
```

---

### Task 2: common.py 基本面拉取层

**Files:**
- Modify: `common.py`(末尾追加"基本面(二期)"区)

**Interfaces:**
- Consumes: 既有 `with_retry` / `log` / `upsert`。
- Produces(供 Task 3/5 调用,签名固定):
  - `FUND_START = date(2015, 12, 31)`(模块常量)
  - `quarter_ends(start: date, end: date) -> list[date]`(纯函数:两日期间全部季末日)
  - `fetch_fin_cross(kind: str, period: str) -> pd.DataFrame`——kind ∈ `yjbb|lrb|zcfz|xjll`,period 形如 `"20250331"`;返回已重命名列(映射字典 `RENAME_YJBB` 等),含 `stock_code`(补后缀)与 `ann_date`
  - `fetch_fin_report_sina(symbol: str, stmt_type: str) -> pd.DataFrame`——stmt_type ∈ `income|balance|cashflow`;返回列 `report_date` + 全科目原始列(不重命名,供 JSONB)
  - `fetch_share_structure(symbol: str) -> pd.DataFrame`——列 `change_date, total_shares, float_shares, reason`(股本单位:股)
  - `fetch_valuation(symbol: str) -> pd.DataFrame`——列 `trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, total_mv`
  - `upsert_jsonb_statement(conn, stock_code, stmt_type, df) -> int`(内部把行转 `{列:值}` JSON,报告期过滤 ≥ FUND_START)

- [ ] **Step 1: 探测未定接口(股本 + 估值;截面/新浪已实探过)**

Run:
```bash
ASTOCK_DB_USER=zhu .venv/bin/python - <<'EOF'
import akshare as ak
df = ak.stock_zh_a_gbjg_em(symbol="600519")   # 若签名报错依次试 "SH600519"/"sh600519"
print("股本:", len(df), list(df.columns))
import inspect
print("stock_value_em 签名:", inspect.signature(ak.stock_value_em))
df2 = ak.stock_value_em(symbol="600519")       # 按实际签名调整
print("估值:", len(df2), list(df2.columns)[:12])
EOF
```
Expected: 两接口各返回非空 DataFrame。**把实际列名写进 Step 2 的映射字典**;若 `stock_value_em` 是按日截面而非按股历史,估值层改为"按交易日循环截面拉取"并同步改 Task 3 阶段 3 的循环维度(在报告中说明)。

- [ ] **Step 2: 写拉取层代码**

`common.py` 末尾追加(列名映射以 Step 1 与下方 2026-07-10 实探为准):

```python
# ===========================================================================
# 基本面(二期)。设计: docs/superpowers/specs/2026-07-10-ashare-fundamental-design.md
# 截面接口(东财,含公告日)供指标骨干;新浪按股供全科目 JSONB。
# ===========================================================================
from datetime import date as _date

FUND_START = _date(2015, 12, 31)

# stock_yjbb_em 实探列(2026-07-10):序号/股票代码/股票简称/每股收益/营业总收入-营业总收入/
# 营业总收入-同比增长/营业总收入-季度环比增长/净利润-净利润/净利润-同比增长/净利润-季度环比增长/
# 每股净资产/净资产收益率/每股经营现金流量/销售毛利率/所处行业/最新公告日期
RENAME_YJBB = {
    "股票代码": "symbol", "每股收益": "eps", "营业总收入-营业总收入": "revenue",
    "营业总收入-同比增长": "revenue_yoy", "净利润-净利润": "net_profit",
    "净利润-同比增长": "net_profit_yoy", "每股净资产": "bps",
    "净资产收益率": "roe", "每股经营现金流量": "ocf_ps", "销售毛利率": "gross_margin",
    "所处行业": "industry", "最新公告日期": "ann_date",
}
# lrb/zcfz/xjll 截面列在实施时按实际响应补映射(公告日期 -> ann_date,
# 资产总计 -> total_assets, 负债合计 -> total_liab, 股东权益合计 -> total_equity,
# 经营性现金流量净额 -> ocf;其余列丢弃)
RENAME_LRB, RENAME_ZCFZ, RENAME_XJLL = { ... }, { ... }, { ... }  # Step 1/首跑时填实

_CROSS_FN = {"yjbb": "stock_yjbb_em", "lrb": "stock_lrb_em",
             "zcfz": "stock_zcfz_em", "xjll": "stock_xjll_em"}
_CROSS_RENAME = {"yjbb": RENAME_YJBB, "lrb": RENAME_LRB,
                 "zcfz": RENAME_ZCFZ, "xjll": RENAME_XJLL}


def quarter_ends(start: _date, end: _date) -> list[_date]:
    """start~end 间全部季末日(3/31, 6/30, 9/30, 12/31),含端点。"""
    out, y = [], start.year
    while y <= end.year:
        for m, d in ((3, 31), (6, 30), (9, 30), (12, 31)):
            q = _date(y, m, d)
            if start <= q <= end:
                out.append(q)
        y += 1
    return out


def fetch_fin_cross(kind: str, period: str) -> pd.DataFrame:
    """东财按报告期截面。period 'YYYYMMDD'(季末日)。返回含 stock_code/ann_date 的重命名帧。"""
    import akshare as ak

    fn = getattr(ak, _CROSS_FN[kind])
    df = with_retry(fn, date=period)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=_CROSS_RENAME[kind])
    keep = [c for c in set(_CROSS_RENAME[kind].values()) if c in df.columns]
    df = df[keep].copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["stock_code"] = df["symbol"].map(to_full_code)
    if "ann_date" in df.columns:
        df["ann_date"] = pd.to_datetime(df["ann_date"], errors="coerce").dt.date
    return df


_SINA_STMT = {"balance": "资产负债表", "income": "利润表", "cashflow": "现金流量表"}


def fetch_fin_report_sina(symbol: str, stmt_type: str) -> pd.DataFrame:
    """新浪全科目报表(单请求全历史)。返回 report_date + 原始中文科目列。"""
    import akshare as ak

    df = with_retry(ak.stock_financial_report_sina,
                    stock=to_sina_code(symbol), symbol=_SINA_STMT[stmt_type])
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"报告日": "report_date"})
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    return df.dropna(subset=["report_date"])


def upsert_jsonb_statement(conn, stock_code: str, stmt_type: str, df: pd.DataFrame) -> int:
    """新浪报表帧 → fin_statement JSONB(过滤 report_date >= FUND_START;NaN 键剔除)。"""
    import json

    if df.empty:
        return 0
    df = df[df["report_date"] >= FUND_START]
    rows = []
    for _, r in df.iterrows():
        payload = {k: (None if pd.isna(v) else (float(v) if isinstance(v, (int, float)) else str(v)))
                   for k, v in r.items() if k != "report_date" and not pd.isna(v)}
        rows.append((stock_code, r["report_date"], stmt_type, json.dumps(payload, ensure_ascii=False)))
    return upsert(conn, "fin_statement",
                  ["stock_code", "report_date", "stmt_type", "data"],
                  rows, ["stock_code", "report_date", "stmt_type"], update_cols=["data"])


def fetch_share_structure(symbol: str) -> pd.DataFrame:
    """东财股本结构变动。列: change_date, total_shares, float_shares, reason。单位:股。"""
    import akshare as ak
    # 列映射按 Task2 Step1 探测结果落定;变动日期 -> change_date,总股本 -> total_shares,
    # 流通股/流通A股 -> float_shares,变动原因 -> reason;万股口径则 ×10000。
    ...


def fetch_valuation(symbol: str) -> pd.DataFrame:
    """东财估值历史。列: trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, total_mv。"""
    import akshare as ak
    # 按 Task2 Step1 探测的 stock_value_em 实际签名/列名实现;市值单位统一为元。
    ...
```

> 实现者注意:`RENAME_LRB/ZCFZ/XJLL` 与两个 `...` 函数体是**必须在本任务内完成**的(依据 Step 1 探测输出),不是留白——完成后文件内不得有 `...`。

- [ ] **Step 3: 验证(轻量真实请求:截面 1 期 ×2 接口、新浪 1 股、股本/估值各 1 股)**

Run:
```bash
ASTOCK_DB_USER=zhu .venv/bin/python - <<'EOF'
import common as c
y = c.fetch_fin_cross("yjbb", "20250331")
assert len(y) > 5000 and {"stock_code","eps","roe","ann_date"} <= set(y.columns)
assert y["ann_date"].notna().mean() > 0.95, "公告日覆盖率异常"
z = c.fetch_fin_cross("zcfz", "20250331")
assert {"stock_code","total_assets","total_liab","ann_date"} <= set(z.columns)
s = c.fetch_fin_report_sina("600519", "balance")
assert len(s) > 40 and "report_date" in s.columns and len(s.columns) > 100
import psycopg2; conn = c.get_conn()
n = c.upsert_jsonb_statement(conn, "600519.SH", "balance", s)
assert n >= 35, n   # 近10年约 40 期
with conn.cursor() as cur:
    cur.execute("select data->>'货币资金' from fin_statement where stock_code='600519.SH' and stmt_type='balance' order by report_date desc limit 1")
    assert cur.fetchone()[0] is not None
    cur.execute("delete from fin_statement where stock_code='600519.SH'")  # 试验数据清理
conn.commit(); conn.close()
sh = c.fetch_share_structure("600519"); assert not sh.empty and "total_shares" in sh.columns
v = c.fetch_valuation("600519"); assert not v.empty and {"trade_date","pe_ttm","pb"} <= set(v.columns)
assert len(c.quarter_ends(c.FUND_START, __import__("datetime").date(2026,7,10))) == 43
print("FUND_COMMON_OK")
EOF
```
Expected: `FUND_COMMON_OK`(quarter_ends:2015Q4~2026Q2 共 43 期)。

- [ ] **Step 4: Commit**

```bash
git add common.py
git commit -m "feat: add fundamental fetch layer (cross-section, sina statements, shares, valuation)"
```

---

### Task 3: 09_init_fundamental.py 全量初始化

**Files:**
- Create: `09_init_fundamental.py`

**Interfaces:**
- Consumes: Task 1 表与函数、Task 2 全部拉取函数、既有 `run_stock_todo(todo, task, load_fn, workers, max_consecutive_errors=15)` / `get_done_codes` / `upsert` / `mark_progress`。
- Produces: CLI `09_init_fundamental.py [--workers N] [--limit N] [--reset] [--phase 1|2|3|4|all]`;etl_progress task 名:`init_fund_stmt`(阶段2)、`init_fund_misc`(阶段3);阶段 1/4 为集合操作不走 per-stock 进度。

- [ ] **Step 1: 写脚本(完整骨架,四阶段)**

```python
"""
09_init_fundamental.py — A股基本面全量初始化(近10年,四阶段,断点续传)。

阶段1 截面:40+ 个报告期 × 4 东财截面接口 → fin_indicator 骨干(含 ann_date)。
        幂等:按 (report_date, kind) 记录于 etl_progress(task='init_fund_cross',
        stock_code 字段借存 'YYYYMMDD:kind'),重跑跳过已完成期。
阶段2 全科目:逐股新浪三大报表 → fin_statement(JSONB),run_stock_todo 并行。
阶段3 股本+估值:逐股东财 → share_capital / daily_valuation,run_stock_todo 并行。
阶段4 派生+回填(纯 SQL/本地计算,无网络):
   a) fin_statement.ann_date ← fin_indicator.ann_date(同 stock+report_date);
   b) 派生列 UPDATE:net_margin/roa/debt_ratio/ocf_to_profit(用指标表已有列),
      current_ratio(从 balance JSONB 取 流动资产合计/流动负债合计)。

用法:
  python 09_init_fundamental.py                # 四阶段顺序全跑
  python 09_init_fundamental.py --phase 2 --workers 3 --limit 20
  python 09_init_fundamental.py --reset        # 清空本脚本相关进度
"""
```

主体要点(实现者按此写全,含 argparse 与 main;与 02/05 同构):

```python
def phase1_cross(conn):
    periods = c.quarter_ends(c.FUND_START, date.today())
    done = c.get_done_codes(conn, "init_fund_cross")   # 'YYYYMMDD:kind' 集合
    for p in periods:
        ps = p.strftime("%Y%m%d")
        for kind in ("yjbb", "lrb", "zcfz", "xjll"):
            key = f"{ps}:{kind}"
            if key in done:
                continue
            df = c.fetch_fin_cross(kind, ps)
            n = _upsert_indicator_from_cross(conn, kind, p, df)   # 按 kind 写对应列子集
            c.mark_progress(conn, "init_fund_cross", key, p, "done", f"rows={n}")
            c.log.info("  截面 %s %s: %d 行", ps, kind, n)

def _upsert_indicator_from_cross(conn, kind, report_date, df):
    # yjbb → eps/bps/ocf_ps/roe/gross_margin/revenue/revenue_yoy/net_profit/
    #        net_profit_yoy/industry/ann_date
    # zcfz → total_assets/total_liab/total_equity(+ann_date 取更早者不覆盖非空)
    # xjll → ocf;lrb → (net_profit 为空时兜底)
    # 全部走 c.upsert(conn, "fin_indicator", cols, rows, ["stock_code","report_date"],
    #                update_cols=<本 kind 涉及列>)

def phase2_statements(conn, workers, limit):
    stocks = c.fetch_stock_list()[:limit or None]
    done = c.get_done_codes(conn, "init_fund_stmt")
    todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
    def load(conn2, r):
        total = 0
        for st in ("income", "balance", "cashflow"):
            total += c.upsert_jsonb_statement(conn2, r.stock_code, st,
                                              c.fetch_fin_report_sina(r.symbol, st))
        c.mark_progress(conn2, "init_fund_stmt", r.stock_code, None, "done", f"rows={total}")
    c.run_stock_todo(todo, "init_fund_stmt", load, workers, max_consecutive_errors=15)

def phase3_misc(conn, workers, limit):
    # 同构 phase2:fetch_share_structure → share_capital;fetch_valuation →
    # daily_valuation(过滤 trade_date >= 2016-01-01);task='init_fund_misc'

def phase4_derive(conn):
    # 纯 SQL(完整语句写在脚本里):
    # UPDATE fin_statement fs SET ann_date = fi.ann_date FROM fin_indicator fi
    #   WHERE fs.stock_code=fi.stock_code AND fs.report_date=fi.report_date
    #     AND fs.ann_date IS NULL AND fi.ann_date IS NOT NULL;
    # UPDATE fin_indicator SET
    #   net_margin   = CASE WHEN revenue    <> 0 THEN net_profit/revenue*100 END,
    #   roa          = CASE WHEN total_assets <> 0 THEN net_profit/total_assets*100 END,
    #   debt_ratio   = CASE WHEN total_assets <> 0 THEN total_liab/total_assets*100 END,
    #   ocf_to_profit= CASE WHEN net_profit <> 0 THEN ocf/net_profit END;
    # current_ratio 从 JSONB:
    # UPDATE fin_indicator fi SET current_ratio = 流动资产/流动负债(NULLIF 防零)
    #   FROM fin_statement fs WHERE fs.stmt_type='balance' AND 键匹配
    #   AND (fs.data->>'流动资产合计') ~ '^[0-9.eE+-]+$' AND (fs.data->>'流动负债合计') ~ '^[0-9.eE+-]+$';
```

- [ ] **Step 2: 阶段 1 试跑(2 个报告期)**

临时用 `--phase 1` 配合把 `quarter_ends` 起点临时参数化?**不改代码**:直接全跑阶段 1(43 期 × 4 ≈ 172 次分页调用,~30 分钟,预算内),Run:
`ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 1`
Expected: 逐期日志,fin_indicator 行数 `select count(*)` ≈ 43 期 × ~5,000 股 ≈ 20 万+;`select count(*) from fin_indicator where ann_date is not null` 占比 >95%。

- [ ] **Step 3: 阶段 2 试跑 20 只**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 2 --limit 20 --workers 2`
Expected: fin_statement ≈ 20 股 × 3 表 × ~40 期 ≈ 2,400 行。

- [ ] **Step 4: 阶段 3 试跑 20 只 + 阶段 4 全跑**

Run:
```bash
ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 3 --limit 20 --workers 2
ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 4
```
Expected: share_capital/daily_valuation 有数据;派生列抽查——
`select stock_code, report_date, net_margin, debt_ratio, current_ratio from fin_indicator where stock_code='600519.SH' order by report_date desc limit 2` 手算对照(茅台净利率 ~50%、资产负债率 ~20% 量级)。

- [ ] **Step 5: as-of 真数据验收(spec 验收项 2)**

Run:
```bash
psql -d astock -tAc "select report_date, ann_date, eps from fin_indicator where stock_code='600519.SH' and ann_date is not null order by report_date desc limit 3"
# 取其中一行的 ann_date 记为 D,验证:
psql -d astock -tAc "select report_date from fin_asof('600519.SH', (D - 1))"   # 应返回上一期
psql -d astock -tAc "select report_date from fin_asof('600519.SH', D)"          # 应返回该期
```
Expected: 边界行为与注释一致。

- [ ] **Step 6: Commit**

```bash
git add 09_init_fundamental.py
git commit -m "feat: add fundamental full-history init loader (4 phases)"
```

---

### Task 4: 10_fundamental_update.py 每日增量

**Files:**
- Create: `10_fundamental_update.py`

**Interfaces:**
- Consumes: Task 2/3 全部;`c.MARKETS` 不涉及(纯 A 股)。
- Produces: CLI `10_fundamental_update.py [--workers N] [--limit N] [--force-cross]`;etl_progress task=`daily_fund`。

- [ ] **Step 1: 写脚本(完整内容)**

逻辑(实现者写全):

```python
"""
10_fundamental_update.py — A股基本面每日增量。

1) 估值:对全部股票增量拉 daily_valuation(从库内该股 max(trade_date) 起),
   run_stock_todo 并行 + 熔断 15。
2) 财报核查(自适应):当月 ∈ {1,2,4,7,8,10} (披露季)或 --force-cross 或距上次
   核查 ≥7 天(etl_progress task='daily_fund' stock_code='_cross_check' 的
   last_date 记录)时:对「最近 2 个报告期」重拉 4 个截面接口 → upsert
   fin_indicator;随后对「本次 ann_date 有变化的股票」重拉新浪三大报表 →
   fin_statement,并对这些股票重算阶段4派生列 + ann_date 回填。
3) 股本:每周核查一次(同 _cross_check 机制,stock_code='_share_check'),
   变化则整段重取覆盖。
cron(README 同步):40 18 * * 1-5(在分钟线 18:30 之后)
"""
```

- [ ] **Step 2: 验证(轻量)**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 10_fundamental_update.py --limit 20 --workers 2 --force-cross`
Expected: 估值增量秒过(刚初始化无缺口);截面重拉最近 2 期;无异常堆栈;重跑一遍幂等(daily_valuation 行数不变)。

- [ ] **Step 3: Commit**

```bash
git add 10_fundamental_update.py
git commit -m "feat: add fundamental daily updater (valuation daily, reports adaptive)"
```

---

### Task 5: README 与 cron

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 新增「基本面数据(二期)」章节**

内容必须含:四层表用途速查、`fin_asof`/`fin_asof_all` 用法示例 SQL、初始化命令(分阶段)、已知限制三条(无修订历史 / ann_date 防提前看不防事后修正 / ann_date IS NULL 的行在 as-of 里不可见)、cron 行:

```cron
40 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu .venv/bin/python 10_fundamental_update.py >> update_fund.log 2>&1
```

(cron 的实际安装由控制者在验收时执行,README 只记录。)

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add fundamental layer section to README"
```

---

### Task 6: 全量执行与验收

**Files:** 无新文件。前置:Task 1-5 完成;执行窗口避开 18:00-18:40 cron。

- [ ] **Step 1: 阶段 2 全量(新浪,~5,500 股 × 3 请求,workers 3,~1.5h)**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 2 --workers 3`(后台)

- [ ] **Step 2: 阶段 3 全量(东财,~5,500 股 × 2 请求,workers 2,与阶段 2 错峰串行)**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 3 --workers 2`

- [ ] **Step 3: 失败重跑一轮 + 阶段 4 全量派生**

Run: 重复 Step 1/2 同命令(断点续传只补 error);然后 `--phase 4`。

- [ ] **Step 4: 验收查询(spec 验收 1/3/4)**

```bash
psql -d astock -c "
select 'fin_statement' t, count(*), count(distinct stock_code) from fin_statement
union all select 'fin_indicator', count(*), count(distinct stock_code) from fin_indicator
union all select 'share_capital', count(*), count(distinct stock_code) from share_capital
union all select 'daily_valuation', count(*), count(distinct stock_code) from daily_valuation;
select count(*) filter (where ann_date is null)::float / count(*) as ann_null_ratio from fin_indicator;"
```
Expected: fin_statement ≈ 60万±(5,500×3×~40,新股少期数);fin_indicator ≈ 20万+;daily_valuation ≈ 1,300万;ann_null_ratio < 0.05。ROE/净利率抽查 2 只(银行 + 消费)与东财 F10 页面对照误差 <1%。

- [ ] **Step 5: 装 cron(控制者执行,需用户在场确认)+ 账本关账**

---

## Self-Review 结果

- **Spec 覆盖**:四层 schema(T1)、fin_asof 及边界用例(T1 Step3 + T3 Step5)、数据源修订矩阵(T2)、四阶段初始化含派生回填(T3)、自适应增量(T4)、README 已知限制(T5)、验收数字与幂等(T6)——全覆盖。历史过滤 `FUND_START` 贯穿 T2/T3。
- **占位符**:T2 Step2 的 `RENAME_LRB/ZCFZ/XJLL` 与两个函数体、T3 的 phase3/phase4 SQL 细节,均已显式标注"必须在本任务内依据探测完成,文件内不得残留 `...`"——探测依赖使然,非留白;T3 Step1 骨架配有每段的行为规格。
- **类型一致性**:`fetch_fin_cross(kind, period)->df[stock_code,ann_date,...]` T2 定义 T3 消费;`upsert_jsonb_statement(conn, stock_code, stmt_type, df)` 一致;`run_stock_todo(..., max_consecutive_errors=15)` 与现库签名一致;etl_progress 借用约定(`'YYYYMMDD:kind'`/`'_cross_check'`)在 T3/T4 一致。
