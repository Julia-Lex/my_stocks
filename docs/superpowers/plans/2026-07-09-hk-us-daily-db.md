# 港股/美股日线数据库 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有 A 股库(PostgreSQL `astock`)中新增港股(全量 ~2,700 只)与美股(市值前 600 + 中概,~650-700 只)的历史日线、复权因子、指数与增量更新——方案 B:每市场独立表(`hk_*` / `us_*` 前缀)。

**Architecture:** 镜像 A 股三件套(原始价日线分区表 + 复权因子表 + 视图派生复权/周月线),交易日历从指数日线派生。ETL 复用 `common.py` 的重试/upsert/进度机制与 `02_init_load.py` 已验证的线程池并行,把并行 runner 下沉到 `common.py` 后新增两个市场无关脚本。

**Tech Stack:** Python 3.12(uv venv `.venv`)、AKShare(东财/新浪源)、psycopg2、PostgreSQL 14。

**Spec:** `docs/superpowers/specs/2026-07-09-hk-us-daily-db-design.md`

## Global Constraints

- 数据库连接必须带 `ASTOCK_DB_USER=zhu`(本机 Homebrew PG,超级用户是 zhu,无密码)。
- Python 一律用 `.venv/bin/python`(项目根的 uv venv),不用系统 python3。
- 只存不复权原始价;因子单独存;前/后复权只通过视图;绝不落地前复权价。
- 价格列 `NUMERIC` 不用 FLOAT;代码带后缀 `00700.HK` / `AAPL.US`。
- 港/美股成交量单位为**股**(A 股为手)——只在注释/README 记录,不做换算。
- 分区范围:港股 1980~2030,美股 1970~2030。
- 所有网络调用经 `common.with_retry`(指数退避 2s→4s→8s→16s)。
- ⚠️ A 股全量初始化正在后台运行(至 ~17:00)。运行中只允许 `--limit ≤ 20` 且 `--workers 1` 的实测;全量港/美初始化必须等 A 股跑完(同源限流)。
- 本项目无 pytest 依赖:纯函数用一次性断言脚本验证,ETL 用小规模真实拉取 + SQL 核对验证。

## File Structure

| 文件 | 动作 | 职责 |
| --- | --- | --- |
| `04_schema_hk_us.sql` | Create | 港/美两套表、分区、视图、物化视图 |
| `common.py` | Modify | 下沉并行 runner;新增 MARKETS 配置、港/美拉取函数、upsert/查询的表名参数化 |
| `02_init_load.py` | Modify | 删除本地并行代码,改调 `common.run_stock_todo` |
| `05_init_load_intl.py` | Create | 港/美全量初始化(`--market hk\|us`) |
| `06_daily_update_intl.py` | Create | 港/美每日增量 + 补漏 |
| `README.md` | Modify | 新增港美股章节(范围、单位、cron) |

---

### Task 1: 港/美 Schema

**Files:**
- Create: `04_schema_hk_us.sql`

**Interfaces:**
- Produces: 表 `hk_stock_basic`, `hk_daily_price`(分区), `hk_adj_factor`, `hk_trade_calendar`, `hk_index_daily`;视图 `hk_daily_price_hfq/qfq`;物化视图 `hk_weekly_price_hfq`, `hk_monthly_price_hfq`;`us_` 同构(us_stock_basic 多 `em_symbol` 列)。

- [ ] **Step 1: 写 schema 文件**

`04_schema_hk_us.sql` 完整内容(两个市场镜像,用 psql 变量循环不可行,直接写两遍;此处以港股为例,美股段把 `hk_` 全替换为 `us_`、分区起点 1980 改 1970、`symbol VARCHAR(6)` 改 `VARCHAR(12)` 并在 us_stock_basic 增加 `em_symbol VARCHAR(16)` 列——文件里两段都要完整写出):

```sql
-- =============================================================================
-- 港股/美股日线库 · 方案 B:每市场独立表(前缀 hk_ / us_)
-- 设计同 A 股 01_schema.sql:原始价 + 因子分离、年度分区、视图派生复权。
-- 成交量单位:股(A 股为手)。货币按表隐含:hk_*=HKD,us_*=USD。
-- 用法: psql -d astock -f 04_schema_hk_us.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 港股
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hk_stock_basic (
    stock_code   VARCHAR(12) PRIMARY KEY,   -- 00700.HK
    symbol       VARCHAR(6)  NOT NULL,      -- 00700
    name         VARCHAR(64),
    exchange     VARCHAR(8)  NOT NULL DEFAULT 'HKEX',
    list_date    DATE,
    delist_date  DATE,
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hk_trade_calendar (
    trade_date DATE    PRIMARY KEY,
    is_open    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS hk_daily_price (
    stock_code VARCHAR(12)   NOT NULL,
    trade_date DATE          NOT NULL,
    open       NUMERIC(12,3),
    high       NUMERIC(12,3),
    low        NUMERIC(12,3),
    close      NUMERIC(12,3),
    pre_close  NUMERIC(12,3),
    volume     BIGINT,          -- 单位:股
    amount     NUMERIC(20,3),   -- 单位:港元
    pct_chg    NUMERIC(10,4),
    turnover   NUMERIC(10,4),
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_hk_daily_price_date
    ON hk_daily_price (trade_date, stock_code);

DO $$
DECLARE y INT;
BEGIN
    FOR y IN 1980..2030 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS hk_daily_price_%s '
            'PARTITION OF hk_daily_price FOR VALUES FROM (%L) TO (%L)',
            y, format('%s-01-01', y), format('%s-01-01', y + 1)
        );
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS hk_adj_factor (
    stock_code VARCHAR(12)   NOT NULL,
    trade_date DATE          NOT NULL,
    adj_factor NUMERIC(18,6) NOT NULL,
    PRIMARY KEY (stock_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_hk_adj_factor_date
    ON hk_adj_factor (trade_date, stock_code);

CREATE TABLE IF NOT EXISTS hk_index_daily (
    index_code VARCHAR(12) NOT NULL,   -- HSI / HSTECH ...
    trade_date DATE        NOT NULL,
    open       NUMERIC(14,3),
    high       NUMERIC(14,3),
    low        NUMERIC(14,3),
    close      NUMERIC(14,3),
    pre_close  NUMERIC(14,3),
    volume     BIGINT,
    amount     NUMERIC(24,3),
    PRIMARY KEY (index_code, trade_date)
);

CREATE OR REPLACE VIEW hk_daily_price_hfq AS
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * a.adj_factor, 3) AS open,
    round(d.high  * a.adj_factor, 3) AS high,
    round(d.low   * a.adj_factor, 3) AS low,
    round(d.close * a.adj_factor, 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM hk_daily_price d
JOIN hk_adj_factor a USING (stock_code, trade_date);

CREATE OR REPLACE VIEW hk_daily_price_qfq AS
WITH latest AS (
    SELECT DISTINCT ON (stock_code) stock_code, adj_factor AS f
    FROM hk_adj_factor ORDER BY stock_code, trade_date DESC
)
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * a.adj_factor / l.f, 3) AS open,
    round(d.high  * a.adj_factor / l.f, 3) AS high,
    round(d.low   * a.adj_factor / l.f, 3) AS low,
    round(d.close * a.adj_factor / l.f, 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM hk_daily_price d
JOIN hk_adj_factor a USING (stock_code, trade_date)
JOIN latest     l USING (stock_code);

CREATE MATERIALIZED VIEW IF NOT EXISTS hk_weekly_price_hfq AS
SELECT
    stock_code,
    date_trunc('week', trade_date)::date               AS period_start,
    max(trade_date)                                    AS trade_date,
    (array_agg(open  ORDER BY trade_date))[1]          AS open,
    max(high)                                          AS high,
    min(low)                                           AS low,
    (array_agg(close ORDER BY trade_date DESC))[1]     AS close,
    sum(volume)                                        AS volume,
    sum(amount)                                        AS amount
FROM hk_daily_price_hfq
GROUP BY stock_code, date_trunc('week', trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hk_weekly_hfq_pk
    ON hk_weekly_price_hfq (stock_code, period_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS hk_monthly_price_hfq AS
SELECT
    stock_code,
    date_trunc('month', trade_date)::date              AS period_start,
    max(trade_date)                                    AS trade_date,
    (array_agg(open  ORDER BY trade_date))[1]          AS open,
    max(high)                                          AS high,
    min(low)                                           AS low,
    (array_agg(close ORDER BY trade_date DESC))[1]     AS close,
    sum(volume)                                        AS volume,
    sum(amount)                                        AS amount
FROM hk_daily_price_hfq
GROUP BY stock_code, date_trunc('month', trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hk_monthly_hfq_pk
    ON hk_monthly_price_hfq (stock_code, period_start);

-- ---------------------------------------------------------------------------
-- 美股(与港股同构;差异:分区 1970 起、symbol 更宽、多 em_symbol 列)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS us_stock_basic (
    stock_code   VARCHAR(16) PRIMARY KEY,   -- AAPL.US
    symbol       VARCHAR(12) NOT NULL,      -- AAPL
    name         VARCHAR(64),
    exchange     VARCHAR(8)  NOT NULL DEFAULT 'US',  -- NASDAQ / NYSE / AMEX / US
    em_symbol    VARCHAR(16),               -- 东财拉数代码:105.AAPL
    list_date    DATE,
    delist_date  DATE,
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- (us_trade_calendar / us_daily_price 分区 1970..2030 / us_adj_factor /
--  us_index_daily / us_daily_price_hfq / us_daily_price_qfq /
--  us_weekly_price_hfq / us_monthly_price_hfq —— 与上面 hk_ 段逐行同构,
--  stock_code 列宽用 VARCHAR(16)。实现时完整写出,不得省略。)
```

> 实现者注意:上方括号里的"同构省略"只是**计划文档**为省篇幅;实际 `04_schema_hk_us.sql` 里美股段必须完整写出全部 DDL。

- [ ] **Step 2: 应用 schema**

Run: `psql -d astock -f 04_schema_hk_us.sql`
Expected: 全部 `CREATE TABLE` / `CREATE VIEW` / `DO`,无 ERROR。

- [ ] **Step 3: 验证表与分区数量**

Run:
```bash
psql -d astock -tAc "select count(*) from pg_class where relname like 'hk_daily_price_%' and relkind='r'"
psql -d astock -tAc "select count(*) from pg_class where relname like 'us_daily_price_%' and relkind='r'"
psql -d astock -tAc "\d us_stock_basic" | grep em_symbol
```
Expected: `51`(1980..2030)、`61`(1970..2030)、em_symbol 行存在。

- [ ] **Step 4: Commit**

```bash
git add 04_schema_hk_us.sql
git commit -m "feat: add HK/US per-market schema (Plan B)"
```

---

### Task 2: 并行 runner 下沉到 common.py

**Files:**
- Modify: `common.py`(文件末尾新增)
- Modify: `02_init_load.py`(删除本地并行实现,改调用)

**Interfaces:**
- Produces: `common.run_stock_todo(todo, task: str, load_fn, workers: int) -> None`,其中 `load_fn(conn, row)`,row 至少有 `stock_code` 属性;失败自动 rollback + `mark_progress(status='error')`,每 100 只打进度,结束时关闭全部线程连接。
- Consumes: 现有 `get_conn` / `mark_progress` / `log`。

⚠️ A 股全量正在用旧版 `02_init_load.py` 运行——只改文件不影响已加载进内存的进程;本任务**不得**重新运行 02。

- [ ] **Step 1: 在 common.py 末尾追加 runner(从 02 平移)**

```python
# ---------------------------------------------------------------------------
# 并行执行:每个工作线程持有自己的数据库连接(psycopg2 连接不能跨线程共享)。
# 断点续传由 etl_progress 保证,与并发无关。
# ---------------------------------------------------------------------------
import itertools
import threading
from concurrent.futures import ThreadPoolExecutor

_tls = threading.local()
_all_conns: list = []
_conns_lock = threading.Lock()


def _thread_conn():
    """当前线程专属的数据库连接(懒创建,run_stock_todo 结束时统一关闭)。"""
    conn = getattr(_tls, "conn", None)
    if conn is None or conn.closed:
        conn = get_conn()
        _tls.conn = conn
        with _conns_lock:
            _all_conns.append(conn)
    return conn


def run_stock_todo(todo, task: str, load_fn, workers: int) -> None:
    """
    按 workers 数串行或并行处理股票清单。
    load_fn(conn, row):处理单只;抛异常则记 error 进度,不中断整体。
    """
    todo = list(todo)
    total = len(todo)
    counter = itertools.count(1)  # CPython 下 next() 原子,足够做进度计数

    def work(r):
        conn = _thread_conn()
        try:
            load_fn(conn, r)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            mark_progress(conn, task, r.stock_code, None, status="error", message=str(exc))
            log.error("  %s 失败: %s", r.stock_code, exc)
        i = next(counter)
        if i % 100 == 0:
            log.info("进度 %d / %d", i, total)

    if workers <= 1:
        for r in todo:
            work(r)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(work, todo))
    with _conns_lock:
        for conn in _all_conns:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        _all_conns.clear()
```

- [ ] **Step 2: 02_init_load.py 改为调用 common 版**

删除 02 中 `_tls` / `_all_conns` / `_conns_lock` / `_thread_conn` / `_run_todo` 整段与 `itertools/threading/ThreadPoolExecutor` 导入,主流程改:

```python
        def _load(conn2, r):
            load_one_stock(conn2, r.stock_code, r.symbol)

        c.run_stock_todo(todo, TASK, _load, args.workers)
```

- [ ] **Step 3: 编译检查 + runner 行为验证(不碰网络)**

Run:
```bash
.venv/bin/python -m py_compile common.py 02_init_load.py && echo COMPILE_OK
ASTOCK_DB_USER=zhu .venv/bin/python - <<'EOF'
from types import SimpleNamespace
import common as c
seen = []
rows = [SimpleNamespace(stock_code=f"T{i:03d}.XX") for i in range(7)]
c.run_stock_todo(rows, "unit_test", lambda conn, r: seen.append(r.stock_code), workers=3)
assert sorted(seen) == sorted(r.stock_code for r in rows), seen
# 异常路径:应记 error 而不抛出
c.run_stock_todo(rows[:2], "unit_test", lambda conn, r: 1/0, workers=2)
conn = c.get_conn()
with conn.cursor() as cur:
    cur.execute("select count(*) from etl_progress where task='unit_test' and status='error'")
    assert cur.fetchone()[0] == 2
    cur.execute("delete from etl_progress where task='unit_test'")
conn.commit(); conn.close()
print("RUNNER_OK")
EOF
```
Expected: `COMPILE_OK` 和 `RUNNER_OK`。

- [ ] **Step 4: Commit**

```bash
git add common.py 02_init_load.py
git commit -m "refactor: move parallel stock runner into common.run_stock_todo"
```

---

### Task 3: common.py 港/美市场层

**Files:**
- Modify: `common.py`

**Interfaces:**
- Produces:
  - `MARKETS: dict` —— `MARKETS["hk"] = {"prefix": "hk_", "suffix": ".HK", "indexes": ["HSI", "HSTECH"], "start": "19800101", "mviews": ("hk_weekly_price_hfq", "hk_monthly_price_hfq")}`;`MARKETS["us"]` 同构(prefix `us_`,suffix `.US`,indexes `[".INX", ".IXIC", ".DJI"]`,start `"19700101"`,mviews us 前缀)。
  - `fetch_hk_stock_list() -> DataFrame[stock_code, symbol, name, exchange]`
  - `fetch_us_stock_list(top_n=600) -> DataFrame[stock_code, symbol, name, exchange, em_symbol]`
  - `fetch_intl_daily(market, fetch_symbol, start=None, end=None, adjust="") -> DataFrame`(列同 `fetch_daily`)
  - `fetch_intl_hfq_factor(market, fetch_symbol, raw=None) -> DataFrame[trade_date, adj_factor]`
  - `fetch_intl_index(market, index_code) -> DataFrame`(列同 `fetch_index`)
  - `rebuild_intl_calendar(conn, market) -> None`(从 `{prefix}index_daily` 派生日历)
  - 既有 `upsert_daily/upsert_adj_factor/upsert_index/get_max_trade_date` 增加 `table=` 关键字参数(默认值 = 现 A 股表名,旧调用零改动);`refresh_matviews(conn, names=("weekly_price_hfq", "monthly_price_hfq"))`。
- Consumes: Task 2 的 runner、既有 `with_retry` / `RENAME_HIST` / `RENAME_INDEX` / `upsert`。

- [ ] **Step 1: 先探测指数接口可用性(接口名易变,写代码前定死)**

Run:
```bash
.venv/bin/python - <<'EOF'
import akshare as ak
for name, kw in [("stock_hk_index_daily_sina", {"symbol": "HSI"}),
                 ("stock_hk_index_daily_em",   {"symbol": "HSI"}),
                 ("index_us_stock_sina",       {"symbol": ".INX"})]:
    try:
        fn = getattr(ak, name)
        df = fn(**kw)
        print(name, "OK", len(df), list(df.columns)[:7])
    except Exception as e:
        print(name, "FAIL", type(e).__name__, str(e)[:80])
EOF
```
Expected: 至少一个 HK 接口 OK + `index_us_stock_sina` OK。**用探测结果决定 `fetch_intl_index` 内用哪个接口/列名映射**;若列名与 `RENAME_INDEX` 不符,新增 `RENAME_INDEX_EM` 映射。若 `HSTECH` 不被支持,indexes 改 `["HSI", "HSCEI"]` 并同步改设计文档。

- [ ] **Step 2: 在 common.py 追加市场层代码**

```python
# ===========================================================================
# 港股 / 美股(方案 B 分表)。表前缀、拉数函数按 MARKETS 配置分发。
# 成交量单位:股;货币按表隐含(hk_*=HKD,us_*=USD)。
# ===========================================================================
MARKETS = {
    "hk": {
        "prefix": "hk_", "suffix": ".HK",
        "indexes": ["HSI", "HSTECH"],          # 以 Task3 Step1 探测结果为准
        "start": "19800101",
        "mviews": ("hk_weekly_price_hfq", "hk_monthly_price_hfq"),
    },
    "us": {
        "prefix": "us_", "suffix": ".US",
        "indexes": [".INX", ".IXIC", ".DJI"],
        "start": "19700101",
        "mviews": ("us_weekly_price_hfq", "us_monthly_price_hfq"),
    },
}

_US_EXCHANGE = {"105": "NASDAQ", "106": "NYSE", "107": "AMEX"}


def fetch_hk_stock_list() -> pd.DataFrame:
    """东财港股全列表。返回列: stock_code, symbol, name, exchange。"""
    import akshare as ak

    df = with_retry(ak.stock_hk_spot_em)
    df = df.rename(columns={"代码": "symbol", "名称": "name"})
    df["symbol"] = df["symbol"].astype(str).str.zfill(5)
    df["stock_code"] = df["symbol"] + ".HK"
    df["exchange"] = "HKEX"
    return df[["stock_code", "symbol", "name", "exchange"]].drop_duplicates("stock_code")


def fetch_us_stock_list(top_n: int = 600) -> pd.DataFrame:
    """
    东财美股列表:总市值前 top_n + 知名中概股,去重。
    返回列: stock_code, symbol, name, exchange, em_symbol。
    中概接口不可用时仅用市值前 top_n(其已覆盖主要中概)。
    """
    import akshare as ak

    spot = with_retry(ak.stock_us_spot_em)
    spot = spot.rename(columns={"代码": "em_symbol", "名称": "name", "总市值": "mktcap"})
    spot["mktcap"] = pd.to_numeric(spot["mktcap"], errors="coerce")
    frames = [spot.dropna(subset=["mktcap"])
                  .sort_values("mktcap", ascending=False).head(top_n)[["em_symbol", "name"]]]
    try:
        zh = with_retry(ak.stock_us_famous_spot_em, symbol="中概股")
        zh = zh.rename(columns={"代码": "em_symbol", "名称": "name"})
        frames.append(zh[["em_symbol", "name"]])
    except Exception as exc:  # noqa: BLE001
        log.warning("知名中概股接口不可用,仅用市值前 %d: %s", top_n, exc)

    df = pd.concat(frames, ignore_index=True).drop_duplicates("em_symbol")
    df["em_symbol"] = df["em_symbol"].astype(str)
    df["symbol"] = df["em_symbol"].str.split(".").str[-1]
    df["stock_code"] = df["symbol"] + ".US"
    df["exchange"] = df["em_symbol"].str.split(".").str[0].map(_US_EXCHANGE).fillna("US")
    return df.drop_duplicates("stock_code")[
        ["stock_code", "symbol", "name", "exchange", "em_symbol"]]


def fetch_intl_daily(market: str, fetch_symbol: str,
                     start: Optional[str] = None, end: Optional[str] = None,
                     adjust: str = "") -> pd.DataFrame:
    """港/美单只不复权日线。fetch_symbol:港股 '00700',美股 '105.AAPL'。"""
    import akshare as ak

    cfg = MARKETS[market]
    start = start or cfg["start"]
    end = end or datetime.now().strftime("%Y%m%d")
    fn = ak.stock_hk_hist if market == "hk" else ak.stock_us_hist
    df = with_retry(fn, symbol=fetch_symbol, period="daily",
                    start_date=start, end_date=end, adjust=adjust)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_HIST)
    keep = [c for c in RENAME_HIST.values() if c in df.columns]
    df = df[keep].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def fetch_intl_hfq_factor(market: str, fetch_symbol: str,
                          raw: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """后复权因子 = hfq 收盘 ÷ 原始收盘。raw 可传入已拉取的不复权日线省一次请求。"""
    if raw is None:
        raw = fetch_intl_daily(market, fetch_symbol)
    hfq = fetch_intl_daily(market, fetch_symbol, adjust="hfq")
    if raw.empty or hfq.empty:
        return pd.DataFrame()
    merged = raw[["trade_date", "close"]].merge(
        hfq[["trade_date", "close"]], on="trade_date", suffixes=("_raw", "_hfq"))
    merged = merged[merged["close_raw"] > 0]
    merged["adj_factor"] = merged["close_hfq"] / merged["close_raw"]
    return merged[["trade_date", "adj_factor"]].dropna()


def fetch_intl_index(market: str, index_code: str) -> pd.DataFrame:
    """港/美指数日线。港:HSI 等;美:.INX/.IXIC/.DJI(新浪代码)。"""
    import akshare as ak

    if market == "hk":
        df = with_retry(ak.stock_hk_index_daily_sina, symbol=index_code)  # 以探测结果为准
    else:
        df = with_retry(ak.index_us_stock_sina, symbol=index_code)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_INDEX)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    keep = [c for c in ["trade_date", "open", "high", "low", "close", "volume", "amount"]
            if c in df.columns]
    return df[keep].copy()


def rebuild_intl_calendar(conn, market: str) -> None:
    """交易日历 = 指数日线出现过的日期(设计:从指数派生,无独立日历源)。"""
    p = MARKETS[market]["prefix"]
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {p}trade_calendar (trade_date, is_open) "
            f"SELECT DISTINCT trade_date, TRUE FROM {p}index_daily "
            f"ON CONFLICT (trade_date) DO NOTHING"
        )
    conn.commit()
```

- [ ] **Step 3: 表名参数化(改既有四个函数,默认值保证 A 股调用零改动)**

`upsert_daily`、`upsert_adj_factor`、`upsert_index`、`get_max_trade_date` 各加尾参:

```python
def upsert_daily(conn, stock_code: str, df: pd.DataFrame, table: str = "daily_price") -> int:
    ...
    return upsert(conn, table, cols, rows, ["stock_code", "trade_date"])

def upsert_adj_factor(conn, stock_code: str, df: pd.DataFrame, table: str = "adj_factor") -> int:
    ...
    return upsert(conn, table, cols, rows, ["stock_code", "trade_date"])

def upsert_index(conn, index_code: str, df: pd.DataFrame, table: str = "index_daily") -> int:
    ...
    return upsert(conn, table, cols, rows, ["index_code", "trade_date"])

def get_max_trade_date(conn, stock_code: Optional[str] = None,
                       table: str = "daily_price") -> Optional[date]:
    # 两条 SQL 里的表名改为 f-string 插入 table
```

`refresh_matviews` 改签名:

```python
def refresh_matviews(conn, names: Sequence[str] = ("weekly_price_hfq", "monthly_price_hfq")) -> None:
    with conn.cursor() as cur:
        for mv in names:
            ...(循环体不变)
```

- [ ] **Step 4: 纯函数与小规模真实拉取验证**

Run:
```bash
.venv/bin/python -m py_compile common.py && echo COMPILE_OK
ASTOCK_DB_USER=zhu .venv/bin/python - <<'EOF'
import common as c
# 港股日线 + 因子(单请求级,不违反限流约束)
d = c.fetch_intl_daily("hk", "00700", start="20240102", end="20240131")
assert not d.empty and {"trade_date","open","close","volume"} <= set(d.columns), d.columns
f = c.fetch_intl_hfq_factor("hk", "00700", raw=c.fetch_intl_daily("hk", "00700"))
assert not f.empty and float(f["adj_factor"].iloc[-1]) > 0
# 美股
u = c.fetch_intl_daily("us", "105.AAPL", start="20240102", end="20240131")
assert not u.empty
# 指数 + 日历派生(写入真实库)
hsi = c.fetch_intl_index("hk", "HSI"); assert not hsi.empty
conn = c.get_conn()
c.upsert_index(conn, "HSI", hsi, table="hk_index_daily")
c.rebuild_intl_calendar(conn, "hk")
with conn.cursor() as cur:
    cur.execute("select count(*) from hk_trade_calendar"); n = cur.fetchone()[0]
assert n > 5000, n
conn.close()
print("INTL_COMMON_OK, hk_calendar =", n)
EOF
```
Expected: `COMPILE_OK`、`INTL_COMMON_OK, hk_calendar = 7000±`(恒指 1986 年起,交易日 ~9000 内)。

- [ ] **Step 5: Commit**

```bash
git add common.py
git commit -m "feat: add HK/US market layer in common (fetchers, table params, calendar)"
```

---

### Task 4: 05_init_load_intl.py 全量初始化脚本

**Files:**
- Create: `05_init_load_intl.py`

**Interfaces:**
- Consumes: Task 2 `run_stock_todo`、Task 3 全部市场层函数。
- Produces: CLI `05_init_load_intl.py --market hk|us [--workers N] [--limit N] [--reset]`;etl_progress task 名 `init_hk` / `init_us`。

- [ ] **Step 1: 写脚本(完整内容)**

```python
"""
05_init_load_intl.py — 港股/美股全量历史初始化(带断点续传)。

流程同 02_init_load.py:参考数据(列表/指数/派生日历)→ 逐只日线+因子 → 物化视图。
用法:
  python 05_init_load_intl.py --market hk               # 港股全量
  python 05_init_load_intl.py --market us --workers 3   # 美股 3 并发
  python 05_init_load_intl.py --market hk --limit 20    # 试跑
  python 05_init_load_intl.py --market hk --reset       # 清空进度重来
"""

from __future__ import annotations

import argparse
import sys

import common as c


def load_reference_data(conn, market: str):
    """股票列表 + 指数日线 + 派生交易日历。返回股票 DataFrame。"""
    cfg = c.MARKETS[market]
    p = cfg["prefix"]

    c.log.info("[%s] 加载股票列表 ...", market)
    if market == "hk":
        stocks = c.fetch_hk_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange"]
    else:
        stocks = c.fetch_us_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange", "em_symbol"]
    c.upsert(conn, f"{p}stock_basic", cols,
             [tuple(getattr(r, x) for x in cols) for r in stocks.itertuples(index=False)],
             ["stock_code"], update_cols=["name", "exchange"])
    c.log.info("[%s] 股票列表 %d 只", market, len(stocks))

    c.log.info("[%s] 加载指数日线 ...", market)
    for idx in cfg["indexes"]:
        try:
            n = c.upsert_index(conn, idx, c.fetch_intl_index(market, idx),
                               table=f"{p}index_daily")
            c.log.info("  指数 %s: %d 行", idx, n)
        except Exception as exc:  # noqa: BLE001
            c.log.warning("  指数 %s 失败: %s", idx, exc)
    c.rebuild_intl_calendar(conn, market)
    return stocks


def make_loader(market: str, task: str):
    """返回 load_fn(conn, row) 供 run_stock_todo 调用。"""
    p = c.MARKETS[market]["prefix"]

    def load_one(conn, r):
        fetch_symbol = getattr(r, "em_symbol", None) or r.symbol
        daily = c.fetch_intl_daily(market, fetch_symbol)
        n_daily = c.upsert_daily(conn, r.stock_code, daily, table=f"{p}daily_price")
        adj = c.fetch_intl_hfq_factor(market, fetch_symbol, raw=daily)
        n_adj = c.upsert_adj_factor(conn, r.stock_code, adj, table=f"{p}adj_factor")
        last = daily["trade_date"].max() if not daily.empty else None
        c.mark_progress(conn, task, r.stock_code, last, status="done",
                        message=f"daily={n_daily},adj={n_adj}")
        c.log.info("  %s: 日线 %d / 因子 %d", r.stock_code, n_daily, n_adj)

    return load_one


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=("hk", "us"))
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(试跑)")
    ap.add_argument("--reset", action="store_true", help="清空该市场 init 进度重来")
    ap.add_argument("--workers", type=int, default=1,
                    help="并发拉取线程数(默认 1;免费源限流,建议不超过 4)")
    args = ap.parse_args()

    task = f"init_{args.market}"
    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (task,))
            conn.commit()
            c.log.info("已清空 %s 进度", task)

        stocks = load_reference_data(conn, args.market)
        if args.limit:
            stocks = stocks.head(args.limit)

        done = c.get_done_codes(conn, task)
        todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
        c.log.info("[%s] 待处理 %d 只(已完成 %d 只,并发 %d)",
                   args.market, len(todo), len(done), args.workers)

        c.run_stock_todo(todo, task, make_loader(args.market, task), args.workers)

        c.log.info("[%s] 刷新周线/月线物化视图 ...", args.market)
        c.refresh_matviews(conn, c.MARKETS[args.market]["mviews"])
        c.log.info("[%s] 全量初始化完成 ✅", args.market)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: 试跑港股 20 只(A 股全量运行期间:workers=1)**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 05_init_load_intl.py --market hk --limit 20 --workers 1`
Expected: 列表 ~2700 只、两条指数、`待处理 20 只`、逐只 `XXXXX.HK: 日线 N / 因子 M`、`完成 ✅`。

- [ ] **Step 3: 试跑美股 20 只**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 05_init_load_intl.py --market us --limit 20 --workers 1`
Expected: 列表 600~700 只(留意日志是否出现"中概接口不可用"警告——出现也算通过,但要记录)、`完成 ✅`。

- [ ] **Step 4: SQL 核对**

Run:
```bash
psql -d astock -c "select count(*) from hk_daily_price; select count(*) from us_daily_price;
select * from hk_daily_price_hfq where stock_code='00700.HK' order by trade_date desc limit 3;
select stock_code, count(*) from us_adj_factor group by 1 order by 2 desc limit 3;"
```
Expected: 两表各数万行;00700.HK 后复权行存在且 close>0;因子表有数据。

- [ ] **Step 5: Commit**

```bash
git add 05_init_load_intl.py
git commit -m "feat: add HK/US full-history init loader"
```

---

### Task 5: 06_daily_update_intl.py 每日增量

**Files:**
- Create: `06_daily_update_intl.py`

**Interfaces:**
- Consumes: Task 3 市场层 + `get_max_trade_date(table=)` + `run_stock_todo`。
- Produces: CLI `06_daily_update_intl.py --market hk|us [--days N] [--limit N] [--no-matview] [--workers N]`;etl_progress task 名 `daily_hk` / `daily_us`。

- [ ] **Step 1: 写脚本(完整内容;逻辑平移 03,表名带前缀)**

```python
"""
06_daily_update_intl.py — 港股/美股每日增量更新(带自动补漏)。

同 03_daily_update.py:先刷参考数据(列表/指数/日历),再按缺口增量。
cron 建议(北京时间):
  港股: 0 18 * * 1-5  ... python 06_daily_update_intl.py --market hk
  美股: 0 9  * * 2-6  ... python 06_daily_update_intl.py --market us   # 拉前一交易日
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta

import common as c


def expected_open_dates(conn, market: str, since: date) -> list[date]:
    p = c.MARKETS[market]["prefix"]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date FROM {p}trade_calendar "
            f"WHERE is_open AND trade_date >= %s AND trade_date <= %s ORDER BY trade_date",
            (since, date.today()),
        )
        return [r[0] for r in cur.fetchall()]


def existing_dates(conn, market: str, stock_code: str, since: date) -> set[date]:
    p = c.MARKETS[market]["prefix"]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date FROM {p}daily_price "
            f"WHERE stock_code = %s AND trade_date >= %s",
            (stock_code, since),
        )
        return {r[0] for r in cur.fetchall()}


def update_reference(conn, market: str):
    cfg = c.MARKETS[market]
    p = cfg["prefix"]
    if market == "hk":
        stocks = c.fetch_hk_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange"]
    else:
        stocks = c.fetch_us_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange", "em_symbol"]
    c.upsert(conn, f"{p}stock_basic", cols,
             [tuple(getattr(r, x) for x in cols) for r in stocks.itertuples(index=False)],
             ["stock_code"], update_cols=["name", "exchange"])
    for idx in cfg["indexes"]:
        try:
            c.upsert_index(conn, idx, c.fetch_intl_index(market, idx),
                           table=f"{p}index_daily")
        except Exception as exc:  # noqa: BLE001
            c.log.warning("指数 %s 更新失败: %s", idx, exc)
    c.rebuild_intl_calendar(conn, market)
    return stocks


def make_updater(market: str, task: str, lookback_days: int):
    p = c.MARKETS[market]["prefix"]
    init_start = {"hk": date(1980, 1, 1), "us": date(1970, 1, 1)}[market]

    def update_one(conn, r):
        fetch_symbol = getattr(r, "em_symbol", None) or r.symbol
        max_d = c.get_max_trade_date(conn, r.stock_code, table=f"{p}daily_price")
        start = init_start if max_d is None else max_d - timedelta(days=lookback_days)

        need = set(expected_open_dates(conn, market, start))
        if not need:
            return
        have = existing_dates(conn, market, r.stock_code, start)
        missing = need - have
        if not missing:
            return

        daily = c.fetch_intl_daily(market, fetch_symbol,
                                   start=min(missing).strftime("%Y%m%d"),
                                   end=max(missing).strftime("%Y%m%d"))
        n = c.upsert_daily(conn, r.stock_code, daily, table=f"{p}daily_price")
        adj = c.fetch_intl_hfq_factor(market, fetch_symbol)   # 因子整段重取覆盖
        c.upsert_adj_factor(conn, r.stock_code, adj, table=f"{p}adj_factor")
        last = daily["trade_date"].max() if not daily.empty else max_d
        c.mark_progress(conn, task, r.stock_code, last, status="done", message=f"+{n}")

    return update_one


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=("hk", "us"))
    ap.add_argument("--days", type=int, default=5, help="回看天数(补漏安全边界)")
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(调试)")
    ap.add_argument("--no-matview", action="store_true", help="跳过物化视图刷新")
    ap.add_argument("--workers", type=int, default=1, help="并发线程数")
    args = ap.parse_args()

    task = f"daily_{args.market}"
    conn = c.get_conn()
    try:
        c.log.info("[%s] 更新参考数据(列表/指数/日历) ...", args.market)
        stocks = update_reference(conn, args.market)
        if args.limit:
            stocks = stocks.head(args.limit)

        rows = list(stocks.itertuples(index=False))
        c.log.info("[%s] 增量更新 %d 只 ...", args.market, len(rows))
        c.run_stock_todo(rows, task, make_updater(args.market, task, args.days),
                         args.workers)

        if not args.no_matview:
            c.log.info("[%s] 刷新周线/月线物化视图 ...", args.market)
            c.refresh_matviews(conn, c.MARKETS[args.market]["mviews"])
        c.log.info("[%s] 增量更新完成 ✅ (%s)", args.market,
                   datetime.now().strftime("%Y-%m-%d %H:%M"))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: 验证(对已初始化的 20 只港股跑增量,应几乎无缺口)**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 06_daily_update_intl.py --market hk --limit 20 --no-matview`
Expected: `增量更新完成 ✅`;因试跑刚拉过,绝大多数股票无缺口秒过;无异常堆栈。

- [ ] **Step 3: 幂等性验证(再跑一遍,行数不变)**

Run:
```bash
psql -d astock -tAc "select count(*) from hk_daily_price" \
&& ASTOCK_DB_USER=zhu .venv/bin/python 06_daily_update_intl.py --market hk --limit 20 --no-matview >/dev/null 2>&1 \
&& psql -d astock -tAc "select count(*) from hk_daily_price"
```
Expected: 前后两个 count 相等(或仅差当日新增)。

- [ ] **Step 4: Commit**

```bash
git add 06_daily_update_intl.py
git commit -m "feat: add HK/US daily incremental updater"
```

---

### Task 6: README 与文档

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 在 README「目录」表加两行,「后续」前插一章**

目录表追加:

```markdown
| `04_schema_hk_us.sql` | 港股/美股建表(方案 B 分表,前缀 hk_/us_) |
| `05_init_load_intl.py` | 港/美全量初始化(`--market hk|us`) |
| `06_daily_update_intl.py` | 港/美每日增量 + 补漏 |
```

新章节(插在「排错」之后):

```markdown
## 港股 / 美股

方案 B 分市场独立表(设计:`docs/superpowers/specs/2026-07-09-hk-us-daily-db-design.md`):

- 范围:港股全列表 ~2,700 只;美股市值前 600 + 知名中概(快照式,只增不删)。
- **成交量单位为股**(A 股为手);货币按表隐含:`hk_*`=HKD,`us_*`=USD。
- 交易日历从指数日线派生(恒指 / 标普500);复权因子 = 东财 hfq 收盘 ÷ 原始收盘。

```bash
psql -d astock -f 04_schema_hk_us.sql
python 05_init_load_intl.py --market hk --limit 20   # 试跑
python 05_init_load_intl.py --market hk --workers 3  # 港股全量 ~1.5h
python 05_init_load_intl.py --market us --workers 3  # 美股 ~20min
```

每日定时(北京时间;美股收盘为北京次日凌晨,早上拉前一交易日):

```cron
0 18 * * 1-5  cd /path/to/my_stocks && python 06_daily_update_intl.py --market hk >> update_hk.log 2>&1
0 9  * * 2-6  cd /path/to/my_stocks && python 06_daily_update_intl.py --market us >> update_us.log 2>&1
```
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add HK/US section to README"
```

---

### Task 7: 全量执行与验收(A 股初始化完成后)

**Files:** 无新文件;运行 + 核对。

前置:确认 A 股全量已结束(`ps aux | grep 02_init_load` 无进程)。

- [ ] **Step 1: 港股全量(后台,3 并发)**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 05_init_load_intl.py --market hk --workers 3`(后台运行)
Expected: ~1.5 小时完成;失败率 ≤15%(与 A 股一致的限流特征)。

- [ ] **Step 2: 美股全量(港股完成后再启,错峰)**

Run: `ASTOCK_DB_USER=zhu .venv/bin/python 05_init_load_intl.py --market us --workers 3`
Expected: ~20 分钟完成。

- [ ] **Step 3: 失败重跑(两市场各一次,补 error)**

Run: 重复 Step 1/2 同命令。Expected: `待处理` 数 = 上轮失败数,跑完 error 显著减少。

- [ ] **Step 4: 验收查询**

```bash
psql -d astock -c "
select 'hk' m, count(*) rows, count(distinct stock_code) stocks, max(trade_date) latest from hk_daily_price
union all
select 'us', count(*), count(distinct stock_code), max(trade_date) from us_daily_price;
select * from hk_daily_price where stock_code='00700.HK' order by trade_date desc limit 2;
select * from us_daily_price_hfq where stock_code='AAPL.US' order by trade_date desc limit 2;
select count(*) from hk_weekly_price_hfq; select count(*) from us_monthly_price_hfq;"
```
Expected: 港股 800万~1000万 行 / ~2700 只;美股 400万~600万 行 / 600~700 只;00700.HK 最新价与行情软件一致(±0.01);AAPL 后复权>原始价(历史多次拆股);物化视图非空。

- [ ] **Step 5: 抽查复权正确性(设计文档验收项)**

00700.HK 用行情软件对照最近一次除息日前后的前复权价;AAPL.US 对照 2020-08-31(4:1 拆股)前后原始价断崖、后复权价连续。不一致 → 检查 `fetch_intl_hfq_factor` 合并逻辑。

- [ ] **Step 6: 记录验收结果 + Commit(如有修复)**

```bash
git add -A && git commit -m "fix: adjustments found during HK/US full load acceptance"  # 仅当有改动
```

---

## Self-Review 结果

- **Spec 覆盖**:分表 schema(T1)、范围与清单策略含中概兜底(T3)、日历派生(T3/T4)、ETL 并行+断点续传(T2/T4)、增量+补漏(T5)、cron 时区(T6)、验收含拆股抽查(T7)、错峰约束(Global Constraints + T7 前置)——全覆盖。
- **占位符**:T1 计划文档内的"美股段同构省略"已显式标注为计划排版约定,并要求实现时完整写出,不构成实现占位。
- **类型一致性**:`run_stock_todo(todo, task, load_fn, workers)` 在 T2 定义、T4/T5 消费一致;`upsert_daily(..., table=)` 等签名在 T3 定义、T4/T5 使用一致;`MARKETS` 键名(prefix/suffix/indexes/start/mviews)各任务一致。
