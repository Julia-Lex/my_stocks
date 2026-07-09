-- =============================================================================
-- A股全量历史数据库 · 建库建表脚本
-- =============================================================================
-- 设计原则:
--   1. daily_price 只存「不复权原始价格」;复权因子单独存 adj_factor。
--      前/后复权价格通过视图动态计算,绝不直接落地(否则除权后即失效)。
--   2. daily_price 按年度 RANGE 分区(1990~2030),主键 (stock_code, trade_date)。
--      另建 (trade_date, stock_code) 反向索引,支持整市场截面查询。
--   3. 股票代码统一带交易所后缀:000001.SZ / 600000.SH / 830799.BJ。
--   4. 价格一律 NUMERIC,不用 FLOAT;成交量单位为「股」(三市场统一;东财/腾讯 A 股原始为手,入库层换算)。
--   5. 周线/月线不单独拉取,从后复权日线用物化视图派生(单一事实来源)。
--
-- 用法:
--   createdb astock
--   psql -d astock -f 01_schema.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 股票基础信息(含退市股,防幸存者偏差)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_basic (
    stock_code   VARCHAR(12) PRIMARY KEY,   -- 000001.SZ
    symbol       VARCHAR(6)  NOT NULL,      -- 000001
    name         VARCHAR(64),               -- 平安银行(可能随时间变化,存最新)
    exchange     VARCHAR(4)  NOT NULL,       -- SZ / SH / BJ
    list_date    DATE,                       -- 上市日
    delist_date  DATE,                       -- 退市日(在市为 NULL)
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_stock_basic_active ON stock_basic (is_active);

-- ---------------------------------------------------------------------------
-- 交易日历
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trade_calendar (
    trade_date DATE    PRIMARY KEY,
    is_open    BOOLEAN NOT NULL DEFAULT TRUE
);

-- ---------------------------------------------------------------------------
-- 日线行情(不复权原始价)· 按年度 RANGE 分区
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_price (
    stock_code VARCHAR(12)   NOT NULL,
    trade_date DATE          NOT NULL,
    open       NUMERIC(12,3),
    high       NUMERIC(12,3),
    low        NUMERIC(12,3),
    close      NUMERIC(12,3),
    pre_close  NUMERIC(12,3),
    volume     BIGINT,          -- 成交量,单位:股(2026-07-09 起全库统一;源返回"手"时在入库层 ×100)
    amount     NUMERIC(20,3),   -- 成交额,单位:元
    pct_chg    NUMERIC(10,4),   -- 涨跌幅 %
    turnover   NUMERIC(10,4),   -- 换手率 %
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY RANGE (trade_date);

-- 反向索引:支持「某一天全市场」的截面查询
CREATE INDEX IF NOT EXISTS idx_daily_price_date
    ON daily_price (trade_date, stock_code);

-- 年度分区 1990 ~ 2030(含一个兜底的历史前分区)
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1990..2030 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS daily_price_%s '
            'PARTITION OF daily_price FOR VALUES FROM (%L) TO (%L)',
            y, format('%s-01-01', y), format('%s-01-01', y + 1)
        );
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 后复权因子(与日线分开存,单点更新即全历史生效)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS adj_factor (
    stock_code VARCHAR(12)   NOT NULL,
    trade_date DATE          NOT NULL,
    adj_factor NUMERIC(18,6) NOT NULL,   -- 后复权因子(累计)
    PRIMARY KEY (stock_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_adj_factor_date
    ON adj_factor (trade_date, stock_code);

-- ---------------------------------------------------------------------------
-- 指数日线(不复权,指数无需复权)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS index_daily (
    index_code VARCHAR(12) NOT NULL,   -- sh000001 / sz399001 ...
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

-- ---------------------------------------------------------------------------
-- ETL 进度表(断点续传)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_progress (
    task       VARCHAR(32) NOT NULL,   -- init_daily / init_adj / ...
    stock_code VARCHAR(12) NOT NULL,
    last_date  DATE,                    -- 已成功入库的最新交易日
    status     VARCHAR(16) NOT NULL DEFAULT 'done',  -- done / partial / error
    message    TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task, stock_code)
);

-- ===========================================================================
-- 复权视图:原始价 × 因子动态计算
-- ===========================================================================
-- 重要:adj_factor 是「稀疏」表 —— 只在除权除息日才有一行,平常交易日没有
-- 对应记录。早期版本用 JOIN ... USING (stock_code, trade_date) 精确匹配,
-- 导致视图只吐出除权日那几行(如 000001.SZ 8432 个交易日,精确匹配版视图
-- 只剩 30 行)。改为 LEFT JOIN LATERAL 前向填充:每个交易日取「≤ 该日」的
-- 最近一个因子,查不到则视为 1(尚未发生过除权除息,不复权=复权)。

-- 后复权:原始价 × 「当日为止最近」的后复权因子(前向填充,查不到记为 1)
CREATE OR REPLACE VIEW daily_price_hfq AS
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * f.factor, 3) AS open,
    round(d.high  * f.factor, 3) AS high,
    round(d.low   * f.factor, 3) AS low,
    round(d.close * f.factor, 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM daily_price d
LEFT JOIN LATERAL (
    SELECT a.adj_factor AS factor FROM adj_factor a
    WHERE a.stock_code = d.stock_code AND a.trade_date <= d.trade_date
    ORDER BY a.trade_date DESC LIMIT 1
) f0 ON true
CROSS JOIN LATERAL (SELECT coalesce(f0.factor, 1) AS factor) f;

-- 前复权:后复权价(同上前向填充)÷ 该股票最新一个后复权因子(查不到记为 1)
CREATE OR REPLACE VIEW daily_price_qfq AS
WITH latest AS (
    SELECT DISTINCT ON (stock_code)
           stock_code, adj_factor AS f
    FROM adj_factor
    ORDER BY stock_code, trade_date DESC
)
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * f.factor / coalesce(l.f, 1), 3) AS open,
    round(d.high  * f.factor / coalesce(l.f, 1), 3) AS high,
    round(d.low   * f.factor / coalesce(l.f, 1), 3) AS low,
    round(d.close * f.factor / coalesce(l.f, 1), 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM daily_price d
LEFT JOIN LATERAL (
    SELECT a.adj_factor AS factor FROM adj_factor a
    WHERE a.stock_code = d.stock_code AND a.trade_date <= d.trade_date
    ORDER BY a.trade_date DESC LIMIT 1
) f0 ON true
CROSS JOIN LATERAL (SELECT coalesce(f0.factor, 1) AS factor) f
LEFT JOIN latest l ON l.stock_code = d.stock_code;

-- ===========================================================================
-- 周线 / 月线物化视图:从后复权日线聚合(单一事实来源)
--   刷新: REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_price_hfq;
-- ===========================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS weekly_price_hfq AS
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
FROM daily_price_hfq
GROUP BY stock_code, date_trunc('week', trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_hfq_pk
    ON weekly_price_hfq (stock_code, period_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_price_hfq AS
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
FROM daily_price_hfq
GROUP BY stock_code, date_trunc('month', trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_hfq_pk
    ON monthly_price_hfq (stock_code, period_start);
