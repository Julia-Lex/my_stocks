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

-- hk_adj_factor 是稀疏表(仅除权除息日有行),同 A 股改为前向填充(见
-- 01_schema.sql 顶部说明),查不到因子记为 1。
CREATE OR REPLACE VIEW hk_daily_price_hfq AS
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * f.factor, 3) AS open,
    round(d.high  * f.factor, 3) AS high,
    round(d.low   * f.factor, 3) AS low,
    round(d.close * f.factor, 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM hk_daily_price d
LEFT JOIN LATERAL (
    SELECT a.adj_factor AS factor FROM hk_adj_factor a
    WHERE a.stock_code = d.stock_code AND a.trade_date <= d.trade_date
    ORDER BY a.trade_date DESC LIMIT 1
) f0 ON true
CROSS JOIN LATERAL (SELECT coalesce(f0.factor, 1) AS factor) f;

CREATE OR REPLACE VIEW hk_daily_price_qfq AS
WITH latest AS (
    SELECT DISTINCT ON (stock_code) stock_code, adj_factor AS f
    FROM hk_adj_factor ORDER BY stock_code, trade_date DESC
)
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * f.factor / coalesce(l.f, 1), 3) AS open,
    round(d.high  * f.factor / coalesce(l.f, 1), 3) AS high,
    round(d.low   * f.factor / coalesce(l.f, 1), 3) AS low,
    round(d.close * f.factor / coalesce(l.f, 1), 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM hk_daily_price d
LEFT JOIN LATERAL (
    SELECT a.adj_factor AS factor FROM hk_adj_factor a
    WHERE a.stock_code = d.stock_code AND a.trade_date <= d.trade_date
    ORDER BY a.trade_date DESC LIMIT 1
) f0 ON true
CROSS JOIN LATERAL (SELECT coalesce(f0.factor, 1) AS factor) f
LEFT JOIN latest l ON l.stock_code = d.stock_code;

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
-- 美股(与港股同构;差异:分区 1970 起、symbol 更宽、stock_code 列宽 VARCHAR(16)、多 em_symbol 列)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS us_stock_basic (
    stock_code   VARCHAR(16) PRIMARY KEY,   -- AAPL.US
    symbol       VARCHAR(12) NOT NULL,      -- AAPL
    name         VARCHAR(64),
    exchange     VARCHAR(8)  NOT NULL DEFAULT 'US',
    em_symbol    VARCHAR(16),               -- 东财拉数代码:105.AAPL
    list_date    DATE,
    delist_date  DATE,
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS us_trade_calendar (
    trade_date DATE    PRIMARY KEY,
    is_open    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS us_daily_price (
    stock_code VARCHAR(16)   NOT NULL,
    trade_date DATE          NOT NULL,
    open       NUMERIC(12,3),
    high       NUMERIC(12,3),
    low        NUMERIC(12,3),
    close      NUMERIC(12,3),
    pre_close  NUMERIC(12,3),
    volume     BIGINT,          -- 单位:股
    amount     NUMERIC(20,3),   -- 单位:美元
    pct_chg    NUMERIC(10,4),
    turnover   NUMERIC(10,4),
    PRIMARY KEY (stock_code, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_us_daily_price_date
    ON us_daily_price (trade_date, stock_code);

DO $$
DECLARE y INT;
BEGIN
    FOR y IN 1970..2030 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS us_daily_price_%s '
            'PARTITION OF us_daily_price FOR VALUES FROM (%L) TO (%L)',
            y, format('%s-01-01', y), format('%s-01-01', y + 1)
        );
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS us_adj_factor (
    stock_code VARCHAR(16)   NOT NULL,
    trade_date DATE          NOT NULL,
    adj_factor NUMERIC(18,6) NOT NULL,
    PRIMARY KEY (stock_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_us_adj_factor_date
    ON us_adj_factor (trade_date, stock_code);

CREATE TABLE IF NOT EXISTS us_index_daily (
    index_code VARCHAR(16) NOT NULL,   -- IXIC / INX ...
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

-- us_adj_factor 是稀疏表(仅除权除息日有行),同 A 股改为前向填充(见
-- 01_schema.sql 顶部说明),查不到因子记为 1。
CREATE OR REPLACE VIEW us_daily_price_hfq AS
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * f.factor, 3) AS open,
    round(d.high  * f.factor, 3) AS high,
    round(d.low   * f.factor, 3) AS low,
    round(d.close * f.factor, 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM us_daily_price d
LEFT JOIN LATERAL (
    SELECT a.adj_factor AS factor FROM us_adj_factor a
    WHERE a.stock_code = d.stock_code AND a.trade_date <= d.trade_date
    ORDER BY a.trade_date DESC LIMIT 1
) f0 ON true
CROSS JOIN LATERAL (SELECT coalesce(f0.factor, 1) AS factor) f;

CREATE OR REPLACE VIEW us_daily_price_qfq AS
WITH latest AS (
    SELECT DISTINCT ON (stock_code) stock_code, adj_factor AS f
    FROM us_adj_factor ORDER BY stock_code, trade_date DESC
)
SELECT
    d.stock_code, d.trade_date,
    round(d.open  * f.factor / coalesce(l.f, 1), 3) AS open,
    round(d.high  * f.factor / coalesce(l.f, 1), 3) AS high,
    round(d.low   * f.factor / coalesce(l.f, 1), 3) AS low,
    round(d.close * f.factor / coalesce(l.f, 1), 3) AS close,
    d.volume, d.amount, d.pct_chg, d.turnover
FROM us_daily_price d
LEFT JOIN LATERAL (
    SELECT a.adj_factor AS factor FROM us_adj_factor a
    WHERE a.stock_code = d.stock_code AND a.trade_date <= d.trade_date
    ORDER BY a.trade_date DESC LIMIT 1
) f0 ON true
CROSS JOIN LATERAL (SELECT coalesce(f0.factor, 1) AS factor) f
LEFT JOIN latest l ON l.stock_code = d.stock_code;

CREATE MATERIALIZED VIEW IF NOT EXISTS us_weekly_price_hfq AS
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
FROM us_daily_price_hfq
GROUP BY stock_code, date_trunc('week', trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_us_weekly_hfq_pk
    ON us_weekly_price_hfq (stock_code, period_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS us_monthly_price_hfq AS
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
FROM us_daily_price_hfq
GROUP BY stock_code, date_trunc('month', trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_us_monthly_hfq_pk
    ON us_monthly_price_hfq (stock_code, period_start);

-- ---------------------------------------------------------------------------
-- 兼容性修正
-- ---------------------------------------------------------------------------
-- etl_progress 复用于港/美股断点续传;美股代码最长可到 16 位(如 GOOGL.US),
-- 原 VARCHAR(12) 不够,统一放宽到 VARCHAR(16)。
ALTER TABLE etl_progress ALTER COLUMN stock_code TYPE VARCHAR(16);

-- 补齐与 01_schema.sql 同构的 is_active 索引
CREATE INDEX IF NOT EXISTS idx_hk_stock_basic_active ON hk_stock_basic (is_active);
CREATE INDEX IF NOT EXISTS idx_us_stock_basic_active ON us_stock_basic (is_active);
