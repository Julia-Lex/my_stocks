-- =============================================================================
-- 港股/美股基本面(三期)· 富途主源。设计: docs/superpowers/specs/2026-07-10-hkus-fundamental-design.md
-- 两层架构(本期裁定):JSONB 报表 + 指标宽表;无股本/估值层。
-- ann_date 宁缺勿假:NULL 行在 as-of 中不可见。currency 如实存不换算。
-- 用法: psql -d astock -f 11_schema_fundamental_intl.sql
-- =============================================================================

-- ======================== 港股基本面表 ========================
CREATE TABLE IF NOT EXISTS hk_fin_statement (
    stock_code  VARCHAR(12) NOT NULL,
    report_date DATE        NOT NULL,
    stmt_type   VARCHAR(8)  NOT NULL,          -- income / balance / cashflow
    ann_date    DATE,
    currency    VARCHAR(8),                    -- 富途 currency_code(HKD/CNY/USD...)
    data        JSONB       NOT NULL,          -- {display_name: data},科目名保留源中文
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date, stmt_type)
);
CREATE INDEX IF NOT EXISTS idx_hk_fin_statement_period ON hk_fin_statement (report_date, stock_code);

CREATE TABLE IF NOT EXISTS hk_fin_indicator (
    stock_code      VARCHAR(12)  NOT NULL,
    report_date     DATE         NOT NULL,
    ann_date        DATE,
    currency        VARCHAR(8),
    eps             NUMERIC(12,4),
    eps_diluted     NUMERIC(12,4),
    bps             NUMERIC(12,4),
    ocf_ps          NUMERIC(12,4),
    roe             NUMERIC(10,4),
    roa             NUMERIC(10,4),
    gross_margin    NUMERIC(10,4),
    net_margin      NUMERIC(10,4),
    debt_ratio      NUMERIC(10,4),
    current_ratio   NUMERIC(10,4),
    revenue         NUMERIC(20,2),
    revenue_yoy     NUMERIC(10,4),
    net_profit      NUMERIC(20,2),
    net_profit_yoy  NUMERIC(10,4),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date)
);
CREATE INDEX IF NOT EXISTS idx_hk_fin_indicator_period ON hk_fin_indicator (report_date, stock_code);
CREATE INDEX IF NOT EXISTS idx_hk_fin_indicator_ann ON hk_fin_indicator (stock_code, ann_date);

CREATE OR REPLACE FUNCTION hk_fin_asof(p_stock VARCHAR, p_date DATE)
RETURNS SETOF hk_fin_indicator AS $$
    SELECT * FROM hk_fin_indicator
    WHERE stock_code = p_stock AND ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY report_date DESC LIMIT 1;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION hk_fin_asof_all(p_date DATE)
RETURNS SETOF hk_fin_indicator AS $$
    SELECT DISTINCT ON (stock_code) * FROM hk_fin_indicator
    WHERE ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY stock_code, report_date DESC;
$$ LANGUAGE sql STABLE;

-- ======================== 美股基本面表 ========================
CREATE TABLE IF NOT EXISTS us_fin_statement (
    stock_code  VARCHAR(16) NOT NULL,
    report_date DATE        NOT NULL,
    stmt_type   VARCHAR(8)  NOT NULL,          -- income / balance / cashflow
    ann_date    DATE,
    currency    VARCHAR(8),                    -- 富途 currency_code(HKD/CNY/USD...)
    data        JSONB       NOT NULL,          -- {display_name: data},科目名保留源中文
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date, stmt_type)
);
CREATE INDEX IF NOT EXISTS idx_us_fin_statement_period ON us_fin_statement (report_date, stock_code);

CREATE TABLE IF NOT EXISTS us_fin_indicator (
    stock_code      VARCHAR(16)  NOT NULL,
    report_date     DATE         NOT NULL,
    ann_date        DATE,
    currency        VARCHAR(8),
    eps             NUMERIC(12,4),
    eps_diluted     NUMERIC(12,4),
    bps             NUMERIC(12,4),
    ocf_ps          NUMERIC(12,4),
    roe             NUMERIC(10,4),
    roa             NUMERIC(10,4),
    gross_margin    NUMERIC(10,4),
    net_margin      NUMERIC(10,4),
    debt_ratio      NUMERIC(10,4),
    current_ratio   NUMERIC(10,4),
    revenue         NUMERIC(20,2),
    revenue_yoy     NUMERIC(10,4),
    net_profit      NUMERIC(20,2),
    net_profit_yoy  NUMERIC(10,4),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date)
);
CREATE INDEX IF NOT EXISTS idx_us_fin_indicator_period ON us_fin_indicator (report_date, stock_code);
CREATE INDEX IF NOT EXISTS idx_us_fin_indicator_ann ON us_fin_indicator (stock_code, ann_date);

CREATE OR REPLACE FUNCTION us_fin_asof(p_stock VARCHAR, p_date DATE)
RETURNS SETOF us_fin_indicator AS $$
    SELECT * FROM us_fin_indicator
    WHERE stock_code = p_stock AND ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY report_date DESC LIMIT 1;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION us_fin_asof_all(p_date DATE)
RETURNS SETOF us_fin_indicator AS $$
    SELECT DISTINCT ON (stock_code) * FROM us_fin_indicator
    WHERE ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY stock_code, report_date DESC;
$$ LANGUAGE sql STABLE;
