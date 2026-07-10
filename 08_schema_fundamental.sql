-- =============================================================================
-- A股基本面数据层(二期)· 设计见 docs/superpowers/specs/2026-07-10-ashare-fundamental-design.md
-- 核心原则:全表带 ann_date(公告日);回测取数唯一入口是 fin_asof(防未来函数)。
-- 已知限制:免费源无财报修订历史,ann_date 防"提前看"不防"事后修正"。
-- 用法: psql -d astock -f 08_schema_fundamental.sql
-- =============================================================================

-- 幂等加宽:etl_progress.stock_code 原在 01_schema.sql 里定义为 VARCHAR(12)
-- (够装 '000001.SZ' 这类股票代码),但本二期的核查节奏借用同一列存哨兵 key
-- 'YYYYMMDD:kind'(如 '20260630:yjbb',共 13 字符),超出 VARCHAR(12)。若新装
-- 环境只跑 01_schema.sql + 08_schema_fundamental.sql(不经过后续手工修复),
-- 阶段1 截面核查写 etl_progress 会报 "value too long for type character
-- varying(12)"。这里加宽到 16(留出余量),放在 08 开头以便新旧环境重放
-- 本文件都能补齐;ALTER COLUMN TYPE 到相同或更宽的 VARCHAR 是幂等操作,列已是
-- VARCHAR(16)(或更宽)时重放不报错、不改变数据。
ALTER TABLE etl_progress ALTER COLUMN stock_code TYPE VARCHAR(16);

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
