-- =============================================================================
-- C 补全包:事件类数据(业绩预告/快报、龙虎榜、北向持股)+ 股票域 alias 治理
-- 设计: docs/superpowers/specs/2026-07-11-events-pack-design.md
-- 预告/快报自带公告日 ann_date,防未来查询直接 WHERE ann_date <= d(表小不建 asof 函数)。
-- 用法: psql -d astock -f 14_schema_events.sql
-- =============================================================================

-- 业绩预告(东财 stock_yjyg_em 按报告期截面;同期同股可有多个预测指标行)
CREATE TABLE IF NOT EXISTS fin_forecast (
    stock_code    VARCHAR(12)  NOT NULL,
    report_date   DATE         NOT NULL,
    forecast_type VARCHAR(64)  NOT NULL,       -- 预测指标(部分值超32字符)(如 归母净利润/扣非净利润)
    ann_date      DATE,                        -- 公告日期
    change_desc   TEXT,                        -- 业绩变动(整句描述,实测最长109+字符)(预增/预减/扭亏...)
    forecast_value NUMERIC(20,2),              -- 预测数值(区间/定性预告可为 NULL)
    change_pct    NUMERIC(12,4),               -- 业绩变动幅度 %
    reason        TEXT,                        -- 变动原因
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date, forecast_type)
);
CREATE INDEX IF NOT EXISTS idx_fin_forecast_period ON fin_forecast (report_date, stock_code);
CREATE INDEX IF NOT EXISTS idx_fin_forecast_ann ON fin_forecast (stock_code, ann_date);

-- 业绩快报(东财 stock_yjkb_em 按报告期截面;数值列按接口实际列映射,实施探测后定)
CREATE TABLE IF NOT EXISTS fin_express (
    stock_code   VARCHAR(12)  NOT NULL,
    report_date  DATE         NOT NULL,
    ann_date     DATE,
    eps          NUMERIC(12,4),
    revenue      NUMERIC(20,2),
    revenue_yoy  NUMERIC(12,4),
    net_profit   NUMERIC(20,2),
    net_profit_yoy NUMERIC(12,4),
    bps          NUMERIC(12,4),
    roe          NUMERIC(10,4),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date)
);
CREATE INDEX IF NOT EXISTS idx_fin_express_period ON fin_express (report_date, stock_code);
CREATE INDEX IF NOT EXISTS idx_fin_express_ann ON fin_express (stock_code, ann_date);

-- 龙虎榜明细(东财 stock_lhb_detail_em;同日同股可因多个原因上榜)
CREATE TABLE IF NOT EXISTS lhb_detail (
    stock_code  VARCHAR(12)  NOT NULL,
    trade_date  DATE         NOT NULL,
    reason      VARCHAR(128) NOT NULL,          -- 上榜原因
    close       NUMERIC(12,3),
    pct_chg     NUMERIC(10,4),
    net_buy     NUMERIC(20,2),                  -- 龙虎榜净买额(元)
    buy_amount  NUMERIC(20,2),
    sell_amount NUMERIC(20,2),
    interpret   VARCHAR(256),                   -- 解读(东财口径)
    PRIMARY KEY (stock_code, trade_date, reason)
);
CREATE INDEX IF NOT EXISTS idx_lhb_detail_date ON lhb_detail (trade_date, stock_code);

-- 北向持股(列按实施探测所及;若免费源无个股序列则本表缓建,见 README)
CREATE TABLE IF NOT EXISTS nb_hold (
    stock_code  VARCHAR(12) NOT NULL,
    trade_date  DATE        NOT NULL,
    hold_shares BIGINT,                         -- 持股数量(股)
    hold_value  NUMERIC(20,2),                  -- 持股市值(元)
    hold_ratio  NUMERIC(10,4),                  -- 占流通股比 %(源提供则存)
    PRIMARY KEY (stock_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_nb_hold_date ON nb_hold (trade_date, stock_code);

-- 股票域 alias(改码股治理:用 new 码拉数、按 old 码入库,保历史连续;人工维护)
CREATE TABLE IF NOT EXISTS stock_alias (
    old_code   VARCHAR(16) PRIMARY KEY,
    new_code   VARCHAR(16) NOT NULL,
    new_symbol VARCHAR(12) NOT NULL,
    note       TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO stock_alias (old_code, new_code, new_symbol, note)
VALUES ('BGNE.US', 'ONC.US', 'ONC', '2025 改名 BeOne Medicines,代码 BGNE→ONC')
ON CONFLICT (old_code) DO NOTHING;
