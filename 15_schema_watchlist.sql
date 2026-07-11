-- =============================================================================
-- 自选股(webapp 应用数据,非行情数据层)。
-- 用法: psql -d astock -f 15_schema_watchlist.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS watchlist (
    market     VARCHAR(4)  NOT NULL,        -- cn | hk | us
    stock_code VARCHAR(16) NOT NULL,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, stock_code)
);
