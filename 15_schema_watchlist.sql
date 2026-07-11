-- =============================================================================
-- 自选股(webapp 应用数据,非行情数据层)。支持分组与手工排序。
-- 幂等:全部 IF NOT EXISTS / ADD COLUMN IF NOT EXISTS,可重复执行。
-- 用法: psql -d astock -f 15_schema_watchlist.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS watchlist (
    market     VARCHAR(4)  NOT NULL,        -- cn | hk | us
    stock_code VARCHAR(16) NOT NULL,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, stock_code)
);

CREATE TABLE IF NOT EXISTS watchlist_group (
    name       VARCHAR(32) PRIMARY KEY,
    sort_order INT NOT NULL DEFAULT 0
);
INSERT INTO watchlist_group (name, sort_order) VALUES ('默认分组', 0)
ON CONFLICT DO NOTHING;

ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS grp VARCHAR(32) NOT NULL DEFAULT '默认分组';
ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS sort_order INT NOT NULL DEFAULT 0;
