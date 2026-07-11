-- =============================================================================
-- 港美板块层(富途源)。设计: docs/superpowers/specs/2026-07-11-board-intl-design.md
-- 每市场一张表(方案B);成分区间语义同 index_member;A股板块见 11_schema_board.sql(东财)。
-- 用法: psql -d astock -f 18_schema_board_intl.sql
-- =============================================================================
CREATE TABLE IF NOT EXISTS hk_board (
    board_code VARCHAR(24) PRIMARY KEY,   -- 富途 plate 码,如 HK.BK1001
    board_name VARCHAR(64) NOT NULL,
    board_type VARCHAR(12) NOT NULL,      -- industry / concept
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS hk_board_member (
    board_code VARCHAR(24) NOT NULL,
    stock_code VARCHAR(12) NOT NULL,
    in_date    DATE        NOT NULL,
    out_date   DATE,                      -- NULL=在册
    note       VARCHAR(32),               -- snapshot-open / diff
    PRIMARY KEY (board_code, stock_code, in_date)
);
CREATE INDEX IF NOT EXISTS idx_hk_board_member_stock ON hk_board_member (stock_code);

CREATE TABLE IF NOT EXISTS us_board (
    board_code VARCHAR(24) PRIMARY KEY,
    board_name VARCHAR(64) NOT NULL,
    board_type VARCHAR(12) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS us_board_member (
    board_code VARCHAR(24) NOT NULL,
    stock_code VARCHAR(16) NOT NULL,
    in_date    DATE        NOT NULL,
    out_date   DATE,
    note       VARCHAR(32),
    PRIMARY KEY (board_code, stock_code, in_date)
);
CREATE INDEX IF NOT EXISTS idx_us_board_member_stock ON us_board_member (stock_code);
