-- =============================================================================
-- 港美指数成分区间表。设计: docs/superpowers/specs/2026-07-11-index-member-intl-design.md
-- 区间语义:任意日成分 = in_date <= d AND (out_date IS NULL OR out_date > d)。
-- A股成分归另一数据层(board_member / 另行建设),本表仅港美指数。
-- 用法: psql -d astock -f 16_schema_index_member.sql
-- =============================================================================
CREATE TABLE IF NOT EXISTS index_member (
    index_code VARCHAR(16) NOT NULL,   -- HSI / HSTECH / SPX / NDX ...
    stock_code VARCHAR(16) NOT NULL,   -- 00700.HK / AAPL.US
    in_date    DATE        NOT NULL,   -- 纳入日(快照启用日或真实变更日,见 note)
    out_date   DATE,                   -- 剔除日;NULL=在册
    note       VARCHAR(64),            -- 'snapshot-open'=启用日快照(非真实纳入日)/ 'history'=真实变更
    PRIMARY KEY (index_code, stock_code, in_date)
);
CREATE INDEX IF NOT EXISTS idx_index_member_stock ON index_member (stock_code, index_code);
