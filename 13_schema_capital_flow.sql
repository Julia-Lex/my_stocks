-- =============================================================================
-- 个股资金流(富途 get_capital_flow,日级)。
-- 背景:板块资金流只有东财板块级接口(被封),改为富途个股级入库、板块按
-- 现役成员聚合(口径与 board/board_member 的富途板块天然一致,零名称映射)。
-- 限制:富途日级资金流历史仅滚动一年,靠日增量向后积累;金额单位:元。
-- 目前仅 A股(板块资金流需求所在);港美股如需可扩(表结构市场无关)。
-- 用法: psql -d astock -f 13_schema_capital_flow.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS capital_flow (
    stock_code VARCHAR(12) NOT NULL,        -- 300308.SZ
    trade_date DATE        NOT NULL,
    main_net   NUMERIC(20,2),               -- 主力净流入(=超大+大单)
    super_net  NUMERIC(20,2),               -- 超大单净流入
    big_net    NUMERIC(20,2),               -- 大单净流入
    mid_net    NUMERIC(20,2),               -- 中单净流入
    sml_net    NUMERIC(20,2),               -- 小单净流入
    total_net  NUMERIC(20,2),               -- 整体净流入(富途 in_flow)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_capital_flow_date ON capital_flow (trade_date, stock_code);
