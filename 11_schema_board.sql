-- 11_schema_board.sql — 板块数据层(行业/概念)。
-- 设计: docs/superpowers/specs/2026-07-10-board-rotation-design.md
-- 应用: psql -U zhu -d astock -f 11_schema_board.sql(幂等)

CREATE TABLE IF NOT EXISTS board (
    board_code  TEXT PRIMARY KEY,          -- 东财代码,如 BK0475
    board_name  TEXT NOT NULL,             -- 最新名称(改名时更新)
    board_type  TEXT NOT NULL CHECK (board_type IN ('industry', 'concept')),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,  -- 从东财列表消失置 false,历史数据保留
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 成分区间表:valid_from 是"观测到纳入"的日期(首次建库日=观测起点,非真实纳入日),
-- valid_to NULL 表示当前仍在板块内;精度=每日快照粒度。
-- 某日 d 的成分: valid_from <= d AND (valid_to IS NULL OR valid_to > d)
CREATE TABLE IF NOT EXISTS board_member (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    stock_code  TEXT NOT NULL,
    valid_from  DATE NOT NULL,
    valid_to    DATE,
    PRIMARY KEY (board_code, stock_code, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_board_member_stock ON board_member (stock_code);
CREATE INDEX IF NOT EXISTS idx_board_member_open  ON board_member (board_code) WHERE valid_to IS NULL;

CREATE TABLE IF NOT EXISTS board_daily (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    trade_date  DATE NOT NULL,
    open  NUMERIC(12,3), high NUMERIC(12,3), low NUMERIC(12,3), close NUMERIC(12,3),
    volume BIGINT,                          -- 股(源为手,入库 ×100)
    amount NUMERIC(20,2),                   -- 元
    pct_chg  NUMERIC(8,4),
    turnover NUMERIC(8,4),
    PRIMARY KEY (board_code, trade_date)
);

CREATE TABLE IF NOT EXISTS board_fund_flow (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    trade_date  DATE NOT NULL,
    main_net   NUMERIC(20,2), main_net_pct   NUMERIC(8,4),  -- 主力净流入 额(元)/占比(%)
    xlarge_net NUMERIC(20,2), xlarge_net_pct NUMERIC(8,4),  -- 超大单
    large_net  NUMERIC(20,2), large_net_pct  NUMERIC(8,4),  -- 大单
    mid_net    NUMERIC(20,2), mid_net_pct    NUMERIC(8,4),  -- 中单
    small_net  NUMERIC(20,2), small_net_pct  NUMERIC(8,4),  -- 小单
    PRIMARY KEY (board_code, trade_date)
);
