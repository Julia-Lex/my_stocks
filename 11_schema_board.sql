-- 11_schema_board.sql — 板块数据层(行业/概念)。
-- 设计: docs/superpowers/specs/2026-07-10-board-rotation-design.md
-- 应用: psql -U zhu -d astock -f 11_schema_board.sql(幂等)

CREATE TABLE IF NOT EXISTS board (
    board_code  TEXT PRIMARY KEY,          -- 东财 'BK0475' / 富途 'SH.LIST0001',天然不同名字空间
    board_name  TEXT NOT NULL,             -- 最新名称(改名时更新)
    board_type  TEXT NOT NULL CHECK (board_type IN ('industry', 'concept')),
    source      TEXT NOT NULL DEFAULT 'em' CHECK (source IN ('em', 'futu')),  -- 板块体系口径
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,  -- 从该源列表消失置 false,历史数据保留
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 已建库的存量升级(幂等)
ALTER TABLE board ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'em'
    CHECK (source IN ('em', 'futu'));

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
    open  NUMERIC(18,3), high NUMERIC(18,3), low NUMERIC(18,3), close NUMERIC(18,3),  -- 富途个别特殊板块点位超 10^9,放宽
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

-- 派生:板块资金流聚合(个股 capital_flow × 当前成分求和;富途/东财口径板块通用)。
-- 注意:用"当前成分"回算全部历史(与板块指数回算同一近似)——观测起点前的真实成分不可知。
-- 每日由 21_board_update.py 末尾 REFRESH。
CREATE MATERIALIZED VIEW IF NOT EXISTS board_capital_flow AS
SELECT m.board_code, cf.trade_date,
       sum(cf.main_net)  AS main_net,
       sum(cf.super_net) AS super_net,
       sum(cf.big_net)   AS big_net,
       sum(cf.mid_net)   AS mid_net,
       sum(cf.sml_net)   AS sml_net,
       count(*)          AS n_members
FROM capital_flow cf
JOIN board_member m ON m.stock_code = cf.stock_code AND m.valid_to IS NULL
GROUP BY m.board_code, cf.trade_date;
CREATE UNIQUE INDEX IF NOT EXISTS idx_bcf_pk ON board_capital_flow (board_code, trade_date);
