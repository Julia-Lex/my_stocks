-- 29_schema_holders.sql — 股权结构层(控盘度 / 机构散户占比 / 股东户数)。
-- 移交清单 #8:share_capital.float_shares 只是无限售流通股,缺十大股东/流通股东、
-- 实控人持股、股东户数、机构类型 —— 算不了控盘度与持有人结构。本层补齐。
-- 源:东财 datacenter f10(stock_gdfx_top_10_em / _free_top_10_em / stock_zh_a_gdhs_detail_em),
-- 不在东财行情族封禁范围。应用:psql -U zhu -d astock -f 29_schema_holders.sql(幂等)

-- 十大股东(total=占总股本,算控盘度/实控人)+ 十大流通股东(float=占流通,含股东性质,算机构占比)
CREATE TABLE IF NOT EXISTS top10_holder (
    stock_code    TEXT NOT NULL,
    report_date   DATE NOT NULL,          -- 报告期(季末)
    holder_type   TEXT NOT NULL CHECK (holder_type IN ('total', 'float')),
    rank          INT  NOT NULL,          -- 名次 1..10
    holder_name   TEXT NOT NULL,
    holder_nature TEXT,                   -- 股东性质(证券投资基金/私募基金/QFII/险资/证券公司/投资公司/其它…;仅 float 口径有)
    share_type    TEXT,                   -- 股份类型
    hold_shares   BIGINT,                 -- 持股数(股)
    hold_ratio    NUMERIC(10,4),          -- 占比(%):total=占总股本,float=占流通
    change_flag   TEXT,                   -- 增减(增/减/不变/新进)
    PRIMARY KEY (stock_code, report_date, holder_type, rank)
);
CREATE INDEX IF NOT EXISTS idx_top10_stock  ON top10_holder (stock_code, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_top10_nature ON top10_holder (holder_nature);

-- 股东户数(算散户户数/户均持股/分散度)
CREATE TABLE IF NOT EXISTS shareholder_count (
    stock_code      TEXT NOT NULL,
    report_date     DATE NOT NULL,        -- 股东户数统计截止日
    holder_num      INT,                  -- 股东户数
    avg_hold_shares NUMERIC(20,2),        -- 户均持股数量(股)
    avg_hold_value  NUMERIC(20,2),        -- 户均持股市值(元)
    total_shares    BIGINT,               -- 总股本
    ann_date        DATE,                 -- 股东户数公告日期
    PRIMARY KEY (stock_code, report_date)
);
CREATE INDEX IF NOT EXISTS idx_gdhs_stock ON shareholder_count (stock_code, report_date DESC);
