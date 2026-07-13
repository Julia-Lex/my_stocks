-- 27_schema_announcement.sql — A股公告流(发布时间到秒)。
-- 设计:与 fin_forecast/express/lhb 不同——那些是解析后的结构化数据(数字/榜单,按报告期/交易日,
-- 无发布时分);本表是原始公告披露流,一条公告一行,自带精确发布时间与全部披露类型。
-- 源:东财公告 API(np-anotice-stock,datacenter 族,不在行情族封禁范围)。
-- 应用:psql -U zhu -d astock -f 27_schema_announcement.sql(幂等)

CREATE TABLE IF NOT EXISTS announcement (
    art_code     TEXT PRIMARY KEY,           -- 东财公告唯一 ID(去重键,如 AN202607131826937264)
    stock_code   TEXT NOT NULL,              -- 关联个股(首个 A 股代码,带交易所后缀)
    title        TEXT NOT NULL,              -- 公告标题
    category     TEXT,                       -- 主类型(首个 column_name,展示用)
    categories   TEXT[],                     -- 全部类型标签(一条公告可多标签;按类型筛用 '业绩预告' = ANY(categories))
    publish_time TIMESTAMP NOT NULL,         -- 发布时间(到秒,东财 display_time,北京时间)
    url          TEXT,                       -- 公告详情页
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ann_stock ON announcement (stock_code, publish_time DESC);
CREATE INDEX IF NOT EXISTS idx_ann_time  ON announcement (publish_time DESC);
CREATE INDEX IF NOT EXISTS idx_ann_cat   ON announcement USING GIN (categories);
