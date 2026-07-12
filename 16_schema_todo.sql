-- =============================================================================
-- 待办事项(webapp 应用数据):记录后续要做的分析想法。
-- 用法: psql -d astock -f 16_schema_todo.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS todo (
    id         BIGSERIAL PRIMARY KEY,
    content    TEXT        NOT NULL,
    done       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at    TIMESTAMPTZ
);

-- 关联分析报告:docs/analysis/ 下的文件名(webapp 经 /reports/{name} 提供)
ALTER TABLE todo ADD COLUMN IF NOT EXISTS report TEXT;
