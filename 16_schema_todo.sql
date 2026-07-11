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
