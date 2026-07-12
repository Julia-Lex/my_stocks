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

-- 定时验证任务:报告阅读后的未来校验点,一条待办可挂多个。
-- due_date 到期未完成的在页面高亮;验证完成后同样可挂报告。
CREATE TABLE IF NOT EXISTS todo_schedule (
    id         BIGSERIAL PRIMARY KEY,
    todo_id    BIGINT      NOT NULL REFERENCES todo(id) ON DELETE CASCADE,
    content    TEXT        NOT NULL,
    due_at     TIMESTAMPTZ NOT NULL,       -- 到期时刻(日期+时间)
    done       BOOLEAN     NOT NULL DEFAULT FALSE,
    done_at    TIMESTAMPTZ,
    report     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_todo_schedule_todo ON todo_schedule (todo_id, due_at);

-- 迁移:早期版本 due_date DATE → due_at TIMESTAMPTZ(幂等)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'todo_schedule' AND column_name = 'due_date') THEN
    ALTER TABLE todo_schedule RENAME COLUMN due_date TO due_at;
    ALTER TABLE todo_schedule ALTER COLUMN due_at TYPE TIMESTAMPTZ USING due_at::timestamptz;
  END IF;
END $$;
