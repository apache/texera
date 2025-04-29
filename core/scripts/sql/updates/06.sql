\c texera_db

SET search_path TO texera_db;

-- 为 workflow_executions 添加 runtime_stats_size 字段
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db'
          AND table_name = 'workflow_executions'
          AND column_name = 'runtime_stats_size'
    ) THEN
        ALTER TABLE workflow_executions
            ADD COLUMN runtime_stats_size INT DEFAULT 0;
    END IF;
END
$$;

-- 为 operator_executions 添加 console_messages_size 字段
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db'
          AND table_name = 'operator_executions'
          AND column_name = 'console_messages_size'
    ) THEN
        ALTER TABLE operator_executions
            ADD COLUMN console_messages_size INT DEFAULT 0;
    END IF;
END
$$;

-- 为 operator_port_executions 添加 result_size 字段
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db'
          AND table_name = 'operator_port_executions'
          AND column_name = 'result_size'
    ) THEN
        ALTER TABLE operator_port_executions
            ADD COLUMN result_size INT DEFAULT 0;
    END IF;
END
$$;
