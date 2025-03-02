-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db

SET search_path TO texera_db;

-- ============================================
-- 2. Update the table schema
-- ============================================

BEGIN;

-- 1. Rename original table temporarily
ALTER TABLE operator_port_executions RENAME TO operator_port_executions_old;

-- 2. Create the new table with columns in the correct order
CREATE TABLE operator_port_executions
(
    workflow_execution_id INT NOT NULL,
    operator_id           VARCHAR(100) NOT NULL,
    layer_name            VARCHAR(100) NOT NULL DEFAULT 'main',
    port_id               INT NOT NULL,
    result_uri            TEXT,
    PRIMARY KEY (workflow_execution_id, operator_id, layer_name, port_id),
    FOREIGN KEY (workflow_execution_id) REFERENCES workflow_executions(eid) ON DELETE CASCADE
);

-- 3. Copy data from old table (use the default value for `layer_id`)
INSERT INTO operator_port_executions (workflow_execution_id, operator_id, port_id, result_uri)
SELECT workflow_execution_id, operator_id, port_id, result_uri
FROM operator_port_executions_old;

-- 4. Drop the old table after copying data
DROP TABLE operator_port_executions_old;

COMMIT;