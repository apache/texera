-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db

SET search_path TO texera_db;

-- ============================================
-- 2. Update the table schema
-- ============================================

BEGIN;

-- 1. Drop the existing table
DROP TABLE IF EXISTS operator_port_executions;

-- 2. Create the new table with updated columns
CREATE TABLE operator_port_executions
(
    workflow_execution_id INT NOT NULL,
    global_port_id        VARCHAR(200) NOT NULL,
    result_uri            TEXT,
    PRIMARY KEY (workflow_execution_id, global_port_id),
    FOREIGN KEY (workflow_execution_id) REFERENCES workflow_executions(eid) ON DELETE CASCADE
);

COMMIT;