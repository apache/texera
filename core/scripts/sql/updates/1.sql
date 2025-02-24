-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db

-- Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS texera_db;
SET search_path TO texera_db;

-- ============================================
-- 2. Update the table schema
-- ============================================

-- Step 1: Drop the existing primary key
ALTER TABLE operator_port_executions
    DROP CONSTRAINT operator_port_executions_pkey;

-- Step 2: Add the new column `layer_id`
ALTER TABLE operator_port_executions
    ADD COLUMN layer_id VARCHAR(100) NOT NULL DEFAULT 'main';

-- Step 3: Recreate the primary key with `layer_id` included
ALTER TABLE operator_port_executions
    ADD CONSTRAINT operator_port_executions_pkey
        PRIMARY KEY (workflow_execution_id, operator_id, layer_id, port_id);