-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db

SET search_path TO texera_db;

-- ============================================
-- 2. Update the table schema
-- ============================================

BEGIN;

DROP TABLE IF EXISTS cluster_activity;
DROP TABLE IF EXISTS cluster;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cluster_status') THEN
        CREATE TYPE cluster_status AS ENUM (
            'LAUNCH_RECEIVED',
            'PENDING',
            'RUNNING',
            'TERMINATE_RECEIVED',
            'SHUTTING_DOWN',
            'TERMINATED',
            'STOP_RECEIVED',
            'STOPPING',
            'STOPPED',
            'START_RECEIVED',
            'LAUNCH_FAILED',
            'TERMINATE_FAILED',
            'STOP_FAILED',
            'START_FAILED'
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS cluster (
    cid SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    owner_id INTEGER NOT NULL,
    machine_type VARCHAR(255) NOT NULL,
    number_of_machines INTEGER NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status cluster_status,
    FOREIGN KEY (owner_id) REFERENCES "user" (uid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cluster_activity (
    id SERIAL PRIMARY KEY,
    cluster_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NULL,
    FOREIGN KEY (cluster_id) REFERENCES cluster (cid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS cluster_activity_cluster_id_start_time_idx
ON cluster_activity (cluster_id, start_time);

COMMIT;
