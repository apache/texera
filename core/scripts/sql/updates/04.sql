-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db

SET search_path TO texera_db;

-- ============================================
-- 2. Update the table schema
-- ============================================

BEGIN;


CREATE TABLE IF NOT EXISTS workflow_computing_unit
(
    uid                INT           NOT NULL,
    name               VARCHAR(128)  NOT NULL,
    cuid               SERIAL PRIMARY KEY,
    creation_time      TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    terminate_time     TIMESTAMP  DEFAULT NULL,
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE
);


COMMIT;