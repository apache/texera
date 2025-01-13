USE texera_db;
DROP TABLE IF EXISTS operator_runtime_statistics;
DROP TABLE IF EXISTS operator_executions;

CREATE TABLE IF NOT EXISTS operator_executions (
    operator_execution_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    workflow_execution_id INT UNSIGNED NOT NULL, 
    operator_id VARCHAR(100) NOT NULL, 
    UNIQUE (workflow_execution_id, operator_id),
    FOREIGN KEY (workflow_execution_id) REFERENCES workflow_executions (eid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS operator_runtime_statistics (
    operator_execution_id BIGINT UNSIGNED NOT NULL, 
    time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    input_tuple_cnt BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    output_tuple_cnt BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    status TINYINT NOT NULL DEFAULT 1, 
    data_processing_time BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    control_processing_time BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    idle_time BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    num_workers INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (operator_execution_id, time),
    FOREIGN KEY (operator_execution_id) REFERENCES operator_executions (operator_execution_id) ON DELETE CASCADE
);

INSERT IGNORE INTO operator_executions (workflow_execution_id, operator_id)
SELECT DISTINCT execution_id, operator_id
FROM workflow_runtime_statistics;

INSERT INTO operator_runtime_statistics (
    operator_execution_id, 
    time, 
    input_tuple_cnt, 
    output_tuple_cnt, 
    status, 
    data_processing_time, 
    control_processing_time, 
    idle_time,
    num_workers
)
SELECT 
    oe.operator_execution_id, 
    wrs.time, 
    wrs.input_tuple_cnt, 
    wrs.output_tuple_cnt, 
    wrs.status, 
    wrs.data_processing_time, 
    wrs.control_processing_time, 
    wrs.idle_time,
    wrs.num_workers 
FROM workflow_runtime_statistics wrs
JOIN operator_executions oe ON wrs.execution_id = oe.workflow_execution_id AND wrs.operator_id = oe.operator_id;

drop table if exists workflow_runtime_statistics;
