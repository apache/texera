use texera_db;

drop table if exists operator_runtime_statistics;

alter table workflow_executions add column runtime_stats_uri text default null;
alter table operator_executions drop column operator_execution_id, add column console_messages_uri text default null;

CREATE TABLE IF NOT EXISTS operator_port_executions
(
    workflow_execution_id INT UNSIGNED NOT NULL,
    operator_id VARCHAR(100) NOT NULL,
    port_id INT NOT NULL,
    result_uri TEXT DEFAULT NULL,
    UNIQUE (workflow_execution_id, operator_id, port_id),
    FOREIGN KEY (workflow_execution_id) REFERENCES workflow_executions (eid) ON DELETE CASCADE
);
