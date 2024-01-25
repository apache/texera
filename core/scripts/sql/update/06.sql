USE `texera_db`;
ALTER TABLE workflow_runtime_statistics
ADD ( 
`data_processing_time` INT UNSIGNED         NOT NULL DEFAULT 0,
`control_processing_time` INT UNSIGNED      NOT NULL DEFAULT 0,
`idle_time`        INT UNSIGNED             NOT NULL DEFAULT 0,
`num_workers`      INT UNSIGNED             NOT NULL DEFAULT 0
)
