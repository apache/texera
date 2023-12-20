USE `texera_db`;
CREATE TABLE IF NOT EXISTS workflow_runtime_statistics
(
    `workflow_id`      INT UNSIGNED             NOT NULL,
    `execution_id`     INT UNSIGNED             NOT NULL,
    `operator_id`      VARCHAR(100)             NOT NULL,
    `time`             TIMESTAMP(6)             NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `input_tuple_cnt`  INT UNSIGNED             NOT NULL DEFAULT 0,
    `output_tuple_cnt` INT UNSIGNED             NOT NULL DEFAULT 0,
    `status`           TINYINT                  NOT NULL DEFAULT 1,
    PRIMARY KEY (`workflow_id`, `execution_id`, `operator_id`, `time`),
    FOREIGN KEY (`workflow_id`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE,
    FOREIGN KEY (`execution_id`) REFERENCES `workflow_executions` (`eid`) ON DELETE CASCADE
) ENGINE = INNODB;
