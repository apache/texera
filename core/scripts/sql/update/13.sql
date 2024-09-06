use `texera_db`;

ALTER TABLE `environment_of_workflow`
    DROP FOREIGN KEY `environment_of_workflow_ibfk_2`;

ALTER TABLE `dataset_of_environment`
    DROP FOREIGN KEY `dataset_of_environment_ibfk_1`;

ALTER TABLE `dataset_of_environment`
    DROP FOREIGN KEY `dataset_of_environment_ibfk_2`;

ALTER TABLE `workflow_executions`
    DROP FOREIGN KEY `workflow_executions_ibfk_3`;
-- Dropping the dependent tables
DROP TABLE IF EXISTS `environment_of_workflow`;
DROP TABLE IF EXISTS `dataset_of_environment`;

-- Dropping the environment table
DROP TABLE IF EXISTS `environment`;

ALTER TABLE `workflow_executions`
    DROP COLUMN `environment_eid`;