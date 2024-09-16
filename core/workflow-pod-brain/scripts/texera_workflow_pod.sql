USE `texera_db`;

CREATE TABLE IF NOT EXISTS pod
(
    `uid`                INT UNSIGNED                NOT NULL,
    `wid`                INT UNSIGNED                NOT NULL,
    `name`               VARCHAR(128)                NOT NULL,
    `pod_uid`            VARCHAR(128)                NOT NULL,
    `creation_time`      TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `terminate_time`     TIMESTAMP                   DEFAULT NULL,
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`),
    PRIMARY KEY (`pod_uid`)
    ) ENGINE = INNODB,
    AUTO_INCREMENT = 1;