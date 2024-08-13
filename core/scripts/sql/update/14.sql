CREATE TABLE IF NOT EXISTS cluster (
    `cluster_id` INT AUTO_INCREMENT PRIMARY KEY,
    `owner_id` INT NOT NULL,
    `machine_type` VARCHAR(255) NOT NULL,
    `number_of_machines` INT NOT NULL,
    `creation_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `deletion_time` TIMESTAMP NULL,
    `total_bill` DECIMAL(10, 2) DEFAULT 0.00
    FOREIGN KEY (`owner_id`) REFERENCES `user` (`uid`) ON DELETE CASCADE
) ENGINE = INNODB;