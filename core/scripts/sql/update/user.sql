ALTER TABLE `texera_db`.`user` 
ADD COLUMN `permission` INT NOT NULL DEFAULT 0 AFTER `google_id`,
CHANGE COLUMN `name` `email` VARCHAR(256) NOT NULL ,
ADD UNIQUE INDEX `email_UNIQUE` (`email` ASC) VISIBLE;
;
