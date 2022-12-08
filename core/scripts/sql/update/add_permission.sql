ALTER TABLE user
    ADD `role` ENUM('inactive', 'basic', 'admin', 'restricted') NOT NULL DEFAULT 'inactive';