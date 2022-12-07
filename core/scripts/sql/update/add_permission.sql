ALTER TABLE user
    ADD permission ENUM('pending', 'approved', 'admin') DEFAULT 'pending';