CREATE USER 'texera'@'%' IDENTIFIED BY 'texera-pass';
GRANT ALL PRIVILEGES ON `texera_db`.* TO 'texera'@'%';
FLUSH PRIVILEGES;
