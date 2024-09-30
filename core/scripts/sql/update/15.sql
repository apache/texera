USE `texera_db`;

ALTER TABLE workflow
ADD is_published BOOLEAN NOT NULL DEFAULT false;