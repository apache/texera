\c texera_db

SET search_path TO texera_db;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'comment'
    ) THEN
ALTER TABLE "user"
    ADD COLUMN comment TEXT;
END IF;
END
$$;
