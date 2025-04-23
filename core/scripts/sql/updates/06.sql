\c texera_db
SET search_path TO texera_db, public;

-- ============================================
-- 1. Create function: Automatically insert into workflow_view_count
-- ============================================
CREATE OR REPLACE FUNCTION insert_workflow_view_count()
RETURNS TRIGGER AS $$
BEGIN
INSERT INTO workflow_view_count (wid, view_count)
VALUES (NEW.wid, 0)
    ON CONFLICT (wid) DO NOTHING;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger: Automatically invoked after inserting into the workflow table
CREATE TRIGGER trg_insert_workflow_view_count
    AFTER INSERT ON workflow
    FOR EACH ROW
    EXECUTE FUNCTION insert_workflow_view_count();

-- ============================================
-- 2. Create function: Automatically insert into dataset_view_count
-- ============================================
CREATE OR REPLACE FUNCTION insert_dataset_view_count()
RETURNS TRIGGER AS $$
BEGIN
INSERT INTO dataset_view_count (did, view_count)
VALUES (NEW.did, 0)
    ON CONFLICT (did) DO NOTHING;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger: Automatically invoked after inserting into the dataset table
CREATE TRIGGER trg_insert_dataset_view_count
    AFTER INSERT ON dataset
    FOR EACH ROW
    EXECUTE FUNCTION insert_dataset_view_count();

-- Initialize view count for existing workflow records if not already present
INSERT INTO workflow_view_count (wid, view_count)
SELECT wid, 0
FROM workflow
WHERE wid NOT IN (SELECT wid FROM workflow_view_count);

-- Initialize view count for existing dataset records if not already present
INSERT INTO dataset_view_count (did, view_count)
SELECT did, 0
FROM dataset
WHERE did NOT IN (SELECT did FROM dataset_view_count);
