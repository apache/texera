UPDATE workflow_user_access SET privilege = IF(write_privilege = true, 'WRITE', IF(read_privilege = true, 'READ', 'NONE')) where privilege = 'NONE';
