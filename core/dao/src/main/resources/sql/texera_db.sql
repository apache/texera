CREATE TABLE IF NOT EXISTS execution (
    eid SERIAL PRIMARY KEY,
    wid INTEGER,
    uid INTEGER,
    name TEXT,
    creation_time TIMESTAMP,
    last_modified_time TIMESTAMP,
    status TEXT,
    result TEXT,
    stats TEXT,
    cuid INTEGER REFERENCES workflow_computing_unit(cuid) ON DELETE SET NULL,
    CONSTRAINT uid_wid_fk FOREIGN KEY (uid, wid) REFERENCES workflow (uid, wid) ON DELETE CASCADE
); 