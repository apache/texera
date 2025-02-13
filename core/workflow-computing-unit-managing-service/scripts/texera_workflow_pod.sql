\c texera_db

CREATE TABLE IF NOT EXISTS pod (
                                   uid BIGINT NOT NULL,
                                   wid BIGINT NOT NULL,
                                   name VARCHAR(128) NOT NULL,
    pod_uid VARCHAR(128) NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    terminate_time TIMESTAMP DEFAULT NULL,
    CONSTRAINT pod_pk PRIMARY KEY (pod_uid),
    CONSTRAINT fk_pod_uid FOREIGN KEY (uid) REFERENCES "user"(uid)
    );
