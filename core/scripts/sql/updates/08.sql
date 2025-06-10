-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

\c texera_db

SET search_path TO texera_db;

DO $$
BEGIN
    -- Create s3_reference_counts table if it doesn't exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'texera_db' AND table_name = 's3_reference_counts'
    ) THEN
        CREATE TABLE s3_reference_counts (
            s3_uri TEXT PRIMARY KEY,
            reference_count INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Add trigger to automatically update updated_at timestamp
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $BODY$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $BODY$ LANGUAGE plpgsql;

        CREATE TRIGGER update_s3_reference_counts_updated_at
            BEFORE UPDATE ON s3_reference_counts
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();

        -- Function to increment reference count
        CREATE OR REPLACE FUNCTION increment_s3_reference_count(s3_uri_param TEXT)
        RETURNS INT AS $BODY$
        DECLARE
            new_count INT;
        BEGIN
            INSERT INTO s3_reference_counts (s3_uri, reference_count)
            VALUES (s3_uri_param, 1)
            ON CONFLICT (s3_uri) 
            DO UPDATE SET reference_count = s3_reference_counts.reference_count + 1
            RETURNING reference_count INTO new_count;
            
            RETURN new_count;
        END;
        $BODY$ LANGUAGE plpgsql;

        -- Function to decrement reference count
        CREATE OR REPLACE FUNCTION decrement_s3_reference_count(s3_uri_param TEXT)
        RETURNS INT AS $BODY$
        DECLARE
            new_count INT;
        BEGIN
            UPDATE s3_reference_counts
            SET reference_count = GREATEST(0, reference_count - 1)
            WHERE s3_uri = s3_uri_param
            RETURNING reference_count INTO new_count;

            -- If count becomes 0, delete the record
            IF new_count = 0 THEN
                DELETE FROM s3_reference_counts WHERE s3_uri = s3_uri_param;
            END IF;

            RETURN COALESCE(new_count, 0);
        END;
        $BODY$ LANGUAGE plpgsql;
    END IF;
END
$$;
