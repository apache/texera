#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import datetime
import pytest
from core.models import Tuple
from core.models.schema.schema import Schema
from core.architecture.managers.table_profile_manager import TableProfileManager
from proto.edu.uci.ics.amber.engine.architecture.worker import TableProfile


@pytest.fixture
def complex_schema() -> Schema:
    return Schema(
        raw_schema={
            "id": "INTEGER",
            "name": "STRING",
            "email": "STRING",
            "signup_date": "TIMESTAMP",
            "age": "INTEGER",
            "height_cm": "DOUBLE",
            "is_active": "BOOLEAN",
            "phone": "STRING",
            "zip_code": "STRING",
        }
    )


@pytest.fixture
def sample_rows(complex_schema) -> list[Tuple]:
    return [
        Tuple(
            {
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "signup_date": datetime.datetime(2023, 1, 15, 9, 0),
                "age": 30,
                "height_cm": 165.4,
                "is_active": True,
                "phone": "555-1234",
                "zip_code": "90001"
            },
            schema=complex_schema,
        ),
        Tuple(
            {
                "id": 2,
                "name": "Bob",
                "email": "bob@example.com",
                "signup_date": datetime.datetime(2022, 5, 20, 14, 30),
                "age": 42,
                "height_cm": 178.2,
                "is_active": False,
                "phone": "555-5678",
                "zip_code": "10001"
            },
            schema=complex_schema,
        ),
        Tuple(
            {
                "id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "signup_date": datetime.datetime(2021, 9, 10, 8, 45),
                "age": 28,
                "height_cm": 172.0,
                "is_active": True,
                "phone": "555-9999",
                "zip_code": "60601"
            },
            schema=complex_schema,
        ),
        Tuple(
            {
                "id": 4,
                "name": "Diana",
                "email": "diana@example.net",
                "signup_date": datetime.datetime(2023, 2, 25, 17, 15),
                "age": 35,
                "height_cm": 160.0,
                "is_active": True,
                "phone": "555-1212",
                "zip_code": "94105"
            },
            schema=complex_schema,
        ),
        Tuple(
            {
                "id": 5,
                "name": "Ethan",
                "email": "ethan@example.org",
                "signup_date": datetime.datetime(2024, 4, 2, 11, 5),
                "age": 25,
                "height_cm": 180.7,
                "is_active": False,
                "phone": "555-7777",
                "zip_code": "30301"
            },
            schema=complex_schema,
        ),
    ]


@pytest.fixture
def mgr(sample_rows) -> TableProfileManager:
    mgr = TableProfileManager()
    for t in sample_rows:
        mgr.update_table_profile(t)
    return mgr


class TestTableProfileManager:
    def test_it_builds_valid_profile(self, mgr: TableProfileManager):
        profile: TableProfile = mgr.get_table_profile()

        # Validate general structure
        assert profile.global_profile.samples_used > 0
        assert profile.global_profile.column_count >= 9
        assert profile.global_profile.row_count >= 5

        assert len(profile.column_profiles) == profile.global_profile.column_count

        # Check that known columns have expected labels/fields populated
        col_names = [c.column_name for c in profile.column_profiles]
        for name in ["email", "signup_date", "phone", "zip_code"]:
            assert name in col_names

        # Check statistics were collected for a sample column
        email_col = next((c for c in profile.column_profiles if c.column_name == "email"), None)
        assert email_col is not None
        assert email_col.statistics.unique_count > 0
        assert email_col.data_type == "string"