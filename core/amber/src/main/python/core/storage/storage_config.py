# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


class StorageConfig:
    """
    A static class to keep the storage-related configs.
    This class should be initialized with the configs passed from Java side and
    is used by all storage-related classes.
    """

    _initialized = False

    ICEBERG_POSTGRES_CATALOG_URI_WITHOUT_SCHEME = None
    ICEBERG_POSTGRES_CATALOG_USERNAME = None
    ICEBERG_POSTGRES_CATALOG_PASSWORD = None
    ICEBERG_TABLE_RESULT_NAMESPACE = None
    ICEBERG_FILE_STORAGE_DIRECTORY_PATH = None
    ICEBERG_TABLE_COMMIT_BATCH_SIZE = None

    @classmethod
    def initialize(
        cls,
        postgres_uri_without_scheme,
        postgres_username,
        postgres_password,
        table_result_namespace,
        directory_path,
        commit_batch_size,
    ):
        if cls._initialized:
            raise RuntimeError(
                "Storage config has already been initialized" "and cannot be modified."
            )

        cls.ICEBERG_POSTGRES_CATALOG_URI_WITHOUT_SCHEME = postgres_uri_without_scheme
        cls.ICEBERG_POSTGRES_CATALOG_USERNAME = postgres_username
        cls.ICEBERG_POSTGRES_CATALOG_PASSWORD = postgres_password
        cls.ICEBERG_TABLE_RESULT_NAMESPACE = table_result_namespace
        cls.ICEBERG_FILE_STORAGE_DIRECTORY_PATH = directory_path
        cls.ICEBERG_TABLE_COMMIT_BATCH_SIZE = int(commit_batch_size)
        cls._initialized = True

    def __new__(cls, *args, **kwargs):
        raise TypeError(f"{cls.__name__} is a static class and cannot be instantiated.")
