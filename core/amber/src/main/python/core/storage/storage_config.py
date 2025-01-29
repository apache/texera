class StorageConfig:
    _initialized = False

    ICEBERG_POSTGRES_CATALOG_USERNAME = None
    ICEBERG_POSTGRES_CATALOG_PASSWORD = None
    ICEBERG_TABLE_NAMESPACE = None
    ICEBERG_FILE_STORAGE_DIRECTORY_PATH = None
    ICEBERG_TABLE_COMMIT_BATCH_SIZE = None

    @classmethod
    def initialize(
        cls,
        postgres_username,
        postgres_password,
        table_namespace,
        directory_path,
        commit_batch_size,
    ):
        if cls._initialized:
            raise RuntimeError(
                "Storage config has already been initialized" "and cannot be modified."
            )

        cls.ICEBERG_POSTGRES_CATALOG_USERNAME = postgres_username
        cls.ICEBERG_POSTGRES_CATALOG_PASSWORD = postgres_password
        cls.ICEBERG_TABLE_NAMESPACE = table_namespace
        cls.ICEBERG_FILE_STORAGE_DIRECTORY_PATH = directory_path
        cls.ICEBERG_TABLE_COMMIT_BATCH_SIZE = commit_batch_size
        cls._initialized = True

    def __new__(cls, *args, **kwargs):
        raise TypeError(f"{cls.__name__} is a static class and cannot be instantiated.")
