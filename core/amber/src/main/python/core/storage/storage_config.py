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
        cls.ICEBERG_TABLE_COMMIT_BATCH_SIZE = commit_batch_size
        cls._initialized = True

    def __new__(cls, *args, **kwargs):
        raise TypeError(f"{cls.__name__} is a static class and cannot be instantiated.")
