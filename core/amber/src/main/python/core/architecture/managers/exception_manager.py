from typing import Optional


class ExceptionManager:
    def __init__(self):
        self.exception: Optional[Exception] = None
        self.exception_history = list()

    def set_exception(self, exception: Exception):
        self.exception = exception
        self.exception_history.append(exception)

    def has_exception(self) -> bool:
        return self.exception is not None

    def get_exception(self):
        exception = self.exception
        self.exception = None
        return exception
