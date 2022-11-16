from typing import Optional


class ExceptionManager:
    def __init__(self):
        self.exception = Optional[Exception]

    def set_exception(self, exception: Exception):
        self.exception = exception

    def has_exception(self) -> bool:
        return self.exception is not None

    def get_exception(self):
        return self.exception
