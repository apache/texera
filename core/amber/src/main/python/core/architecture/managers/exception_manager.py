from typing import Optional, List

from core.models import ExceptionInfo


class ExceptionManager:
    def __init__(self):
        self.exc_info: Optional[ExceptionInfo] = None
        self.exc_info_history: List[ExceptionInfo] = list()

    def set_exception_info(self, exc_info: ExceptionInfo) -> None:
        self.exc_info = exc_info
        self.exc_info_history.append(exc_info)

    def has_exception(self) -> bool:
        return self.exc_info is not None

    def get_exc_info(self) -> ExceptionInfo:
        exc_info = self.exc_info
        self.exc_info = None
        return exc_info
