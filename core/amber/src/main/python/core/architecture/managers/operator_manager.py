from typing import Optional

from core.models import Operator


class OperatorManager:
    def __init__(self):
        self.operator: Optional[Operator] = None
