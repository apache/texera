from typing import List

from worker.models.tuple import ITuple


class DataPayload:
    pass


class DataFrame(DataPayload):
    def __init__(self, frame: List[ITuple]):
        self.frame = frame
