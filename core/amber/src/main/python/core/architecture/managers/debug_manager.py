from threading import Event

from core.models.tdb import SingleBlockingIO


class DebugManager:
    def __init__(self, condition):
        self.talk_with_debugger = Event()
        self.debug_in = SingleBlockingIO(condition)
        self.debug_out = SingleBlockingIO(condition)
