import threading
from pdb import Pdb
from threading import Condition
from typing import IO

from loguru import logger


class SingleBlockingIO(IO):
    def __init__(self, condition: Condition):
        self.value = None
        self.condition = condition

    def write(self, v):
        if self.value is None:
            self.value = ""

        self.value += v
        logger.info("Now value is:" + repr(self.value))

    def flush(self):
        if self.value is None:
            self.value = ""
        self.value += "\n"
        logger.info("calling flush " + str(threading.current_thread()))
        with self.condition:
            logger.info(
                "done with flush "
                + str(threading.current_thread())
                + ": "
                + repr(self.value)
            )
            self.condition.notify()
            logger.info("waiting after flush " + str(threading.current_thread()))
            self.condition.wait()

    def readline(self, limit=None):
        logger.info("calling readline " + str(threading.current_thread()))
        try:
            with self.condition:
                if self.value is None:
                    logger.info("waiting on read " + str(threading.current_thread()))
                    self.condition.wait()
                logger.info(
                    "done with read "
                    + str(threading.current_thread())
                    + ": "
                    + repr(self.value)
                )
                return self.value + "\n"
        finally:
            logger.info("set value to None")
            self.value = None

    readline_unblock = readline


class Tdb(Pdb):
    def __init__(self, stdin: SingleBlockingIO, stdout: SingleBlockingIO):
        super().__init__(stdin=stdin, stdout=stdout)
