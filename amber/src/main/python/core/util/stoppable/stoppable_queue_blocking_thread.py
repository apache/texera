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

from loguru import logger
from overrides import overrides

from core.util.customized_queue.queue_base import IQueue, QueueControl, QueueElement
from core.util.runnable.runnable import Runnable
from core.util.stoppable.stoppable import Stoppable


class StoppableQueueBlockingRunnable(Runnable, Stoppable):
    """
    An implementation of Stoppable, assuming the Runnable.run() would be blocked
    by a blocking Queue.get(block=True, timeout=None).

    For example:
    ```
        def run(self) -> None:
            while True:
                entry = queue.get() # here is a blocking Queue.get()
                # do something with the entry
    ```

    According to https://docs.python.org/3/library/queue.html#queue.Queue.get, which
    quoted as: "Prior to 3.0 on POSIX systems, and for all versions on Windows, if
    block is true and timeout is None, this operation goes into an uninterruptible
    wait on an underlying lock."

    Currently, there is no other workaround for interrupting a waiting stoppable,
    safely.

    This implementation adds a special marker called
    `StoppableQueueBlockingRunnable.RUNNABLE_STOP` into the queue, and when the
    marker is consumed, it should break the Runnable.run().

    """

    RUNNABLE_STOP = QueueControl(msg="__RUNNABLE__STOP__MARKER__")

    def __init__(self, name: str, queue: IQueue):
        self._internal_queue = queue
        self.name = name

    @logger.catch(reraise=True)
    @overrides
    def run(self):
        self.pre_start()
        try:
            while True:
                self.receive(self.interruptible_get())
        except StoppableQueueBlockingRunnable.InterruptRunnable:
            # surpassed the expected interruption
            logger.debug(f"{self.name}-interrupting")
        finally:
            self.post_stop()

    @logger.catch(reraise=True)
    def receive(self, next_entry: QueueElement):
        pass

    @logger.catch(reraise=True)
    def pre_start(self) -> None:
        pass

    @logger.catch(reraise=True)
    def post_stop(self) -> None:
        pass

    @logger.catch(reraise=True)
    @overrides
    def stop(self):
        self._internal_queue.put(StoppableQueueBlockingRunnable.RUNNABLE_STOP)

    def interruptible_get(self):
        next_entry = self._internal_queue.get()
        if next_entry == StoppableQueueBlockingRunnable.RUNNABLE_STOP:
            raise StoppableQueueBlockingRunnable.InterruptRunnable
        return next_entry

    class InterruptRunnable(Exception):
        """
        Used to interrupt a runnable.
        """
