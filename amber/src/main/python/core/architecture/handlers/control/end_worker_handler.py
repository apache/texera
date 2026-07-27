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

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.models.internal_queue import InternalQueue
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    EmptyRequest,
)


class EndWorkerHandler(ControlHandler):
    """
    The EndWorker control messages is needed to ensure all the other
    control messages in a worker are processed before worker termination.

    EndWorker is the last *request* the coordinator sends to a worker, but not
    the last message it receives: the coordinator sends EndWorker from inside
    its port_completed handler, so the reply to that same port_completed trails
    EndWorker on the control channel on every normal region teardown. A queued
    ReturnInvocation therefore does not count as unprocessed work (see
    InternalQueue.has_unprocessed_work).
    """

    async def end_worker(self, req: EmptyRequest) -> EmptyReturn:
        """
        The response of EndWorker to the coordinator indicates that this worker
        has finished not only the data processing logic, but also the processing
        of all the queued control messages that represent work. On failure the
        coordinator retries EndWorker after a delay instead of stopping a worker
        that still has real queued work.
        """
        input_queue: InternalQueue = self.context.input_queue
        if input_queue.has_unprocessed_work():
            # peek(), never get(): a diagnostic must not consume the message it
            # reports. The label is best-effort — peek() returns the highest-
            # priority head, which is not necessarily the element that blocks.
            pending = input_queue.peek()
            raise RuntimeError(
                "worker still has unprocessed messages: "
                f"next = {type(pending).__name__}"
            )
        # Now we can safely acknowledge that this worker can be terminated.
        return EmptyReturn()
