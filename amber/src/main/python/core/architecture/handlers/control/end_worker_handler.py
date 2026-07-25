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

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.models.internal_queue import DCMElement
from core.util import IQueue
from core.util.proto import get_one_of
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    EmptyRequest,
    ReturnInvocation,
)


class EndWorkerHandler(ControlHandler):
    """
    The EndWorker control messages is needed to ensure all the other
    control messages in a worker are processed before worker termination.
    """

    async def end_worker(self, req: EmptyRequest) -> EmptyReturn:
        """
        The response of EndWorker to the coordinator indicates that this worker
        has finished not only the data processing logic, but also the processing
        of all the control messages.
        """
        # Ensure this is really the last message that carries work. RPC acks
        # (ReturnInvocations for this worker's own fire-and-forget calls, e.g.
        # worker_execution_completed) race with EndWorker by design: the
        # coordinator decides to end the worker while its acks are still in
        # flight. The worker never awaits those acks, so an ack-only backlog is
        # safe to drop at termination. Anything else fails loudly AND is put
        # back on the queue so the coordinator's retried EndWorker finds it
        # processed (mirrors the Scala EndHandler).
        input_queue: IQueue = self.context.input_queue
        pending = []
        while not input_queue.is_empty():
            pending.append(input_queue.get())
        if not pending:
            # Now we can safely acknowledge that this worker can be terminated.
            return EmptyReturn()

        def is_ack(element) -> bool:
            return isinstance(element, DCMElement) and isinstance(
                get_one_of(element.payload, sealed=False), ReturnInvocation
            )

        if all(is_ack(element) for element in pending):
            logger.warning(
                f"Received EndHandler with only RPC acks left in the queue; "
                f"proceeding with termination. Pending acks: {pending}"
            )
            return EmptyReturn()

        for element in pending:
            input_queue.put(element)
        logger.warning(
            f"Received EndHandler before all messages are processed. "
            f"Unprocessed messages: {pending}"
        )
        raise RuntimeError("worker still has unprocessed messages")
