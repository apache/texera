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
from core.models.internal_queue import InternalQueue
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyReturn,
    EmptyRequest,
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
        # Ensure this is really the last message. Read the queued count once (InternalQueue
        # exposes size(); the base IQueue interface does not) and branch on it.
        input_queue: InternalQueue = self.context.input_queue
        queued_count = input_queue.size()
        if queued_count > 0:
            logger.warning(
                f"Received EndWorker before all {queued_count} queued "
                f"message(s) were processed; failing the RPC so the coordinator "
                f"retries once the queue has drained."
            )
            # Fail this RPC (the counterpart of the Scala EndHandler's
            # Future.exception) so the coordinator retries once the queue
            # has drained, instead of dropping the pending message.
            raise RuntimeError("worker still has unprocessed messages")
        # Now we can safely acknowledge that this worker can be terminated.
        return EmptyReturn()
