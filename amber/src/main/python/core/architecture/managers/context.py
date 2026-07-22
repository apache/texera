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

from typing import Dict, Optional, Set

from loguru import logger

from core.util.console_message.error_message import create_error_console_message
from proto.org.apache.texera.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.worker import WorkerState
from .console_message_manager import ConsoleMessageManager
from .debug_manager import DebugManager
from .embedded_control_message_manager import EmbeddedControlMessageManager
from .exception_manager import ExceptionManager
from .executor_manager import ExecutorManager
from .pause_manager import PauseManager
from .state_manager import StateManager
from .state_processing_manager import StateProcessingManager
from .statistics_manager import StatisticsManager
from .tuple_processing_manager import TupleProcessingManager
from ..packaging.input_manager import InputManager
from ..packaging.output_manager import OutputManager
from ...models import InternalQueue


# State-transition graph for the Python worker. Mirrors the Scala
# `WorkerStateManager` so both language runtimes recognize the same worker
# lifecycle. A worker may transition `READY -> COMPLETED` directly without
# entering `RUNNING` — this is the path taken when there is nothing to
# process (e.g. the upstream port emits zero tuples before signaling
# end-of-stream).
WORKER_STATE_TRANSITIONS: Dict[WorkerState, Set[WorkerState]] = {
    WorkerState.UNINITIALIZED: {WorkerState.READY},
    WorkerState.READY: {WorkerState.PAUSED, WorkerState.RUNNING, WorkerState.COMPLETED},
    WorkerState.RUNNING: {WorkerState.PAUSED, WorkerState.COMPLETED},
    WorkerState.PAUSED: {WorkerState.RUNNING},
    WorkerState.COMPLETED: set(),
}


class Context:
    """
    Manages context of command handlers. Many of those attributes belongs to the DP
    thread, they are managed here to show a clean interface what handlers can or
    should access.

    Context class can be viewed as a friend of DataProcessor.
    """

    def __init__(self, worker_id, input_queue):
        self.worker_id = worker_id
        self.input_queue: InternalQueue = input_queue
        self.executor_manager = ExecutorManager()
        self.current_input_channel_id: Optional[ChannelIdentity] = None
        self.tuple_processing_manager = TupleProcessingManager()
        self.state_processing_manager = StateProcessingManager()
        self.exception_manager = ExceptionManager()
        self.state_manager = StateManager(
            WORKER_STATE_TRANSITIONS,
            WorkerState.UNINITIALIZED,
        )

        self.statistics_manager = StatisticsManager()
        self.pause_manager = PauseManager(
            self.input_queue, state_manager=self.state_manager
        )
        self.output_manager = OutputManager(worker_id)
        self.input_manager = InputManager(worker_id, self.input_queue)
        self.ecm_manager = EmbeddedControlMessageManager(
            ActorVirtualIdentity(worker_id), self.input_manager
        )
        self.console_message_manager = ConsoleMessageManager()
        self.debug_manager = DebugManager(
            self.tuple_processing_manager.context_switch_condition
        )
        # Loop-back write addresses delivered at setup; see the proto field doc
        # on InitializeExecutorRequest.loopStartStateUris (controlcommands.proto).
        self.loop_start_state_uris: Dict[str, str] = {}

    def report_exception(self, err: BaseException) -> None:
        """Route an operator-facing exception to the exception manager and
        queue its stack trace as an error console message.

        Shared by DataProcessor (a UDF error on the data path) and MainLoop
        (a Loop End condition() or loop-back write error on the main-loop
        thread) so both report identically. Reporting must happen before the reporting worker
        switches/pauses, so the console message reaches the coordinator with
        the error rather than after the worker resumes.
        """
        logger.exception(err)
        exc_info = (type(err), err, err.__traceback__)
        self.exception_manager.set_exception_info(exc_info)
        self.console_message_manager.put_message(
            create_error_console_message(self.worker_id, exc_info)
        )

    def close(self):
        self.executor_manager.close()
