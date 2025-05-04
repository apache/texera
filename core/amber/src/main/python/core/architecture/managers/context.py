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

from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerState
from typing import Optional
from .console_message_manager import ConsoleMessageManager
from .channel_marker_manager import ChannelMarkerManager
from .debug_manager import DebugManager
from .exception_manager import ExceptionManager
from .marker_processing_manager import MarkerProcessingManager
from .tuple_processing_manager import TupleProcessingManager
from .executor_manager import ExecutorManager
from .pause_manager import PauseManager
from .state_manager import StateManager
from .statistics_manager import StatisticsManager
from ..packaging.input_manager import InputManager
from ..packaging.output_manager import OutputManager
from ...models import InternalQueue


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
        self.marker_processing_manager = MarkerProcessingManager()
        self.exception_manager = ExceptionManager()
        self.state_manager = StateManager(
            {
                WorkerState.UNINITIALIZED: {WorkerState.READY},
                WorkerState.READY: {WorkerState.PAUSED, WorkerState.RUNNING},
                WorkerState.RUNNING: {WorkerState.PAUSED, WorkerState.COMPLETED},
                WorkerState.PAUSED: {WorkerState.RUNNING},
                WorkerState.COMPLETED: set(),
            },
            WorkerState.UNINITIALIZED,
        )

        self.statistics_manager = StatisticsManager()
        self.pause_manager = PauseManager(
            self.input_queue, state_manager=self.state_manager
        )
        self.output_manager = OutputManager(worker_id)
        self.input_manager = InputManager()
        self.channel_marker_manager = ChannelMarkerManager(
            ActorVirtualIdentity(worker_id), self.input_manager
        )
        self.console_message_manager = ConsoleMessageManager()
        self.debug_manager = DebugManager(
            self.tuple_processing_manager.context_switch_condition
        )

    def close(self):
        self.executor_manager.close()
