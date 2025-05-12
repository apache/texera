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

from core.architecture.handlers.control.add_input_channel_handler import (
    AddInputChannelHandler,
)
from core.architecture.handlers.control.add_partitioning_handler import (
    AddPartitioningHandler,
)
from core.architecture.handlers.control.assign_port_handler import AssignPortHandler
from core.architecture.handlers.control.debug_command_handler import (
    WorkerDebugCommandHandler,
)
from core.architecture.handlers.control.evaluate_expression_handler import (
    EvaluateExpressionHandler,
)
from core.architecture.handlers.control.initialize_executor_handler import (
    InitializeExecutorHandler,
)
from core.architecture.handlers.control.no_operation_handler import NoOperationHandler
from core.architecture.handlers.control.open_executor_handler import OpenExecutorHandler
from core.architecture.handlers.control.pause_worker_handler import PauseWorkerHandler
from core.architecture.handlers.control.query_statistics_handler import (
    QueryStatisticsHandler,
)
from core.architecture.handlers.control.replay_current_tuple_handler import (
    RetryCurrentTupleHandler,
)
from core.architecture.handlers.control.resume_worker_handler import ResumeWorkerHandler
from core.architecture.handlers.control.start_worker_handler import StartWorkerHandler


class AsyncRPCHandlerInitializer(
    AddInputChannelHandler,
    AddPartitioningHandler,
    AssignPortHandler,
    WorkerDebugCommandHandler,
    EvaluateExpressionHandler,
    InitializeExecutorHandler,
    OpenExecutorHandler,
    PauseWorkerHandler,
    QueryStatisticsHandler,
    RetryCurrentTupleHandler,
    ResumeWorkerHandler,
    StartWorkerHandler,
    NoOperationHandler,
):
    pass
